use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};
use tycho_network::Network;
use tycho_util::futures::{JoinTask, Shared};

use crate::effects::{Cancelled, TaskResult};
use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::UnixTime;
use crate::moderator::ban::core::BanCore;
use crate::moderator::journal::batch::batch;
use crate::moderator::journal::item::{JournalItem, JournalItemFull};
use crate::moderator::journal::record_key::RecordKeyFactory;
use crate::moderator::{
    DelayedDbTask, JournalConfig, JournalEvent, JournalPoint, JournalStore, ModeratorConfig,
};
use crate::storage::MempoolDb;

/// Must outlive [`crate::engine::lifecycle::EngineSession`] just like opened DB
#[derive(Clone)]
pub struct Moderator(Arc<dyn ModeratorTrait>);

impl Moderator {
    #[cfg(any(test, feature = "test"))]
    pub fn new_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn new<Str: ToString>(
        network: &Network,
        mempool_db: Arc<MempoolDb>,
        config: ModeratorConfig,
        version: Str,
    ) -> Result<Self> {
        config.validate()?;

        let journal_store = JournalStore::new(mempool_db)?;

        // unbounded because we don't know the capacity, anyway the queue won't be long
        let (delayed_db_tasks_tx, delayed_db_tasks_rx) = mpsc::unbounded_channel();
        // unbounded because of non-async infallible `send()`, though hardly ever will send updates
        let (mempool_conf_tx, mempool_conf_rx) = mpsc::unbounded_channel();

        let mut record_key_factory = RecordKeyFactory::default();
        let start_item_full = JournalItemFull {
            key: record_key_factory.new_key(), // starts with zero `seq_no`
            item: JournalItem::NodeStarted(*network.peer_id(), version.to_string()),
        };
        let started = start_item_full.key.created;
        let special_since = started - config.journal.ttl.to_time();
        let all_since = started - config.bans.max_duration().to_time();

        delayed_db_tasks_tx
            .send(DelayedDbTask::Items {
                items: vec![start_item_full],
                user_callback: None,
            })
            .context("channel db delayed write of node start")?;

        let ban_core = BanCore::new(config.bans, record_key_factory, delayed_db_tasks_tx);

        let (init_tx, init_rx) = oneshot::channel();

        let inner = Arc::new(ModeratorInner {
            ban_core: ban_core.clone(),
            mempool_conf_tx,
            _delayed_db_writer: {
                let f = ModeratorInner::delayed_db_runner(
                    journal_store.clone(),
                    config.journal,
                    init_rx,
                    mempool_conf_rx,
                    delayed_db_tasks_rx,
                );
                JoinTask::new(async { f.await.expect("delayed db runner failed") })
            },
            init_task: {
                let journal_store_2 = journal_store.clone();
                let f = move || {
                    let short_events = journal_store_2.load_restore(special_since, all_since);
                    (ban_core.restore(started, short_events))
                        .map_err(|e| format!("restore: {e}"))?;
                    (init_tx.send(())).map_err(|()| "cannot notify mempool init".to_string())?;
                    Ok(())
                };
                let task = async move {
                    journal_store.init().await.map_err(|e| e.to_string())?;
                    (db_spawn(f).await)
                        .unwrap_or_else(|Cancelled()| Err("restore aborted".to_string()))
                };
                Shared::new(task.boxed())
            },
        });

        let inner_clone = inner.clone();

        Ok(Self(inner_clone))
    }

    /// applies migrations and guards [`Self::set_peer_schedule`]
    pub async fn wait_init(&self) -> Result<()> {
        tracing::info!("wait Moderator init");
        self.0.wait_init().await.0.map_err(anyhow::Error::msg)?;
        tracing::info!("Moderator is init, continue");
        Ok(())
    }

    pub(crate) fn set_peer_schedule(&self, peer_schedule: &PeerSchedule) {
        self.0.set_peer_schedule(peer_schedule);
    }

    pub(crate) fn report(&self, data: JournalEvent) {
        self.0.report(data);
    }

    pub(crate) fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.0.apply_mempool_config(conf);
    }
}

trait ModeratorTrait: Send + Sync {
    fn wait_init(&self) -> Shared<BoxFuture<'static, std::result::Result<(), String>>>;
    fn set_peer_schedule(&self, peer_schedule: &PeerSchedule);
    fn report(&self, event: JournalEvent);
    fn apply_mempool_config(&self, conf: &MempoolConfig);
}

#[cfg(any(test, feature = "test"))]
impl ModeratorTrait for () {
    fn wait_init(&self) -> Shared<BoxFuture<'static, std::result::Result<(), String>>> {
        Shared::new(futures_util::future::ready(Ok(())).boxed())
    }
    fn set_peer_schedule(&self, _: &PeerSchedule) {}
    fn report(&self, _: JournalEvent) {}
    fn apply_mempool_config(&self, _: &MempoolConfig) {}
}

struct ModeratorInner {
    /// because gets created and init once at a node start and may outlive engine session
    init_task: Shared<BoxFuture<'static, std::result::Result<(), String>>>,
    ban_core: BanCore,
    mempool_conf_tx: mpsc::UnboundedSender<MempoolConfig>,
    _delayed_db_writer: JoinTask<()>, // moderator outlives mempool session(s)
}

impl ModeratorTrait for ModeratorInner {
    fn wait_init(&self) -> Shared<BoxFuture<'static, std::result::Result<(), String>>> {
        self.init_task.clone()
    }

    fn set_peer_schedule(&self, peer_schedule: &PeerSchedule) {
        self.ban_core.set_peer_schedule(peer_schedule);
    }

    fn report(&self, event: JournalEvent) {
        self.ban_core.send_report(event).ok();
    }

    fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.mempool_conf_tx.send(conf.clone()).ok();
    }
}

impl ModeratorInner {
    async fn delayed_db_runner(
        journal_store: JournalStore,
        journal_config: JournalConfig,
        init_rx: oneshot::Receiver<()>,
        mut mempool_conf_rx: mpsc::UnboundedReceiver<MempoolConfig>,
        mut delayed_db_tasks_rx: mpsc::UnboundedReceiver<DelayedDbTask>,
    ) -> Result<()> {
        scopeguard::defer!(tracing::warn!(
            "Mempool moderator delayed db writer shut down"
        ));
        if init_rx.await.is_err() {
            tracing::warn!("Mempool init aborted, shutting down Moderator DB Writer");
            return Ok(());
        }
        let mut j_point_max_bytes = 1_000_000; // only to alloc; don't wait config
        let mut batch_interval = tokio::time::interval(journal_config.batch_interval);
        let mut clean_interval = tokio::time::interval(journal_config.clean_interval);
        batch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut full_items = Vec::new();

        loop {
            tokio::select! {
                Some(conf) = mempool_conf_rx.recv() => {
                    j_point_max_bytes = JournalPoint::max_tl_bytes(&conf);
                },
                Some(task) = delayed_db_tasks_rx.recv() => match task {
                    DelayedDbTask::Items{mut items, user_callback} => {
                        for new in &items {
                            meter_event(&new.item);
                        }
                        full_items.append(&mut items);
                        if let Some(user_callback) = user_callback {
                            store_events(&mut full_items, &journal_store, j_point_max_bytes).await?;
                            batch_interval.reset();
                            user_callback.send(Ok(())).ok();
                        }
                    },
                },
                _ = batch_interval.tick() => {
                    store_events(&mut full_items, &journal_store, j_point_max_bytes).await?;
                    batch_interval.reset(); // give time to form a new batch
                },
                _ = clean_interval.tick() => {
                    store_events(&mut full_items, &journal_store, j_point_max_bytes).await?;
                    batch_interval.reset();
                    let range = UnixTime::from_millis(0).. UnixTime::now() - journal_config.ttl.to_time();
                    delete_events(range, &journal_store).await?;
                },
                else => panic!("unhandled match arm in delayed_db_runner"),
            }
        }
    }
}

async fn store_events(
    full_items: &mut Vec<JournalItemFull>,
    journal_store: &JournalStore,
    j_point_max_bytes: usize,
) -> Result<()> {
    if full_items.is_empty() {
        return Ok(());
    }
    let journal_store = journal_store.clone();
    let moved_items = std::mem::take(full_items);
    db_spawn(move || journal_store.store_records(batch(&moved_items), j_point_max_bytes))
        .await
        .unwrap_or_else(|Cancelled()| Err(anyhow::anyhow!("db call aborted")))
        .context("journal store events")
}

async fn delete_events(range: Range<UnixTime>, journal_store: &JournalStore) -> Result<()> {
    let journal_store = journal_store.clone();
    db_spawn(move || journal_store.delete(range))
        .await
        .unwrap_or_else(|Cancelled()| Err(anyhow::anyhow!("db call aborted")))
        .context("journal delete events")
}

async fn db_spawn<F, R>(f: F) -> TaskResult<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => Ok(result),
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(_) => Err(Cancelled()),
    }
}

/// Meters only events that account for bans. To be Used right before DB write.
fn meter_event(item: &JournalItem) {
    if !item.action().is_ban_related() {
        return;
    }
    let JournalItem::Event(event) = item else {
        return;
    };
    let kind = match event {
        JournalEvent::BadRequest(_, query, _) => format!("{query:?} request tl err"),
        JournalEvent::BadResponse(_, query, _) => format!("{query:?} response tl err"),
        JournalEvent::QueryLimitReached(_, query) => format!("{query:?} rate limit"),
        JournalEvent::PointIntegrityError(_, query, err) => format!("{query:?} {err:?}"),
        other => format!("{:?}", other.tag()),
    };
    let labels = [("kind", kind), ("peer_id", format!("{}", event.peer_id()))];
    metrics::counter!("tycho_mempool_moderator_event", &labels).increment(1);
}
