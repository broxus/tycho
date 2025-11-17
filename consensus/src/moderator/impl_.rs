use std::ops::Range;
use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};
use tycho_network::{Network, PeerId};
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;

use crate::engine::MempoolConfig;
use crate::intercom::PeerSchedule;
use crate::models::UnixTime;
use crate::moderator::ban::CurrentBan;
use crate::moderator::ban::core::BanCore;
use crate::moderator::journal::batch::batch;
use crate::moderator::journal::item::{JournalItem, JournalItemFull};
use crate::moderator::journal::record_key::RecordKeyFactory;
use crate::moderator::{BanConfigDuration, JournalEvent, JournalTtl, ModeratorConfig, RecordFull};
use crate::storage::{JournalStore, MempoolDb};

/// Must outlive [`crate::engine::lifecycle::EngineSession`] just like opened DB
#[derive(Clone)]
pub struct Moderator(Arc<dyn ModeratorTrait>);

impl Moderator {
    #[cfg(any(test, feature = "test"))]
    pub fn new_stub() -> Self {
        Self(Arc::new(ModeratorStub {
            config: ModeratorConfig::test_default(),
        }))
    }

    pub fn new<Str: ToString>(
        network: &Network,
        mempool_db: Arc<MempoolDb>,
        config: ModeratorConfig,
        version: Str,
    ) -> Result<Self> {
        config.validate()?;

        let journal_store = JournalStore::new(mempool_db);

        // unbounded because we don't know the capacity, anyway the queue won't be long
        let (delayed_db_writes_tx, delayed_db_writes_rx) = mpsc::unbounded_channel();
        // unbounded because of non-async infallible `send()`, though hardly ever will send updates
        let (mempool_conf_tx, mempool_conf_rx) = mpsc::unbounded_channel();

        let mut record_key_factory = RecordKeyFactory::default();
        let start_item_full = JournalItemFull {
            key: record_key_factory.new_key(), // starts with zero `seq_no`
            item: JournalItem::NodeStarted(*network.peer_id(), version.to_string()),
        };
        let started = start_item_full.key.created;
        let since = started - config.bans.max_duration().to_time();

        delayed_db_writes_tx.send(vec![start_item_full]).ok();

        let ban_core = BanCore::new(config, record_key_factory, delayed_db_writes_tx);

        let inner = Arc::new(ModeratorInner {
            init: Once::new(),
            local_id: *network.peer_id(),
            ban_core,
            journal_store: journal_store.clone(),
            mempool_conf_tx,
            _delayed_db_writer: JoinTask::new(ModeratorInner::delayed_db_write(
                journal_store,
                delayed_db_writes_rx,
                mempool_conf_rx,
            )),
        });

        let inner_clone = inner.clone();

        tokio::task::spawn_blocking(move || {
            inner.init.call_once(|| {
                let short_events = inner
                    .journal_store
                    .load_restore(since)
                    .expect("load events on init");
                (inner.ban_core).restore(started, &short_events);
            });
        });

        Ok(Self(inner_clone))
    }

    pub fn wait_init_blocking(&self) {
        self.0.wait_init_blocking();
    }

    pub fn event_journal_ttl(&self) -> JournalTtl {
        self.0.event_journal_ttl()
    }

    pub(crate) fn set_peer_schedule(&self, peer_schedule: &PeerSchedule) {
        self.0.set_peer_schedule(peer_schedule);
    }

    pub(crate) fn report(&self, data: JournalEvent) {
        self.0.report(data);
    }

    /// Init or update delayed db writer with mempool config
    pub(crate) fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.0.apply_mempool_config(conf);
    }

    pub fn dump_bans(&self) -> Result<FastHashMap<PeerId, CurrentBan>> {
        self.0.dump_bans()
    }

    pub fn dump_events(&self, peer_id: Option<&PeerId>) -> Result<serde_json::Value> {
        self.0.dump_events(peer_id)
    }

    pub fn manual_ban(&self, peer_id: &PeerId, duration: Duration) -> Result<serde_json::Value> {
        (self.0).manual_ban(peer_id, duration.try_into()?)
    }

    pub fn manual_unban(&self, peer_id: &PeerId) -> BoxFuture<'static, Result<()>> {
        self.0.manual_unban(peer_id)
    }

    pub fn list_events(&self, count: u16, page: u32, asc: bool) -> Result<Vec<RecordFull>> {
        self.0.list_events(count, page, asc)
    }

    pub fn delete_events(&self, millis: Range<u64>) -> Result<()> {
        self.0.delete_events(millis)
    }
}

trait ModeratorTrait: Send + Sync {
    fn wait_init_blocking(&self);
    fn event_journal_ttl(&self) -> JournalTtl;
    fn set_peer_schedule(&self, peer_schedule: &PeerSchedule);
    fn report(&self, event: JournalEvent);
    fn apply_mempool_config(&self, conf: &MempoolConfig);
    fn dump_bans(&self) -> Result<FastHashMap<PeerId, CurrentBan>>;
    fn dump_events(&self, peer_id: Option<&PeerId>) -> Result<serde_json::Value>;
    fn manual_ban(
        &self,
        peer_id: &PeerId,
        duration: BanConfigDuration,
    ) -> Result<serde_json::Value>;
    fn manual_unban(&self, peer_id: &PeerId) -> BoxFuture<'static, Result<()>>;
    fn list_events(&self, count: u16, page: u32, asc: bool) -> Result<Vec<RecordFull>>;
    fn delete_events(&self, millis: Range<u64>) -> Result<()>;
}

#[cfg(any(test, feature = "test"))]
struct ModeratorStub {
    config: ModeratorConfig,
}

#[cfg(any(test, feature = "test"))]
impl ModeratorTrait for ModeratorStub {
    fn wait_init_blocking(&self) {}
    fn event_journal_ttl(&self) -> JournalTtl {
        self.config.event_journal_ttl
    }
    fn set_peer_schedule(&self, _: &PeerSchedule) {}
    fn report(&self, _: JournalEvent) {}
    fn apply_mempool_config(&self, _: &MempoolConfig) {}
    fn dump_bans(&self) -> Result<FastHashMap<PeerId, CurrentBan>> {
        Ok(FastHashMap::default())
    }
    fn dump_events(&self, _: Option<&PeerId>) -> Result<serde_json::Value> {
        Ok(serde_json::json!({}))
    }
    fn manual_ban(&self, _: &PeerId, _: BanConfigDuration) -> Result<serde_json::Value> {
        Ok(serde_json::json!({}))
    }
    fn manual_unban(&self, _: &PeerId) -> BoxFuture<'static, Result<()>> {
        Box::pin(futures_util::future::ready(Ok(())))
    }
    fn list_events(&self, _: u16, _: u32, _: bool) -> Result<Vec<RecordFull>> {
        Ok(Vec::new())
    }
    fn delete_events(&self, _: Range<u64>) -> Result<()> {
        Ok(())
    }
}

struct ModeratorInner {
    /// because gets created and init once at a node start and may outlive engine session
    init: Once,
    local_id: PeerId,
    ban_core: BanCore,
    journal_store: JournalStore,
    mempool_conf_tx: mpsc::UnboundedSender<MempoolConfig>,
    _delayed_db_writer: JoinTask<()>, // moderator outlives mempool session(s)
}

impl ModeratorTrait for ModeratorInner {
    fn wait_init_blocking(&self) {
        tracing::info!("waiting Moderator to init");
        self.init.wait();
        tracing::info!("Moderator is init, resuming");
    }

    fn event_journal_ttl(&self) -> JournalTtl {
        self.ban_core.event_journal_ttl()
    }

    fn set_peer_schedule(&self, peer_schedule: &PeerSchedule) {
        self.ban_core.set_peer_schedule(peer_schedule);
    }

    fn report(&self, event: JournalEvent) {
        self.ban_core.send_report(event);
    }

    fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.mempool_conf_tx.send(conf.clone()).ok();
    }

    fn dump_bans(&self) -> Result<FastHashMap<PeerId, CurrentBan>> {
        self.check_init()?;
        Ok(self.ban_core.dump_bans())
    }

    fn dump_events(&self, peer_id: Option<&PeerId>) -> Result<serde_json::Value> {
        self.check_init()?;
        Ok(self.ban_core.dump_events(peer_id))
    }

    fn manual_ban(
        &self,
        peer_id: &PeerId,
        duration: BanConfigDuration,
    ) -> Result<serde_json::Value> {
        anyhow::ensure!(self.local_id != peer_id, "cannot ban yourself");
        self.check_init()?;
        let until = UnixTime::now() + duration.to_time();
        self.ban_core.manual_ban(peer_id, until)?;
        Ok(serde_json::json!({"banned_until_utc_millis": until.millis()}))
    }

    fn manual_unban(&self, peer_id: &PeerId) -> BoxFuture<'static, Result<()>> {
        // leave a way to unban yourself just in case of DB transplantation
        match self.check_init() {
            Ok(()) => {
                let (tx, rx) = oneshot::channel();
                let result = self.ban_core.manual_unban(peer_id, tx);
                Box::pin(async move {
                    result?;
                    rx.await.context("failed in unban task")?
                })
            }
            Err(e) => Box::pin(futures_util::future::ready(Err(e))),
        }
    }

    fn list_events(&self, count: u16, page: u32, asc: bool) -> Result<Vec<RecordFull>> {
        // no need to check init
        self.journal_store.load_records(count, page, asc)
    }

    fn delete_events(&self, millis: Range<u64>) -> Result<()> {
        if millis.is_empty() {
            anyhow::bail!("time range is empty");
        }
        self.check_init()?;
        let range = UnixTime::from_millis(millis.start)..UnixTime::from_millis(millis.end);
        self.journal_store.delete(range)?;
        Ok(())
    }
}

impl ModeratorInner {
    fn check_init(&self) -> Result<()> {
        if self.init.is_completed() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("moderator is not init yet"))
        }
    }

    async fn delayed_db_write(
        journal_store: JournalStore,
        mut delayed_db_writes_rx: mpsc::UnboundedReceiver<Vec<JournalItemFull>>,
        mut mempool_conf_rx: mpsc::UnboundedReceiver<MempoolConfig>,
    ) {
        scopeguard::defer!(tracing::warn!(
            "Mempool moderator delayed db writer shut down"
        ));
        let Some(mut conf) = mempool_conf_rx.recv().await else {
            tracing::warn!("Mempool config channel closed before init in Moderator DB Writer");
            return;
        };
        let mut batch_interval = tokio::time::interval(Duration::from_secs(2));
        batch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut full_items = Vec::new();
        loop {
            tokio::select! {
                biased;
                Some(new_conf) = mempool_conf_rx.recv() => {
                    conf = new_conf;
                    continue;
                },
                Some(mut new_items) = delayed_db_writes_rx.recv() => {
                    for new in &new_items {
                        meter_event(&new.item);
                    }
                    full_items.append(&mut new_items);
                    continue;
                },
                _ = batch_interval.tick() => if full_items.is_empty() { continue },
                else => return,
            };
            let conf_point_max_bytes = conf.point_max_bytes;
            let journal_store = journal_store.clone();
            let moved_items = std::mem::take(&mut full_items);
            let result = tokio::task::spawn_blocking(move || {
                journal_store
                    .store_records(batch(&moved_items), conf_point_max_bytes)
                    .context("Mempool moderator delayed db write failed")
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => panic!("{e:?}"),
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => {
                    tracing::error!("Mempool moderator delayed db write aborted: {e:?}");
                    return;
                }
            }
            batch_interval.reset(); // give time to form a new batch
        }
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
    let labels = [
        ("kind", kind),
        ("peer_id", format!("{:.4}", event.peer_id())),
    ];
    metrics::counter!("tycho_mempool_moderator_event", &labels).increment(1);
}
