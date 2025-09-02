use std::sync::{Arc, Once};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tycho_network::Network;
use tycho_util::futures::JoinTask;

use crate::effects::{Ctx, RoundCtx};
use crate::engine::MempoolConfig;
use crate::models::UnixTime;
use crate::moderator::ban::core::BanCore;
use crate::moderator::journal::batch::batch;
use crate::moderator::journal::item::{JournalItem, JournalItemFull};
use crate::moderator::{JournalEvent, ModeratorConfig, RecordKey};
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

        let start_item = JournalItemFull {
            key: RecordKey::new(), // always starts with zero
            item: JournalItem::NodeStarted(*network.peer_id(), version.to_string()),
        };
        let started = start_item.key.created;
        delayed_db_writes_tx.send(vec![start_item]).ok();

        let ban_core = BanCore::new(network, &delayed_db_writes_tx);

        let inner = Arc::new(ModeratorInner {
            init: Once::new(),
            delayed_db_writes_tx,
            ban_core,
            journal_store: journal_store.clone(),
            config,
            mempool_conf_tx,
            _delayed_db_writer: JoinTask::new(ModeratorInner::delayed_db_write(
                journal_store,
                delayed_db_writes_rx,
                mempool_conf_rx,
            )),
        });

        let inner_clone = inner.clone();

        tokio::task::spawn_blocking(move || {
            let since = UnixTime::now() - inner.config.bans.max_duration().to_time();
            inner.init.call_once(|| {
                let short_events = inner
                    .journal_store
                    .load_restore(since)
                    .expect("load events on init");
                let new_records =
                    (inner.ban_core).restore(started, &short_events, &inner.config.bans);
                inner.delayed_db_writes_tx.send(new_records).ok();
            });
        });

        Ok(Self(inner_clone))
    }

    pub fn wait_init_blocking(&self) {
        self.0.wait_init_blocking();
    }

    pub fn config(&self) -> &ModeratorConfig {
        self.0.config()
    }

    /// Blocking sequential write to DB of batch of events
    pub fn report_blocking(&self, batch: Vec<JournalEvent>, round_ctx: &RoundCtx) {
        self.0.report_blocking(batch, round_ctx);
    }

    /// Event is channelled to a separate worker that writes events to DB one-by-one
    pub fn send_report(&self, data: JournalEvent) {
        self.0.send_report(data);
    }

    /// Init or update delayed db writer with mempool config
    pub(crate) fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.0.apply_mempool_config(conf);
    }
}

trait ModeratorTrait: Send + Sync {
    fn wait_init_blocking(&self);
    fn config(&self) -> &ModeratorConfig;
    fn report_blocking(&self, batch: Vec<JournalEvent>, round_ctx: &RoundCtx);
    fn send_report(&self, data: JournalEvent);
    fn apply_mempool_config(&self, conf: &MempoolConfig);
}

#[cfg(any(test, feature = "test"))]
struct ModeratorStub {
    config: ModeratorConfig,
}

#[cfg(any(test, feature = "test"))]
impl ModeratorTrait for ModeratorStub {
    fn wait_init_blocking(&self) {}
    fn config(&self) -> &ModeratorConfig {
        &self.config
    }
    fn report_blocking(&self, _: Vec<JournalEvent>, _: &RoundCtx) {}
    fn send_report(&self, _: JournalEvent) {}
    fn apply_mempool_config(&self, _: &MempoolConfig) {}
}

struct ModeratorInner {
    /// because gets created and init once at a node start and may outlive engine session
    init: Once,
    delayed_db_writes_tx: mpsc::UnboundedSender<Vec<JournalItemFull>>,
    ban_core: BanCore,
    journal_store: JournalStore,
    config: ModeratorConfig,
    mempool_conf_tx: mpsc::UnboundedSender<MempoolConfig>,
    _delayed_db_writer: JoinTask<()>, // moderator outlives mempool session(s)
}

impl ModeratorTrait for ModeratorInner {
    fn wait_init_blocking(&self) {
        self.init.wait();
    }

    fn config(&self) -> &ModeratorConfig {
        &self.config
    }

    fn report_blocking(&self, events: Vec<JournalEvent>, round_ctx: &RoundCtx) {
        let full_items = self.events_to_items_and_maybe_ban(events);
        self.journal_store
            .store_records(batch(&full_items), round_ctx.conf().point_max_bytes)
            .expect("store records");
    }

    fn send_report(&self, event: JournalEvent) {
        let records = self.events_to_items_and_maybe_ban(vec![event]);
        // expect receiver is dropped only at shutdown
        self.delayed_db_writes_tx.send(records).ok();
    }

    fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.mempool_conf_tx.send(conf.clone()).ok();
    }
}

impl ModeratorInner {
    fn events_to_items_and_maybe_ban(&self, events: Vec<JournalEvent>) -> Vec<JournalItemFull> {
        let mut items = Vec::with_capacity(events.len());
        let mut new_bans = Vec::new();

        // first: assign keys with `seq_no` inside to external events
        for event in events {
            let key = RecordKey::new();
            if let Some(ban) = self.ban_core.maybe_ban_item(key, &event, &self.config.bans) {
                new_bans.push(ban);
            };
            let item = JournalItem::Event(event);
            items.push(JournalItemFull { key, item });
        }

        // second: assign keys with `seq_no` inside to newly generated ban records
        for item in new_bans {
            let key = RecordKey::new();
            self.ban_core.send_ban(key, &item);
            items.push(JournalItemFull { key, item });
        }

        // now items are in historical order
        items
    }

    async fn delayed_db_write(
        journal_store: JournalStore,
        mut delayed_db_writes_rx: mpsc::UnboundedReceiver<Vec<JournalItemFull>>,
        mut mempool_conf_rx: mpsc::UnboundedReceiver<MempoolConfig>,
    ) {
        let Some(mut conf) = mempool_conf_rx.recv().await else {
            tracing::warn!("Mempool config channel closed before init in Moderator DB Writer");
            return;
        };
        loop {
            tokio::select! {
                Some(new_conf) = mempool_conf_rx.recv() => conf = new_conf,
                Some(full_items) = delayed_db_writes_rx.recv() => {
                    let conf_point_max_bytes = conf.point_max_bytes;
                    let journal_store = journal_store.clone();
                    let result = tokio::task::spawn_blocking(move ||
                        journal_store
                            .store_records(batch(&full_items), conf_point_max_bytes)
                            .context("Mempool moderator delayed db write failed")
                    ).await;
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(e)) => panic!("{e:?}"),
                        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                        Err(e) => {
                            tracing::warn!("Mempool moderator delayed db write aborted: {e:?}");
                            break;
                        }
                    }
                }
                else => break,
            }
        }
        tracing::warn!("Mempool moderator delayed db writer shut down");
    }
}
