use std::ops::Range;
use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tycho_network::{Network, PeerId};
use tycho_util::futures::JoinTask;

use crate::effects::{Ctx, RoundCtx};
use crate::engine::MempoolConfig;
use crate::models::UnixTime;
use crate::moderator::ban::core::BanCore;
use crate::moderator::journal::batch::batch;
use crate::moderator::journal::item::{BanItem, ItemOrigin, JournalItem, JournalItemFull};
use crate::moderator::{BanConfigDuration, JournalEvent, ModeratorConfig, RecordFull, RecordKey};
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
            network: network.clone(),
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
    pub(crate) fn report_blocking(&self, batch: Vec<JournalEvent>, round_ctx: &RoundCtx) {
        self.0.report_blocking(batch, round_ctx);
    }

    /// Event is channelled to a separate worker that writes events to DB one-by-one
    pub(crate) fn send_report(&self, data: JournalEvent) {
        self.0.send_report(data);
    }

    /// Init or update delayed db writer with mempool config
    pub(crate) fn apply_mempool_config(&self, conf: &MempoolConfig) {
        self.0.apply_mempool_config(conf);
    }

    pub fn manual_ban(&self, peer_id: &PeerId, duration: Duration) -> Result<serde_json::Value> {
        (self.0).manual_ban(peer_id, duration.try_into()?)
    }

    pub fn manual_unban(&self, peer_id: &PeerId, force: bool) -> BoxFuture<'static, Result<()>> {
        self.0.manual_unban(peer_id, force)
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
    fn config(&self) -> &ModeratorConfig;
    fn report_blocking(&self, batch: Vec<JournalEvent>, round_ctx: &RoundCtx);
    fn send_report(&self, data: JournalEvent);
    fn apply_mempool_config(&self, conf: &MempoolConfig);
    fn manual_ban(
        &self,
        peer_id: &PeerId,
        duration: BanConfigDuration,
    ) -> Result<serde_json::Value>;
    fn manual_unban(&self, peer_id: &PeerId, force: bool) -> BoxFuture<'static, Result<()>>;
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
    fn config(&self) -> &ModeratorConfig {
        &self.config
    }
    fn report_blocking(&self, _: Vec<JournalEvent>, _: &RoundCtx) {}
    fn send_report(&self, _: JournalEvent) {}
    fn apply_mempool_config(&self, _: &MempoolConfig) {}
    fn manual_ban(&self, _: &PeerId, _: BanConfigDuration) -> Result<serde_json::Value> {
        Ok(serde_json::json!({}))
    }
    fn manual_unban(&self, _: &PeerId, _: bool) -> BoxFuture<'static, Result<()>> {
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
    network: Network,
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
        for full_item in &full_items {
            meter_event(&full_item.item);
        }
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

    fn manual_ban(
        &self,
        peer_id: &PeerId,
        duration: BanConfigDuration,
    ) -> Result<serde_json::Value> {
        anyhow::ensure!(self.network.peer_id() != peer_id, "cannot ban yourself");
        self.check_init()?;
        let key = RecordKey::new();
        let until = UnixTime::now() + duration.to_time();
        let item = JournalItem::Banned(BanItem {
            peer_id: *peer_id,
            until,
            origin: ItemOrigin::Manual { forced: false }, // force is unapplicable
        });
        self.ban_core.send_ban(key, &item);
        self.delayed_db_writes_tx
            .send(vec![JournalItemFull { key, item }])
            .ok();
        Ok(serde_json::json!({"banned_until_utc_millis": until.millis()}))
    }

    fn manual_unban(&self, peer_id: &PeerId, force: bool) -> BoxFuture<'static, Result<()>> {
        // leave a way to unban yourself just in case of DB transplantation
        match self.check_init() {
            Ok(()) => {
                let rx = self.ban_core.manual_unban(peer_id, force);
                Box::pin(async move { rx.await.map_err(anyhow::Error::from)? })
            }
            Err(e) => Box::pin(futures_util::future::ready(Err(e))),
        }
    }

    fn list_events(&self, count: u16, page: u32, asc: bool) -> Result<Vec<RecordFull>> {
        // no need to check init
        self.journal_store.load_records(count, page, asc)
    }

    fn delete_events(&self, millis: Range<u64>) -> Result<()> {
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
                    for full_item in &full_items {
                        meter_event(&full_item.item);
                    }
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

/// Meters only events that account for bans. To be Used right before DB write.
fn meter_event(item: &JournalItem) {
    if !item.action().is_ban_related() {
        return;
    }
    let JournalItem::Event(event) = item else {
        return;
    };
    let labels = [
        ("kind", format!("{:?}", event.tag())),
        ("peer_id", format!("{:.4}", item.peer_id())),
    ];
    metrics::counter!("tycho_mempool_moderator_event", &labels).increment(1);
}
