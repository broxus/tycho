use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic};

use tokio::sync::mpsc;
use tycho_network::Network;
use tycho_util::futures::JoinTask;

use crate::effects::{Ctx, RoundCtx};
use crate::engine::{MempoolConfig, NodeConfig};
use crate::models::UnixTime;
use crate::moderator::ban::core::BanCore;
use crate::moderator::{JournalEvent, JournalRecord, RecordData, RecordKey};
use crate::storage::{JournalStore, MempoolDb};

/// Must outlive [`crate::engine::lifecycle::EngineSession`] just like opened DB
#[derive(Clone)]
pub struct Moderator(Arc<dyn ModeratorTrait>);

impl Moderator {
    #[cfg(any(test, feature = "test"))]
    pub fn new_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn new(network: &Network, mempool_db: Arc<MempoolDb>, version: &'static str) -> Self {
        let journal_store = JournalStore::new(mempool_db);

        // unbounded because we don't know the capacity, anyway the queue won't be long
        let (delayed_db_writes_tx, delayed_db_writes_rx) = mpsc::unbounded_channel();
        // unbounded because of non-async infallible `send()`, though hardly ever will send updates
        let (conf_tx, conf_rx) = mpsc::unbounded_channel();

        let start_event = JournalRecord {
            key: RecordKey::new(), // always starts with zero
            data: RecordData::NodeStarted(*network.peer_id(), version.to_owned()),
        };
        delayed_db_writes_tx.send(vec![start_event]).ok();

        let ban_core = BanCore::new(network, &delayed_db_writes_tx);

        Self(Arc::new(ModeratorInner {
            delayed_db_writes_tx,
            ban_core,
            journal_store: journal_store.clone(),
            is_init: AtomicBool::new(false),
            conf_tx,
            _delayed_db_writer: JoinTask::new(ModeratorInner::delayed_db_write(
                journal_store,
                delayed_db_writes_rx,
                conf_rx,
            )),
        }))
    }

    pub fn init_blocking(&self) {
        self.0.init_blocking();
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
    pub fn apply_config(&self, conf: &MempoolConfig) {
        self.0.apply_config(conf);
    }
}

trait ModeratorTrait: Send + Sync {
    fn init_blocking(&self);
    fn report_blocking(&self, batch: Vec<JournalEvent>, round_ctx: &RoundCtx);
    fn send_report(&self, data: JournalEvent);
    fn apply_config(&self, conf: &MempoolConfig);
}

#[cfg(any(test, feature = "test"))]
impl ModeratorTrait for () {
    fn init_blocking(&self) {}
    fn report_blocking(&self, _: Vec<JournalEvent>, _: &RoundCtx) {}
    fn send_report(&self, _: JournalEvent) {}
    fn apply_config(&self, _: &MempoolConfig) {}
}

struct ModeratorInner {
    delayed_db_writes_tx: mpsc::UnboundedSender<Vec<JournalRecord>>,
    ban_core: BanCore,
    journal_store: JournalStore,
    /// because gets created once in adapter and may outlive engine session,
    /// but gets initialized with engine init routine, so must not be init twice
    is_init: AtomicBool,
    conf_tx: mpsc::UnboundedSender<MempoolConfig>,
    _delayed_db_writer: JoinTask<()>, // moderator outlives mempool session(s)
}

impl ModeratorTrait for ModeratorInner {
    fn init_blocking(&self) {
        if self.is_init.swap(true, atomic::Ordering::Relaxed) {
            tracing::info!("moderator already init");
            return;
        }
        let since = UnixTime::now() - NodeConfig::get().bans.max_duration().to_time();
        let short_events = self
            .journal_store
            .load_records_short(since)
            .expect("load events on init");
        let new_records = self.ban_core.restore(&short_events);
        self.delayed_db_writes_tx.send(new_records).ok();
    }

    fn report_blocking(&self, events: Vec<JournalEvent>, round_ctx: &RoundCtx) {
        let records = self.events_to_records_and_maybe_ban(events);
        self.journal_store
            .store_records(&records, round_ctx.conf())
            .expect("store records");
    }

    fn send_report(&self, event: JournalEvent) {
        let records = self.events_to_records_and_maybe_ban(vec![event]);
        // expect receiver is dropped only at shutdown
        self.delayed_db_writes_tx.send(records).ok();
    }

    fn apply_config(&self, conf: &MempoolConfig) {
        self.conf_tx.send(conf.clone()).ok();
    }
}

impl ModeratorInner {
    fn events_to_records_and_maybe_ban(&self, events: Vec<JournalEvent>) -> Vec<JournalRecord> {
        let mut records = Vec::with_capacity(events.len());
        let mut new_bans = Vec::new();

        // first: assign keys with `seq_no` inside to external events
        for event in events {
            let key = RecordKey::new();
            if let Some(ban) = self.ban_core.maybe_ban_data(key, &event) {
                new_bans.push(ban);
            };
            let data = RecordData::Event(event);
            records.push(JournalRecord { key, data });
        }

        // second: assign keys with `seq_no` inside to newly generated ban records
        for data in new_bans {
            let key = RecordKey::new();
            self.ban_core.send_ban_data(key, &data);
            records.push(JournalRecord { key, data });
        }

        // now records are in historical order
        records
    }

    async fn delayed_db_write(
        journal_store: JournalStore,
        mut delayed_db_writes_rx: mpsc::UnboundedReceiver<Vec<JournalRecord>>,
        mut conf_rx: mpsc::UnboundedReceiver<MempoolConfig>,
    ) {
        let Some(mut conf) = conf_rx.recv().await else {
            tracing::warn!("Mempool config channel closed before init in Moderator DB Writer");
            return;
        };
        loop {
            tokio::select! {
                Some(new_conf) = conf_rx.recv() => conf = new_conf,
                Some(records) = delayed_db_writes_rx.recv() => {
                    journal_store
                        .store_records(&records, &conf)
                        .expect("store delayed journal record");
                    }
                else => unreachable!("unhandled branch in mempool delayed journal db writer")
            }
        }
    }
}
