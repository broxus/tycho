use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic};

use tokio::sync::mpsc;
use tycho_network::Network;
use tycho_util::futures::JoinTask;

use crate::effects::{Ctx, RoundCtx};
use crate::engine::{MempoolConfig, NodeConfig};
use crate::models::UnixTime;
use crate::moderator::ban_core::BanCore;
use crate::moderator::event::{Event, EventData};
use crate::storage::{EventKey, EventStore, MempoolDb};

/// Must outlive [`crate::engine::lifecycle::EngineSession`] just like opened DB
#[derive(Clone)]
pub struct Moderator(Arc<dyn ModeratorTrait>);

impl Moderator {
    #[cfg(any(test, feature = "test"))]
    pub fn new_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn new(network: &Network, mempool_db: Arc<MempoolDb>, version: &'static str) -> Self {
        let event_store = EventStore::new(mempool_db);

        // unbounded because we don't know the capacity, anyway the queue won't be long
        let (delayed_db_writes_tx, delayed_db_writes_rx) = mpsc::unbounded_channel();
        // unbounded because of non-async infallible `send()`, though hardly ever will send updates
        let (conf_tx, conf_rx) = mpsc::unbounded_channel();

        let start_event = Event {
            key: EventKey::new(), // always starts with zero
            data: EventData::NodeStarted(*network.peer_id(), version.to_owned()),
        };
        delayed_db_writes_tx.send(start_event).ok();

        let ban_core = BanCore::new(network, &delayed_db_writes_tx);

        Self(Arc::new(ModeratorInner {
            delayed_db_writes_tx,
            ban_core,
            event_store: event_store.clone(),
            is_init: AtomicBool::new(false),
            conf_tx,
            _delayed_db_writer: JoinTask::new(delayed_db_write(
                event_store,
                delayed_db_writes_rx,
                conf_rx,
            )),
        }))
    }

    pub fn init_blocking(&self) {
        self.0.init_blocking();
    }

    /// Blocking sequential write to DB of batch of events
    pub fn report_blocking(&self, batch: &[EventData], round_ctx: &RoundCtx) {
        self.0.report_blocking(batch, round_ctx);
    }

    /// Event is channelled to a separate worker that writes events to DB one-by-one
    pub fn send_report(&self, data: EventData) {
        self.0.send_report(data);
    }

    /// Init or update delayed db writer with mempool config
    pub fn apply_config(&self, conf: &MempoolConfig) {
        self.0.apply_config(conf);
    }
}

trait ModeratorTrait: Send + Sync {
    fn init_blocking(&self);
    fn report_blocking(&self, batch: &[EventData], round_ctx: &RoundCtx);
    fn send_report(&self, data: EventData);
    fn apply_config(&self, conf: &MempoolConfig);
}

#[cfg(any(test, feature = "test"))]
impl ModeratorTrait for () {
    fn init_blocking(&self) {}
    fn report_blocking(&self, _: &[EventData], _: &RoundCtx) {}
    fn send_report(&self, _: EventData) {}
    fn apply_config(&self, _: &MempoolConfig) {}
}

struct ModeratorInner {
    delayed_db_writes_tx: mpsc::UnboundedSender<Event>,
    ban_core: BanCore,
    event_store: EventStore,
    /// because gets created once in adapter and may outlive engine session,
    /// but gets initialized with engine init routine, so must not be init twice
    is_init: AtomicBool,
    conf_tx: mpsc::UnboundedSender<MempoolConfig>,
    _delayed_db_writer: JoinTask<()>,
}

impl ModeratorTrait for ModeratorInner {
    fn init_blocking(&self) {
        if self.is_init.swap(true, atomic::Ordering::Relaxed) {
            tracing::info!("moderator already init");
            return;
        }
        let since = UnixTime::now() - NodeConfig::get().bans.max_known_duration().to_time();
        let short_events = self
            .event_store
            .load_short_events(since)
            .expect("load events on init");
        for (key, short) in short_events {
            self.ban_core.maybe_ban(key, short);
        }
    }

    fn report_blocking(&self, batch: &[EventData], round_ctx: &RoundCtx) {
        let mut keyed_batch = Vec::with_capacity(batch.len());

        for data in batch {
            let key = EventKey::new();
            if let Some(short) = data.to_short() {
                self.ban_core.maybe_ban(key, short);
            }
            keyed_batch.push((key, data));
        }

        self.event_store
            .store_events(&keyed_batch, round_ctx.conf())
            .expect("store event");
    }

    fn send_report(&self, data: EventData) {
        let key = EventKey::new();
        if let Some(short) = data.to_short() {
            self.ban_core.maybe_ban(key, short);
        }
        // expect receiver is dropped only at shutdown
        self.delayed_db_writes_tx.send(Event { key, data }).ok();
    }

    fn apply_config(&self, conf: &MempoolConfig) {
        self.conf_tx.send(conf.clone()).ok();
    }
}

async fn delayed_db_write(
    event_store: EventStore,
    mut delayed_db_writes_rx: mpsc::UnboundedReceiver<Event>,
    mut conf_rx: mpsc::UnboundedReceiver<MempoolConfig>,
) {
    let Some(mut conf) = conf_rx.recv().await else {
        tracing::warn!("Mempool config channel closed before init in Moderator DB Writer");
        return;
    };
    loop {
        tokio::select! {
            Some(new_conf) = conf_rx.recv() => conf = new_conf,
            Some(event) = delayed_db_writes_rx.recv() => {
                event_store
                    .store_events(&[(event.key, &event.data)], &conf)
                    .expect("store delayed event");
                }
            else => unreachable!("unhandled branch in mempool delayed event db writer")
        }
    }
}
