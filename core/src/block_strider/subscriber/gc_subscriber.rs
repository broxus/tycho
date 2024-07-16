use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use everscale_types::models::BlockId;
use rand::Rng;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tl_proto::TlWrite;
use tokio::select;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::AbortHandle;
use tycho_block_util::block::BlockStuff;
use tycho_storage::{BlocksGcType, Storage};
use tycho_util::metrics::HistogramGuard;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[derive(Clone)]
#[repr(transparent)]
pub struct GcSubscriber {
    inner: Arc<Inner>,
}

impl GcSubscriber {
    pub fn new(
        storage: Storage,
        mut manual_gc_trigger: watch::Receiver<Option<ManualGcTrigger>>,
    ) -> Self {
        let last_key_block_seqno = storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.id().seqno);

        let (trigger_tx, trigger_rx) = watch::channel(None::<GcTrigger>);

        let blocks_gc = tokio::spawn(Self::blocks_gc(trigger_rx.clone(), storage.clone()));
        let states_gc = tokio::spawn(Self::states_gc(trigger_rx, storage.clone()));
        let archives_gc = tokio::spawn(Self::archives_gc(storage.clone()));

        let manual_gc = tokio::spawn(Self::manual_gc(
            manual_gc_trigger,
            trigger_tx.clone(),
            storage,
        ));

        Self {
            inner: Arc::new(Inner {
                trigger_tx,
                last_key_block_seqno: AtomicU32::new(last_key_block_seqno),
                handle_block_task: blocks_gc.abort_handle(),
                handle_state_task: states_gc.abort_handle(),
                archives_handle: archives_gc.abort_handle(),
                manual_gc_handle: manual_gc.abort_handle(),
            }),
        }
    }

    pub fn handle(&self, is_key_block: bool, block: &BlockStuff) {
        if !block.id().is_masterchain() {
            return;
        }

        if is_key_block {
            self.inner
                .last_key_block_seqno
                .store(block.id().seqno, Ordering::Relaxed);
        }

        self.inner.trigger_tx.send_replace(Some(GcTrigger {
            last_key_block_seqno: self.inner.last_key_block_seqno.load(Ordering::Relaxed),
            mc_block_id: *block.id(),
        }));
    }

    #[tracing::instrument(skip_all)]
    async fn manual_gc(
        mut manual_gc_trigger: watch::Receiver<Option<ManualGcTrigger>>,
        trigger_tx: TriggerTx,
        storage: Storage,
    ) {
        while manual_gc_trigger.changed().await.is_ok() {
            let Some(trigger) = manual_gc_trigger.borrow_and_update().clone() else {
                continue;
            };

            tracing::debug!(?trigger);

            let last_mc = storage.node_state().load_last_mc_block_id().unwrap_or_else(|| {
                tracing::warn!("No mc block found. BeforePreviousKeyBlock and BeforePreviousPersistentState gc would not be performed");
                BlockId::default()
            });

            let last_key_block_seqno = match storage.block_handle_storage().find_last_key_block() {
                Some(key_block) => key_block.id().seqno,
                None => {
                    tracing::warn!("No mc block found. BeforePreviousKeyBlock and BeforePreviousPersistentState gc probably would not be performed");
                    0
                }
            };

            let gc_trigger = GcTrigger {
                mc_block_id: last_mc,
                last_key_block_seqno,
            };

            match trigger {
                ManualGcTrigger::Blocks | ManualGcTrigger::States => {
                    if let Err(e) = trigger_tx.send(Some(gc_trigger)) {
                        tracing::error!("Failed to send signal for manual GC. {e:?}");
                    }
                }
                ManualGcTrigger::Archives => {}
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn archives_gc(storage: Storage) {
        let Some(config) = storage.config().archives_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let persistent_state_keeper = storage.runtime_storage().persistent_state_keeper();

        loop {
            let mut new_state_found = pin!(persistent_state_keeper.new_state_found());
            let Some(pss_handle) = persistent_state_keeper.current() else {
                new_state_found.await;
                continue;
            };

            // Compute the wait duration until the safe time point
            let time_to_wait = {
                let gen_utime = pss_handle.meta().gen_utime();
                let created_at = std::time::UNIX_EPOCH + Duration::from_secs(gen_utime as _);
                (created_at + config.persistent_state_offset)
                    .duration_since(std::time::SystemTime::now())
                    .unwrap_or_default()
            };

            tokio::select! {
                _ = tokio::time::sleep(time_to_wait) => {},
                _ = &mut new_state_found => continue,
            }

            if let Err(e) = storage
                .block_storage()
                .remove_outdated_archives(pss_handle.id().seqno)
                .await
            {
                tracing::error!("failed to remove outdated archives: {e:?}");
            }

            new_state_found.await;
        }
    }

    #[tracing::instrument(skip_all)]
    async fn blocks_gc(mut trigger_rx: TriggerRx, storage: Storage) {
        let Some(config) = storage.config().blocks_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let block_handles = storage.block_handle_storage();

        let mut last_tiggered_at = None::<Instant>;
        let mut known_key_block_seqno = 0;

        while trigger_rx.changed().await.is_ok() {
            let Some(trigger) = trigger_rx.borrow_and_update().clone() else {
                continue;
            };
            tracing::debug!(?trigger);

            // NOTE: Track the last mc block seqno since we cannot rely on the broadcasted block.
            // It may be updated faster than the iteration of the GC manager.
            let has_new_key_block = trigger.last_key_block_seqno > known_key_block_seqno;
            known_key_block_seqno = trigger.last_key_block_seqno;

            let target_seqno = match config.ty {
                BlocksGcType::BeforeSafeDistance {
                    safe_distance,
                    min_interval,
                } => {
                    // Compute the target masterchain block seqno
                    let target_seqno = match trigger.mc_block_id.seqno.checked_sub(safe_distance) {
                        // Skip GC for the first N blocks
                        None | Some(0) => continue,
                        Some(seqno) => seqno,
                    };

                    // Debounce GC
                    if let Some(last) = last_tiggered_at {
                        if last.elapsed() < min_interval {
                            // Sleep until the desired interval
                            // AND continue to wait for the next trigger
                            tokio::time::sleep_until((last + min_interval).into()).await;
                            continue;
                        }
                    }

                    // NOTE: You should update this in other branches as well,
                    // if we want to debounce other types of GC.
                    last_tiggered_at = Some(Instant::now());
                    target_seqno
                }
                BlocksGcType::BeforePreviousKeyBlock => {
                    if !has_new_key_block {
                        continue;
                    }

                    // Find a key block before the last key block from the trigger
                    let target_seqno = trigger.last_key_block_seqno;
                    match block_handles.find_prev_key_block(target_seqno) {
                        Some(handle) => handle.id().seqno,
                        None => {
                            tracing::warn!(target_seqno, "previous key block not found");
                            continue;
                        }
                    }
                }
                BlocksGcType::BeforePreviousPersistentState => {
                    if !has_new_key_block {
                        continue;
                    }

                    // Find a persistent block before the last key block from the trigger
                    let target_seqno = trigger.last_key_block_seqno;
                    match block_handles.find_prev_persistent_key_block(target_seqno) {
                        Some(handle) => handle.id().seqno,
                        None => {
                            tracing::warn!(target_seqno, "previous persistent block not found");
                            continue;
                        }
                    }
                }
            };

            if let Err(e) = storage
                .block_storage()
                .remove_outdated_blocks(target_seqno, config.max_blocks_per_batch)
                .await
            {
                tracing::error!("failed to remove outdated blocks: {e:?}");
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn states_gc(mut trigger_rx: TriggerRx, storage: Storage) {
        use tokio::time;
        let Some(config) = storage.config().states_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let mut interval = time::interval(config.interval);
        let mut last_triggered_at = None;

        loop {
            // either the interval has ticked or a new trigger has arrived
            select! {
                _ = interval.tick() => {},
                Ok(_) = trigger_rx.changed() => {},
                else => break,
            }

            let now = Instant::now();

            if let Some(last) = last_triggered_at {
                let next_gc: Instant = last + config.interval;
                if next_gc > now {
                    time::sleep_until(next_gc.into()).await;
                }
            } else if config.random_offset {
                let offset = rand::thread_rng().gen_range(Duration::ZERO..config.interval);
                time::sleep(offset).await;
            }

            last_triggered_at = Some(Instant::now());

            let Some(trigger) = trigger_rx.borrow_and_update().clone() else {
                continue;
            };
            tracing::debug!(?trigger);

            let _hist = HistogramGuard::begin("tycho_gc_states_time");
            if let Err(e) = storage
                .shard_state_storage()
                .remove_outdated_states(trigger.mc_block_id.seqno)
                .await
            {
                tracing::error!("failed to remove outdated states: {e:?}");
            }
            metrics::gauge!("tycho_gc_states_seqno").set(trigger.mc_block_id.seqno as f64);
        }
    }
}

struct Inner {
    trigger_tx: TriggerTx,
    last_key_block_seqno: AtomicU32,

    handle_block_task: AbortHandle,
    handle_state_task: AbortHandle,
    archives_handle: AbortHandle,

    manual_gc_handle: AbortHandle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.handle_block_task.abort();
        self.handle_state_task.abort();
        self.archives_handle.abort();

        self.manual_gc_handle.abort();
    }
}

#[derive(Debug, Clone)]
pub struct GcTrigger {
    pub mc_block_id: BlockId,
    pub last_key_block_seqno: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManualGcTrigger {
    Archives,
    Blocks,
    States,
}

pub type TriggerTx = watch::Sender<Option<GcTrigger>>;
pub type TriggerRx = watch::Receiver<Option<GcTrigger>>;

impl StateSubscriber for GcSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.handle(cx.is_key_block, &cx.block);
        futures_util::future::ready(Ok(()))
    }
}

impl BlockSubscriber for GcSubscriber {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        self.handle(cx.is_key_block, &cx.block);
        futures_util::future::ready(Ok(()))
    }
}
