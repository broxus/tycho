use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::Rng;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::block::BlockStuff;
use tycho_types::models::BlockId;
use tycho_util::metrics::HistogramGuard;

use crate::block_strider::subscriber::find_longest_diffs_tail;
use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use crate::storage::{BlocksGcType, CoreStorage};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ManualGcTrigger {
    /// Triggers GC for the specified MC block seqno.
    Exact(u32),
    /// Triggers GC for the MC block seqno relative to the latest MC block.
    Distance(u32),
}

#[derive(Clone)]
#[repr(transparent)]
pub struct GcSubscriber {
    inner: Arc<Inner>,
}

impl GcSubscriber {
    pub fn new(storage: CoreStorage) -> Self {
        let last_key_block_seqno = storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.id().seqno);

        let (tick_tx, tick_rx) = watch::channel(None::<Tick>);

        let (archives_gc_trigger, archives_gc_rx) = watch::channel(None::<ManualGcTrigger>);
        let archives_gc = tokio::spawn(Self::archives_gc(
            tick_rx.clone(),
            archives_gc_rx,
            storage.clone(),
        ));

        let (blocks_gc_trigger, blocks_gc_rx) = watch::channel(None::<ManualGcTrigger>);
        let blocks_gc = tokio::spawn(Self::blocks_gc(
            tick_rx.clone(),
            blocks_gc_rx,
            storage.clone(),
        ));

        let (states_gc_trigger, states_gc_rx) = watch::channel(None::<ManualGcTrigger>);
        let states_gc = tokio::spawn(Self::states_gc(tick_rx, states_gc_rx, storage.clone()));

        let last_known_mc_block = storage.node_state().load_last_mc_block_id();
        if let Some(mc_block_id) = last_known_mc_block {
            tracing::info!(
                %mc_block_id,
                "starting GC subscriber with the last known master block"
            );
            metrics::gauge!("tycho_core_last_mc_block_seqno").set(mc_block_id.seqno as f64);
            tick_tx.send_replace(Some(Tick {
                last_key_block_seqno,
                mc_block_id,
            }));
        }

        Self {
            inner: Arc::new(Inner {
                tick_tx,
                last_key_block_seqno: AtomicU32::new(last_key_block_seqno),

                archives_gc_trigger,
                blocks_gc_trigger,
                states_gc_trigger,

                archive_gc_handle: archives_gc.abort_handle(),
                blocks_gc_handle: blocks_gc.abort_handle(),
                states_gc_handle: states_gc.abort_handle(),
            }),
        }
    }

    pub fn trigger_archives_gc(&self, trigger: ManualGcTrigger) {
        self.inner.archives_gc_trigger.send_replace(Some(trigger));
    }

    pub fn trigger_blocks_gc(&self, trigger: ManualGcTrigger) {
        self.inner.blocks_gc_trigger.send_replace(Some(trigger));
    }

    pub fn trigger_states_gc(&self, trigger: ManualGcTrigger) {
        self.inner.states_gc_trigger.send_replace(Some(trigger));
    }

    fn handle_impl(&self, is_key_block: bool, block: &BlockStuff) {
        if !block.id().is_masterchain() {
            return;
        }

        if is_key_block {
            self.inner
                .last_key_block_seqno
                .store(block.id().seqno, Ordering::Relaxed);
        }

        self.inner.tick_tx.send_replace(Some(Tick {
            last_key_block_seqno: self.inner.last_key_block_seqno.load(Ordering::Relaxed),
            mc_block_id: *block.id(),
        }));
    }

    #[tracing::instrument(skip_all)]
    async fn archives_gc(
        mut tick_rx: TickRx,
        mut manual_rx: ManualTriggerRx,
        storage: CoreStorage,
    ) {
        let Some(config) = storage.config().archives_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let persistent_states = storage.persistent_state_storage();

        let compute_offset = |gen_utime: u32| -> Duration {
            let usable_at = std::time::UNIX_EPOCH
                + Duration::from_secs(gen_utime as _)
                + BlockStuff::BOOT_OFFSET;
            (usable_at + config.persistent_state_offset)
                .duration_since(std::time::SystemTime::now())
                .unwrap_or_default()
        };

        let mut prev_pss_seqno = 0;
        'outer: loop {
            // Wait for a target seqno
            let target_seqno = 'seqno: {
                let wait_for_state_fut = async {
                    loop {
                        let mut new_state_found =
                            pin!(persistent_states.oldest_known_handle_changed());

                        let pss_handle = match persistent_states.load_oldest_known_handle() {
                            Some(handle) if handle.id().seqno > prev_pss_seqno => handle,
                            _ => {
                                // Wait for the new state
                                new_state_found.await;
                                continue;
                            }
                        };

                        // Wait until it's safe to remove the archives.
                        let time_to_wait = compute_offset(pss_handle.gen_utime());
                        tokio::select! {
                            _ = tokio::time::sleep(time_to_wait) => break pss_handle.id().seqno,
                            _ = &mut new_state_found => {},
                        }
                    }
                };

                tokio::select! {
                    // Wait until we can remove archives before that state
                    seqno = wait_for_state_fut => {
                        prev_pss_seqno = seqno;
                        break 'seqno seqno
                    },
                    // Or handle the manual trigger
                    trigger = manual_rx.changed() => {
                        if trigger.is_err() {
                            break 'outer;
                        }
                    }
                }

                let (Some(tick), Some(trigger)) =
                    (*tick_rx.borrow_and_update(), *manual_rx.borrow_and_update())
                else {
                    continue 'outer;
                };

                tick.adjust(trigger)
            };

            if let Err(e) = storage
                .block_storage()
                .remove_outdated_archives(target_seqno)
            {
                tracing::error!("failed to remove outdated archives: {e:?}");
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn blocks_gc(mut tick_rx: TickRx, mut manual_rx: ManualTriggerRx, storage: CoreStorage) {
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
        let mut sleep_until = None::<Instant>;
        let mut known_key_block_seqno = 0;

        // Wait for the tick or manual trigger or exit the loop if any of the channels are closed
        while let Some(source) = wait_with_sleep(&mut tick_rx, &mut manual_rx, sleep_until).await {
            sleep_until = None;

            let Some(tick) = *tick_rx.borrow_and_update() else {
                continue;
            };
            tracing::debug!(?tick);

            // NOTE: Track the last mc block seqno since we cannot rely on the broadcasted block.
            // It may be updated faster than the iteration of the GC manager.
            let has_new_key_block = tick.last_key_block_seqno > known_key_block_seqno;
            known_key_block_seqno = tick.last_key_block_seqno;

            let target_seqno = match (source, config.ty) {
                (GcSource::Manual, _) => {
                    let Some(trigger) = *manual_rx.borrow_and_update() else {
                        continue;
                    };

                    // Compute the target masterchain block seqno
                    let target_seqno = tick.adjust(trigger);
                    if target_seqno == 0 {
                        continue;
                    }

                    // Don't debounce manual triggers, but update the last trigger time
                    last_tiggered_at = Some(Instant::now());
                    target_seqno
                }
                (
                    GcSource::Schedule,
                    BlocksGcType::BeforeSafeDistance {
                        safe_distance,
                        min_interval,
                    },
                ) => {
                    // TODO: Must be in sync with the largest possible archive size (in mc blocks).
                    const MIN_SAFE_DISTANCE: u32 = 100;

                    let tail_len = find_longest_diffs_tail(tick.mc_block_id, &storage).await;

                    let tail_len = tail_len.unwrap_or_else(|e| {
                        tracing::error!(?e, "failed to find longest diffs tail");
                        0
                    }) as u32;

                    metrics::gauge!("tycho_core_blocks_gc_tail_len").set(tail_len);

                    tracing::info!(tail_len, "found longest diffs tail");

                    let safe_distance = [safe_distance, MIN_SAFE_DISTANCE, tail_len + 1]
                        .into_iter()
                        .max()
                        .unwrap();

                    // Compute the target masterchain block seqno
                    let target_seqno = match tick.mc_block_id.seqno.checked_sub(safe_distance) {
                        // Skip GC for the first N blocks
                        None | Some(0) => continue,
                        Some(seqno) => seqno,
                    };

                    // Debounce GC
                    if let Some(last) = last_tiggered_at {
                        if last.elapsed() < min_interval {
                            // Sleep until the desired interval
                            // AND continue to wait for the next trigger
                            sleep_until = Some(last + min_interval);
                            continue;
                        }
                    }

                    // NOTE: You should update this in other branches as well,
                    // if we want to debounce other types of GC.
                    last_tiggered_at = Some(Instant::now());
                    target_seqno
                }
                (GcSource::Schedule, BlocksGcType::BeforePreviousKeyBlock) => {
                    if !has_new_key_block {
                        continue;
                    }

                    // Find a key block before the last key block from the trigger
                    let target_seqno = tick.last_key_block_seqno;
                    match block_handles.find_prev_key_block(target_seqno) {
                        Some(handle) => handle.id().seqno,
                        None => {
                            tracing::warn!(target_seqno, "previous key block not found");
                            continue;
                        }
                    }
                }
                (GcSource::Schedule, BlocksGcType::BeforePreviousPersistentState) => {
                    if !has_new_key_block {
                        continue;
                    }

                    // Find a persistent block before the last key block from the trigger
                    let target_seqno = tick.last_key_block_seqno;
                    match block_handles.find_prev_persistent_key_block(target_seqno) {
                        Some(handle) => handle.id().seqno,
                        None => {
                            tracing::warn!(target_seqno, "previous persistent block not found");
                            continue;
                        }
                    }
                }
            };

            metrics::gauge!("tycho_core_mc_blocks_gc_lag")
                .set(tick.mc_block_id.seqno.saturating_sub(target_seqno));

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
    async fn states_gc(mut tick_rx: TickRx, mut manual_rx: ManualTriggerRx, storage: CoreStorage) {
        let Some(config) = storage.config().states_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!(?config, "manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let mut random_offset = config
            .random_offset
            .then(|| rand::rng().random_range(Duration::ZERO..config.interval));
        let mut last_triggered_at = None::<Instant>;
        let mut sleep_until = None::<Instant>;

        while let Some(source) = wait_with_sleep(&mut tick_rx, &mut manual_rx, sleep_until).await {
            sleep_until = None;
            if let GcSource::Manual = source {
                tracing::info!("manual states gc triggered");
            }

            let Some(tick) = *tick_rx.borrow_and_update() else {
                tracing::debug!("no tick available, continuing");
                continue;
            };
            tracing::debug!(?tick, "states gc tick");

            let now = Instant::now();

            // Compute the target masterchain block seqno
            let target_seqno = match source {
                // NOTE: Interval is ignored for manual triggers
                GcSource::Manual => {
                    let Some(trigger) = *manual_rx.borrow_and_update() else {
                        tracing::debug!("no manual trigger available, continuing");
                        continue;
                    };
                    tracing::info!(seqno = tick.adjust(trigger), "manual GC triggered");
                    tick.adjust(trigger)
                }
                GcSource::Schedule => {
                    // Make sure to sleep between the ticks
                    if let Some(last) = last_triggered_at {
                        let next_gc = last + config.interval;
                        if next_gc > now {
                            sleep_until = Some(next_gc);
                            let sleep_duration = next_gc - now;
                            tracing::debug!(
                                duration = sleep_duration.as_secs_f64(),
                                "sleeping until next GC"
                            );
                            continue;
                        }
                    } else if let Some(offset) = random_offset.take() {
                        sleep_until = Some(now + offset);
                        let sleep_duration = offset;
                        tracing::debug!(
                            duration = sleep_duration.as_secs_f64(),
                            "sleeping with random offset"
                        );
                        continue;
                    }

                    tracing::info!(seqno = tick.mc_block_id.seqno, "scheduled GC");
                    tick.mc_block_id.seqno
                }
            };

            if target_seqno == 0 {
                tracing::warn!("target seqno is 0, skipping GC");
                continue;
            }

            last_triggered_at = Some(now);
            tracing::info!("starting GC for target seqno: {}", target_seqno);

            let hist = HistogramGuard::begin("tycho_gc_states_time");

            if let Err(e) = storage
                .shard_state_storage()
                .remove_outdated_states(target_seqno)
                .await
            {
                tracing::error!("failed to remove outdated states: {e:?}");
            }

            let took = hist.finish();
            tracing::info!(
                duration = took.as_secs_f64(),
                "completed GC for target seqno: {}",
                target_seqno
            );
        }
    }
}

struct Inner {
    tick_tx: TickTx,
    last_key_block_seqno: AtomicU32,

    archives_gc_trigger: ManualTriggerTx,
    blocks_gc_trigger: ManualTriggerTx,
    states_gc_trigger: ManualTriggerTx,

    blocks_gc_handle: AbortHandle,
    states_gc_handle: AbortHandle,
    archive_gc_handle: AbortHandle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.blocks_gc_handle.abort();
        self.states_gc_handle.abort();
        self.archive_gc_handle.abort();
    }
}

#[derive(Debug, Clone)]
enum GcSource {
    Schedule,
    Manual,
}

#[derive(Debug, Clone, Copy)]
struct Tick {
    pub mc_block_id: BlockId,
    pub last_key_block_seqno: u32,
}

impl Tick {
    fn adjust(&self, trigger: ManualGcTrigger) -> u32 {
        match trigger {
            ManualGcTrigger::Exact(seqno) => seqno,
            ManualGcTrigger::Distance(distance) => self.mc_block_id.seqno.saturating_sub(distance),
        }
    }
}

type TickTx = watch::Sender<Option<Tick>>;
type TickRx = watch::Receiver<Option<Tick>>;

type ManualTriggerTx = watch::Sender<Option<ManualGcTrigger>>;
type ManualTriggerRx = watch::Receiver<Option<ManualGcTrigger>>;

async fn wait_with_sleep(
    tick_rx: &mut TickRx,
    manual_rx: &mut ManualTriggerRx,
    sleep_until: Option<Instant>,
) -> Option<GcSource> {
    use futures_util::future::Either;

    let fut = match sleep_until {
        // Ignore all ticks if we need to sleep
        Some(deadline) => Either::Left(async move {
            tokio::time::sleep_until(deadline.into()).await;
            Some(GcSource::Schedule)
        }),
        // Wait for the tick otherwise
        None => Either::Right(async {
            let res = tick_rx.changed().await;
            res.is_ok().then_some(GcSource::Schedule)
        }),
    };

    tokio::select! {
        res = fut => res,
        trigger = manual_rx.changed() => {
            trigger.is_ok().then_some(GcSource::Manual)
        },
    }
}

impl StateSubscriber for GcSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.handle_impl(cx.is_key_block, &cx.block);
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
        self.handle_impl(cx.is_key_block, &cx.block);
        futures_util::future::ready(Ok(()))
    }
}
