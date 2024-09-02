use std::collections::HashMap;
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use everscale_types::models::{BlockId, PrevBlockRef, ShardIdent};
use rand::Rng;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::{QueueDiff, QueueKey, QueueState};
use tycho_block_util::state::is_persistent_state;
use tycho_storage::{BlockHandle, BlocksGcType, Storage};
use tycho_util::metrics::HistogramGuard;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ManualGcTrigger {
    /// Triggers GC for the specified MC block seqno.
    Exact(u32),
    /// Triggers GC for the MC block seqno relative to the latest MC block.
    Distance(u32),
}

#[derive(Clone)]
#[repr(transparent)]
pub struct UpdateStateSubscriber {
    inner: Arc<Inner>,
}

impl UpdateStateSubscriber {
    pub fn new(storage: Storage) -> Self {
        let last_key_block_seqno = storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.id().seqno);

        let last_key_block_utime = storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.meta().gen_utime());

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

        Self {
            inner: Arc::new(Inner {
                tick_tx,
                last_key_block_seqno: AtomicU32::new(last_key_block_seqno),
                last_key_block_utime: AtomicU32::new(last_key_block_utime),

                archives_gc_trigger,
                blocks_gc_trigger,
                states_gc_trigger,

                archive_gc_handle: archives_gc.abort_handle(),
                blocks_gc_handle: blocks_gc.abort_handle(),
                states_gc_handle: states_gc.abort_handle(),
                storage: storage.clone(),
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
            let key_block_utime = block.block().load_info().unwrap().gen_utime;
            let prev_key_block_utime = self.inner.last_key_block_utime.load(Ordering::Relaxed);

            if is_persistent_state(key_block_utime, prev_key_block_utime) && block.id().seqno != 0 {
                let cloned_storage = self.inner.storage.clone();
                let block_clone = block.clone();

                tokio::spawn(async move {
                    if let Err(error) =
                        Self::save_persistent_state(&cloned_storage, block_clone).await
                    {
                        tracing::error!("failed to handle persistent state: {:?}", error);
                    }
                });
            }

            self.inner
                .last_key_block_utime
                .store(key_block_utime, Ordering::Relaxed);

            self.inner
                .last_key_block_seqno
                .store(block.id().seqno, Ordering::Relaxed);
        }

        self.inner.tick_tx.send_replace(Some(Tick {
            last_key_block_seqno: self.inner.last_key_block_seqno.load(Ordering::Relaxed),
            mc_block_id: *block.id(),
        }));
    }

    async fn save_persistent_state(storage: &Storage, block: BlockStuff) -> Result<()> {
        let (queue_result, state_result) = tokio::join!(
            save_queue_persistent_state(storage, &block),
            save_persistent_state(storage, block.id())
        );

        if let Some(error) = queue_result.err().or(state_result.err()) {
            tracing::error!("failed to save persistent state: {error}");
            return Err(error);
        }

        storage
            .persistent_state_storage()
            .clear_old_persistent_states()
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn archives_gc(mut tick_rx: TickRx, mut manual_rx: ManualTriggerRx, storage: Storage) {
        let Some(config) = storage.config().archives_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let persistent_state_keeper = storage.runtime_storage().persistent_state_keeper();
        let mut last_known_pss_seqno = 0;

        let compute_offset = |gen_utime: u32| -> Duration {
            let created_at = std::time::UNIX_EPOCH + Duration::from_secs(gen_utime as _);
            (created_at + config.persistent_state_offset)
                .duration_since(std::time::SystemTime::now())
                .unwrap_or_default()
        };

        'outer: loop {
            // Wait for a target seqno
            let target_seqno = 'seqno: {
                let wait_for_state_fut = async {
                    loop {
                        let mut new_state_found = pin!(persistent_state_keeper.new_state_found());
                        let Some(pss_handle) = persistent_state_keeper.current() else {
                            // Wait for the new state
                            new_state_found.await;
                            continue;
                        };

                        if pss_handle.id().seqno <= last_known_pss_seqno {
                            // Wait for the new state
                            new_state_found.await;
                            continue;
                        }

                        // Wait until it's safe to remove the archives.
                        let time_to_wait = compute_offset(pss_handle.meta().gen_utime());
                        tokio::select! {
                            _ = tokio::time::sleep(time_to_wait) => break pss_handle.id().seqno,
                            _ = &mut new_state_found => continue,
                        }
                    }
                };

                tokio::select! {
                    // Wait for a timepoint until the reset persistent state
                    seqno = wait_for_state_fut => {
                        last_known_pss_seqno = seqno;
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
                .await
            {
                tracing::error!("failed to remove outdated archives: {e:?}");
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn blocks_gc(mut tick_rx: TickRx, mut manual_rx: ManualTriggerRx, storage: Storage) {
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
    async fn states_gc(mut tick_rx: TickRx, mut manual_rx: ManualTriggerRx, storage: Storage) {
        let Some(config) = storage.config().states_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let mut random_offset = config
            .random_offset
            .then(|| rand::thread_rng().gen_range(Duration::ZERO..config.interval));
        let mut last_triggered_at = None::<Instant>;
        let mut sleep_until = None::<Instant>;

        while let Some(source) = wait_with_sleep(&mut tick_rx, &mut manual_rx, sleep_until).await {
            sleep_until = None;

            let Some(tick) = *tick_rx.borrow_and_update() else {
                continue;
            };
            tracing::debug!(?tick);

            let now = Instant::now();

            // Compute the target masterchain block seqno
            let target_seqno = match source {
                // NOTE: Interval is ignored for manual triggers
                GcSource::Manual => {
                    let Some(trigger) = *manual_rx.borrow_and_update() else {
                        continue;
                    };
                    tick.adjust(trigger)
                }
                GcSource::Schedule => {
                    // Make sure to sleep between the ticks
                    if let Some(last) = last_triggered_at {
                        let next_gc = last + config.interval;
                        if next_gc > now {
                            sleep_until = Some(next_gc);
                            continue;
                        }
                    } else if let Some(offset) = random_offset.take() {
                        sleep_until = Some(now + offset);
                        continue;
                    }

                    tick.mc_block_id.seqno
                }
            };

            if target_seqno == 0 {
                continue;
            }

            last_triggered_at = Some(now);

            let _hist = HistogramGuard::begin("tycho_gc_states_time");
            if let Err(e) = storage
                .shard_state_storage()
                .remove_outdated_states(target_seqno)
                .await
            {
                tracing::error!("failed to remove outdated states: {e:?}");
            }

            metrics::gauge!("tycho_gc_states_seqno").set(target_seqno as f64);
        }
    }
}

struct Inner {
    tick_tx: TickTx,
    last_key_block_seqno: AtomicU32,
    last_key_block_utime: AtomicU32,

    archives_gc_trigger: ManualTriggerTx,
    blocks_gc_trigger: ManualTriggerTx,
    states_gc_trigger: ManualTriggerTx,

    blocks_gc_handle: AbortHandle,
    states_gc_handle: AbortHandle,
    archive_gc_handle: AbortHandle,
    storage: Storage,
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

impl StateSubscriber for UpdateStateSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.handle_impl(cx.is_key_block, &cx.block);
        futures_util::future::ready(Ok(()))
    }
}

impl BlockSubscriber for UpdateStateSubscriber {
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

async fn load_top_blocks(
    mc_block_id: &BlockId,
    storage: &Storage,
) -> Result<HashMap<ShardIdent, BlockId>> {
    let mc_state = storage
        .shard_state_storage()
        .load_state(mc_block_id)
        .await?;

    let mut mc_top_blocks_ids = HashMap::<ShardIdent, BlockId>::new();

    for item in mc_state.shards()?.iter() {
        let (shard_ident, shard_descr) = item?;
        if shard_descr.seqno != 0 {
            mc_top_blocks_ids.insert(shard_ident, BlockId {
                shard: shard_ident,
                seqno: shard_descr.seqno,
                root_hash: shard_descr.root_hash,
                file_hash: shard_descr.file_hash,
            });
        }
    }
    mc_top_blocks_ids.insert(mc_block_id.shard, *mc_block_id);

    Ok(mc_top_blocks_ids)
}

async fn load_queue_states(
    mc_block: &BlockStuff,
    storage: &Storage,
) -> Result<Vec<(QueueState, BlockHandle)>> {
    let mc_top_blocks_ids = load_top_blocks(mc_block.id(), storage).await?;

    let mut mc_top_blocks = HashMap::<ShardIdent, BlockStuff>::new();

    // Initialize the map for tracking the minimum processed queue keys
    let mut min_processed_uptos = HashMap::<ShardIdent, QueueKey>::new();

    // Process each shard's top block to find the minimum processed queue keys
    for (shard, top_block_id) in &mc_top_blocks_ids {
        let top_block_handle = storage
            .block_handle_storage()
            .load_handle(top_block_id)
            .context(format!(
                "Failed to load handle for top block: {:?}",
                top_block_id
            ))?;
        let top_block_stuff = storage
            .block_storage()
            .load_block_data(&top_block_handle)
            .await?;

        mc_top_blocks.insert(*shard, top_block_stuff);

        if let Ok(top_block_diff) = storage
            .block_storage()
            .load_queue_diff(&top_block_handle)
            .await
        {
            for (shard_ident, queue_key) in top_block_diff.diff().processed_upto.iter() {
                if queue_key.lt != 0 {
                    min_processed_uptos
                        .entry(*shard_ident)
                        .and_modify(|last_key| {
                            if queue_key < last_key {
                                *last_key = *queue_key;
                            }
                        })
                        .or_insert_with(|| *queue_key);
                }
            }
        } else {
            tracing::warn!(
                "Failed to load queue diff for top block: {:?}",
                top_block_id
            );
        }
    }

    // Track the diff states for each shard
    let mut shard_diffs_state = HashMap::<ShardIdent, (BlockHandle, Vec<QueueDiff>)>::new();

    for (shard_ident, processed_upto_queue_key) in min_processed_uptos {
        let block_id = mc_top_blocks_ids.get(&shard_ident).context(format!(
            "Failed to find top block for shard: {:?}",
            shard_ident
        ))?;
        let mut block_handle = storage
            .block_handle_storage()
            .load_handle(block_id)
            .context(format!(
                "Failed to load handle for top block: {:?}",
                block_id
            ))?;

        let top_block_handle = block_handle.clone();
        let mut diffs = Vec::new();
        let mut block_stuff = mc_top_blocks
            .get(&shard_ident)
            .context(format!(
                "Failed to find top block stuff for shard: {:?}",
                shard_ident
            ))?
            .clone();

        loop {
            let diff = storage
                .block_storage()
                .load_queue_diff(&block_handle)
                .await?;
            diffs.push(diff.diff().clone());

            let prev_block_ref = block_stuff.load_info()?.load_prev_ref()?;

            match prev_block_ref {
                PrevBlockRef::Single(prev_block) => {
                    if prev_block.seqno == 0 || prev_block.end_lt <= processed_upto_queue_key.lt {
                        break;
                    }

                    let block_id = BlockId {
                        shard: shard_ident,
                        seqno: prev_block.seqno,
                        root_hash: prev_block.root_hash,
                        file_hash: prev_block.file_hash,
                    };

                    block_handle = storage
                        .block_handle_storage()
                        .load_handle(&block_id)
                        .context(format!(
                            "Failed to load handle for prev block: {:?}",
                            block_id
                        ))?;

                    block_stuff = storage
                        .block_storage()
                        .load_block_data(&block_handle)
                        .await?;
                }
                PrevBlockRef::AfterMerge { .. } => {
                    return Err(anyhow::anyhow!("Split/Merge is not supported"));
                }
            }
        }

        shard_diffs_state.insert(shard_ident, (top_block_handle, diffs));
    }

    // Convert shard diffs into QueueState objects
    let states = shard_diffs_state
        .into_iter()
        .map(|(shard_ident, (top_block_handle, queue_diffs))| {
            (
                QueueState {
                    shard_ident,
                    seqno: top_block_handle.id().seqno,
                    queue_diffs,
                },
                top_block_handle,
            )
        })
        .collect();

    Ok(states)
}

async fn save_queue_persistent_state(storage: &Storage, block: &BlockStuff) -> Result<()> {
    let states = load_queue_states(block, storage).await?;
    let handle = storage
        .block_handle_storage()
        .load_handle(block.id())
        .ok_or_else(|| anyhow::anyhow!("Block handle not found"))?;

    storage
        .persistent_state_storage()
        .store_queue_state(&handle, states)
        .await
}

async fn save_persistent_state(storage: &Storage, block_id: &BlockId) -> Result<()> {
    let top_blocks = load_top_blocks(block_id, storage).await?;

    for (_, top_block) in top_blocks {
        let root_hash = storage.shard_state_storage().load_state_root(&top_block)?;
        let block_handle = storage
            .block_handle_storage()
            .load_handle(&top_block)
            .ok_or_else(|| anyhow::anyhow!("Block handle not found"))?;
        storage
            .persistent_state_storage()
            .store_state(&block_handle, &root_hash)
            .await?;
    }

    Ok(())
}
