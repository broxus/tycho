use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use everscale_types::models::BlockId;
use rand::Rng;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::{broadcast, watch};
use tokio::task::AbortHandle;
use tycho_block_util::block::BlockStuff;
use tycho_storage::{BlockHandle, BlocksGcType, Storage};
use tycho_util::metrics::HistogramGuard;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[derive(Debug, Clone)]
enum GcTriggerSource {
    Automatic {
        data: Option<GcTrigger>,
    },
    Manual {
        data: ManualGcTrigger,
        last_known_mc_seqno: Option<u32>,
    },
}

#[derive(Clone)]
#[repr(transparent)]
pub struct GcSubscriber {
    inner: Arc<Inner>,
}

impl GcSubscriber {
    pub fn new(storage: Storage, manual_gc_sender: broadcast::Sender<ManualGcTrigger>) -> Self {
        let last_key_block_seqno = storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.id().seqno);

        let (trigger_tx, trigger_rx) = watch::channel(None::<GcTrigger>);

        let blocks_gc = tokio::spawn(Self::blocks_gc(
            manual_gc_sender.subscribe(),
            trigger_rx.clone(),
            storage.clone(),
        ));
        let states_gc = tokio::spawn(Self::states_gc(
            manual_gc_sender.subscribe(),
            trigger_rx,
            storage.clone(),
        ));
        let archives_gc = tokio::spawn(Self::archives_gc(
            manual_gc_sender.subscribe(),
            storage.clone(),
        ));

        Self {
            inner: Arc::new(Inner {
                trigger_tx,
                last_key_block_seqno: AtomicU32::new(last_key_block_seqno),
                handle_block_task: blocks_gc.abort_handle(),
                handle_state_task: states_gc.abort_handle(),
                archives_handle: archives_gc.abort_handle(),
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
    async fn archives_gc(
        mut manual_gc_trigger: broadcast::Receiver<ManualGcTrigger>,
        storage: Storage,
    ) {
        let Some(config) = storage.config().archives_gc else {
            tracing::warn!("manager disabled");
            return;
        };
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let persistent_state_keeper = storage.runtime_storage().persistent_state_keeper();

        enum HandleSource {
            PersistentStateKeeper { handle: BlockHandle },
            ManualSelection { seqno: u32, distance: u32 },
        }

        let mut source: Option<HandleSource> = None;

        loop {
            let mut new_state_found = pin!(persistent_state_keeper.new_state_found());

            tokio::select! {
                _ = &mut new_state_found => {
                    if let Some(handle) = persistent_state_keeper.current() {
                        source = Some(HandleSource::PersistentStateKeeper {handle});
                    } else {
                        continue;
                    };
                },
                tr = manual_gc_trigger.recv() => {
                    match tr {
                        Ok(ManualGcTrigger::Archives {ty}) => {
                            tracing::debug!("Archives GC manual trigger received {:?}", &ty);
                            if let Ok(Some((seqno, distance))) = Self::get_mc_block_seqno_manual(ty, storage.clone()).await {
                                source = Some(HandleSource::ManualSelection {seqno, distance});
                            } else {
                                continue;
                            };
                        },
                        Err(e) => {
                            tracing::error!("Failed to read manual gv trigger broadcast {e:?}");
                            continue;
                        }
                        _ => continue,
                    }
                }
            }

            let seqno = match source {
                Some(HandleSource::PersistentStateKeeper { handle }) => {
                    // Compute the wait duration until the safe time point
                    let time_to_wait = {
                        let gen_utime = handle.meta().gen_utime();
                        let created_at =
                            std::time::UNIX_EPOCH + Duration::from_secs(gen_utime as _);
                        (created_at + config.persistent_state_offset)
                            .duration_since(std::time::SystemTime::now())
                            .unwrap_or_default()
                    };

                    tokio::select! {
                        _ = tokio::time::sleep(time_to_wait) => handle.id().seqno,
                        _ = &mut new_state_found => continue,
                    }
                }
                Some(HandleSource::ManualSelection { seqno, distance }) => {
                    tracing::info!(target: "gc-subscriber", "Manual seqno selection...");
                    seqno.saturating_sub(distance)
                }
                _ => continue,
            };

            tracing::debug!("Archives GC calculated target seqno {}", seqno);
            if let Err(e) = storage
                .block_storage()
                .remove_outdated_archives(seqno)
                .await
            {
                tracing::error!("failed to remove outdated archives: {e:?}");
            }
        }
    }

    async fn get_mc_block_seqno_manual(
        ty: TriggerType,
        storage: Storage,
    ) -> Result<Option<(u32, u32)>> {
        match ty {
            TriggerType::Distance(d) => match storage.node_state().load_last_mc_block_id() {
                Some(last) => Ok(storage
                    .block_handle_storage()
                    .load_handle(&last)
                    .map(|x| (x.id().seqno, d))),
                None => Ok(None),
            },
            TriggerType::McSeqno(mc_seqno) => {
                return match storage.node_state().load_last_mc_block_id() {
                    Some(block) => {
                        let state = storage.shard_state_storage().load_state(&block).await?;
                        let Some((_, mc_block_ref)) =
                            state.state_extra()?.prev_blocks.get(mc_seqno)?
                        else {
                            anyhow::bail!("Failed to find mc block ref")
                        };
                        Ok(Some((mc_block_ref.block_ref.seqno, 0)))
                    }
                    _ => Ok(None),
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn blocks_gc(
        mut manual_gc_trigger: broadcast::Receiver<ManualGcTrigger>,
        mut trigger_rx: TriggerRx,
        storage: Storage,
    ) {
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

        loop {
            let source = tokio::select! {
                _ = trigger_rx.changed() => {
                    let data = trigger_rx.borrow_and_update().clone();
                    GcTriggerSource::Automatic {data}
                }
                Ok(data) = manual_gc_trigger.recv() => {
                    tracing::debug!("Blocks GC manual trigger received {:?}", &data);
                    let last_known_mc_seqno = trigger_rx.borrow_and_update().clone().map(|x| x.mc_block_id.seqno);
                    GcTriggerSource::Manual {data, last_known_mc_seqno}
                }
                else => {
                    tracing::warn!("Failed to read determine gc trigger source");
                    continue;
                }
            };

            let target_seqno = match (source, config.ty) {
                (
                    GcTriggerSource::Manual {
                        data:
                            ManualGcTrigger::Blocks {
                                ty: TriggerType::Distance(d),
                            },
                        last_known_mc_seqno: Some(mc_seqno),
                    },
                    _,
                ) => match mc_seqno.checked_sub(d) {
                    None | Some(0) => continue,
                    Some(seqno) => seqno,
                },
                (
                    GcTriggerSource::Manual {
                        data:
                            ManualGcTrigger::Blocks {
                                ty: TriggerType::McSeqno(mc_seqno),
                            },
                        ..
                    },
                    _,
                ) => mc_seqno,
                (GcTriggerSource::Manual { .. }, _) => continue,
                (
                    GcTriggerSource::Automatic {
                        data: Some(trigger),
                    },
                    config_type,
                ) => {
                    tracing::debug!(?trigger);
                    // NOTE: Track the last mc block seqno since we cannot rely on the broadcasted block.
                    // It may be updated faster than the iteration of the GC manager.
                    let has_new_key_block = trigger.last_key_block_seqno > known_key_block_seqno;
                    known_key_block_seqno = trigger.last_key_block_seqno;

                    match config_type {
                        BlocksGcType::BeforeSafeDistance {
                            safe_distance,
                            min_interval,
                        } => {
                            // Compute the target masterchain block seqno
                            let target_seqno =
                                match trigger.mc_block_id.seqno.checked_sub(safe_distance) {
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
                                    tracing::warn!(
                                        target_seqno,
                                        "previous persistent block not found"
                                    );
                                    continue;
                                }
                            }
                        }
                    }
                }
                (GcTriggerSource::Automatic { data: None }, _) => continue,
            };

            // NOTE: You should update this in other branches as well,
            // if we want to debounce other types of GC.
            last_tiggered_at = Some(Instant::now());

            tracing::debug!("Blocks GC calculated target seqno {}", target_seqno);

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
    async fn states_gc(
        mut manual_gc_trigger: broadcast::Receiver<ManualGcTrigger>,
        mut trigger_rx: TriggerRx,
        storage: Storage,
    ) {
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
            let trigger_source = select! {
                _ = interval.tick() => {
                    let data = trigger_rx.borrow_and_update().clone();
                    GcTriggerSource::Automatic {data}
                },
                Ok(data) = manual_gc_trigger.recv() => {
                    tracing::debug!("Archives GC manual trigger received {:?}", &data);
                    let last_known_mc_seqno = trigger_rx.borrow_and_update().clone().map(|x| x.mc_block_id.seqno);
                    GcTriggerSource::Manual {data, last_known_mc_seqno}
                }
                Ok(_) = trigger_rx.changed() => {
                    let data = trigger_rx.borrow_and_update().clone();
                    GcTriggerSource::Automatic {data}
                },
                else => break,
            };

            tracing::debug!(?trigger_source);

            match trigger_source {
                GcTriggerSource::Automatic { .. } => {
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
                }
                _ => (),
            }

            last_triggered_at = Some(Instant::now());

            let target_seqno = match trigger_source {
                GcTriggerSource::Manual {
                    data: ManualGcTrigger::States { ty },
                    ..
                } => match Self::get_mc_block_seqno_manual(ty, storage.clone()).await {
                    Ok(Some((seqno, distance))) => seqno.saturating_sub(distance),
                    _ => continue,
                },
                GcTriggerSource::Manual { .. } => {
                    tracing::debug!("Ignoring other types of GC");
                    continue;
                }
                GcTriggerSource::Automatic { data } => {
                    let Some(data) = data else {
                        continue;
                    };
                    data.mc_block_id.seqno
                }
            };

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
    trigger_tx: TriggerTx,
    last_key_block_seqno: AtomicU32,

    handle_block_task: AbortHandle,
    handle_state_task: AbortHandle,
    archives_handle: AbortHandle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.handle_block_task.abort();
        self.handle_state_task.abort();
        self.archives_handle.abort();
    }
}

#[derive(Debug, Clone)]
pub struct GcTrigger {
    pub mc_block_id: BlockId,
    pub last_key_block_seqno: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    McSeqno(u32),
    Distance(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManualGcTrigger {
    Archives { ty: TriggerType },
    Blocks { ty: TriggerType },
    States { ty: TriggerType },
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
