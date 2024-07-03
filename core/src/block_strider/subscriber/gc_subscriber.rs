use std::arch::x86_64::_mm256_insert_epi16;
use std::ops::Add;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
use tycho_block_util::block::BlockStuff;
use tycho_storage::{ArchivesGcInterval, Storage};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::time::duration_between_unix_and_instant;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[repr(transparent)]
pub struct GcSubscriber {
    inner: Arc<Inner>,
}

impl GcSubscriber {
    pub fn new(storage: Storage) -> Self {
        let (block_sender, mut block_receiver) =
            tokio::sync::watch::channel::<Option<BlockStuff>>(None);
        let (state_sender, mut state_receiver) =
            tokio::sync::watch::channel::<Option<BlockStuff>>(None);

        let mut inner = Inner {
            storage: storage.clone(),
            block_sender,
            state_sender,
            handle_block_task: None,
            handle_state_task: None,
            archives_handle: None,
        };

        let hb_handle = Self::handle_block_gc(block_receiver, storage.clone()).abort_handle();
        let hs_handle = Self::handle_state_gc(state_receiver, storage.clone()).abort_handle();
        let archives_handle =
            tokio::spawn(Self::handle_archives_gc(storage.clone())).abort_handle();

        inner.handle_block_task = Some(hb_handle);
        inner.handle_state_task = Some(hs_handle);
        inner.archives_handle = Some(archives_handle);

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn handle(&self, block_stuff: BlockStuff) {
        if !block_stuff.id().is_masterchain() {
            return ();
        }
        let block = match block_stuff.load_info() {
            Ok(block) => block,
            Err(e) => {
                tracing::error!("Failed to load block info: {:?} {e:?}", block_stuff.id());
                return ();
            }
        };

        if let Err(e) = self.inner.state_sender.send(Some(block_stuff.clone())) {
            tracing::error!("Failed to execute handle_state for state_sender. {e:?} ");
        }

        if block.key_block {
            if let Err(e) = self.inner.block_sender.send(Some(block_stuff.clone())) {
                tracing::error!("Failed to execute handle_state for block_sender. {e:?} ");
            }
        }
    }

    async fn handle_archives_gc(storage: Storage) {
        let options = match &storage.config().archives {
            Some(options) => options,
            None => return,
        };

        struct LowerBound {
            archive_id: AtomicU32,
            changed: Notify,
        }

        #[allow(unused_mut)]
        let mut lower_bound = None::<Arc<LowerBound>>;

        match options.gc_interval {
            ArchivesGcInterval::Manual => return,
            ArchivesGcInterval::PersistentStates { offset } => {
                tokio::spawn(async move {
                    let persistent_state_keeper =
                        storage.runtime_storage().persistent_state_keeper();

                    loop {
                        tokio::pin!(let new_state_found = persistent_state_keeper.new_state_found(););

                        let (until_id, untile_time) = match persistent_state_keeper.current() {
                            Some(state) => {
                                let untile_time =
                                    (state.meta().gen_utime() as u64).add(offset.as_secs());
                                (state.id().seqno, untile_time)
                            }
                            None => {
                                new_state_found.await;
                                continue;
                            }
                        };

                        tokio::select!(
                            _ = tokio::time::sleep(duration_between_unix_and_instant(untile_time, Instant::now())) => {},
                            _ = &mut new_state_found => continue,
                        );

                        if let Some(lower_bound) = &lower_bound {
                            loop {
                                tokio::pin!(let lower_bound_changed = lower_bound.changed.notified(););

                                let lower_bound = lower_bound.archive_id.load(Ordering::Acquire);
                                if until_id < lower_bound {
                                    break;
                                }

                                tracing::info!(
                                    until_id,
                                    lower_bound,
                                    "waiting for the archives barrier"
                                );
                                lower_bound_changed.await;
                            }
                        }

                        if let Err(e) = storage
                            .block_storage()
                            .remove_outdated_archives(until_id)
                            .await
                        {
                            tracing::error!("failed to remove outdated archives: {e:?}");
                        }

                        new_state_found.await;
                    }
                });
            }
        }
    }

    fn handle_block_gc(
        mut block_receiver: tokio::sync::watch::Receiver<Option<BlockStuff>>,
        storage: Storage,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = block_receiver.changed().await {
                    tracing::error!("Failed to receive block from block_receiver. {e:?}");
                    continue;
                }

                tracing::info!("Block GC executed...");

                let block = block_receiver.borrow_and_update().clone();

                if !storage.gc_enable_for_sync() {
                    tracing::debug!("Block GC is not enabled for sync.");
                    continue;
                }

                let (Some(bs), Some(config)) = (block, storage.config().blocks_gc_config) else {
                    tracing::debug!("Block GC is disabled by config or boundary block not found");
                    continue;
                };

                tracing::debug!(
                    "Removing outdated blocks with boundary block {}, batch of {:?} and kind {:?}",
                    bs.id(),
                    config.max_blocks_per_batch,
                    config.kind
                );

                if let Err(e) = storage
                    .block_storage()
                    .remove_outdated_blocks(&bs.id(), config.max_blocks_per_batch, config.kind)
                    .await
                {
                    tracing::error!("Failed to remove_outdated_blocks. {e:?}")
                }
            }
        })
    }

    fn handle_state_gc(
        mut state_receiver: tokio::sync::watch::Receiver<Option<BlockStuff>>,
        storage: Storage,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = state_receiver.changed().await {
                    tracing::error!("Failed to receive block from block_receiver. {e:?}");
                    continue;
                }

                tracing::info!("State GC executed...");

                let Some(block) = state_receiver.borrow_and_update().clone() else {
                    tracing::info!("Boundary GC block is not found");
                    continue;
                };
                tracing::info!("GC state is executing for block: {:?}", block.id());

                let shard_state_storage = storage.shard_state_storage();

                match shard_state_storage
                    .remove_outdated_states(block.id().seqno)
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        tracing::error!(target: "storage", "Failed to GC state: {e:?}");
                    }
                };
            }
        })
    }
}

struct Inner {
    storage: Storage,
    block_sender: tokio::sync::watch::Sender<Option<BlockStuff>>,
    state_sender: tokio::sync::watch::Sender<Option<BlockStuff>>,

    handle_block_task: Option<AbortHandle>,
    handle_state_task: Option<AbortHandle>,
    archives_handle: Option<AbortHandle>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self.handle_block_task.take() {
            handle.abort();
        }

        if let Some(handle) = self.handle_state_task.take() {
            handle.abort();
        }

        if let Some(handle) = self.archives_handle.take() {
            handle.abort();
        }
    }
}

impl StateSubscriber for GcSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.handle(cx.block.clone());
        futures_util::future::ready(Ok(()))
    }
}

impl BlockSubscriber for GcSubscriber {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;

    fn prepare_block<'a>(&'a self, _cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        self.handle(cx.block.clone());
        futures_util::future::ready(Ok(()))
    }
}
