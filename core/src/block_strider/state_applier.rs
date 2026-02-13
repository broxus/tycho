use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::future::BoxFuture;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_util::metrics::HistogramGuard;

use crate::block_strider::{
    BlockSaver, BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use crate::storage::{BlockConnection, BlockHandle, CoreStorage, StoreStateHint};

#[repr(transparent)]
pub struct ShardStateApplier<S> {
    inner: Arc<Inner<S>>,
}

impl<S> ShardStateApplier<S>
where
    S: StateSubscriber,
{
    pub fn new(storage: CoreStorage, state_subscriber: S) -> Self {
        Self::new_ext(storage, state_subscriber, true)
    }

    pub fn without_persistent_states(storage: CoreStorage, state_subscriber: S) -> Self {
        Self::new_ext(storage, state_subscriber, false)
    }

    fn new_ext(storage: CoreStorage, state_subscriber: S, store_persistent_states: bool) -> Self {
        let last_key_block_utime = if store_persistent_states {
            Self::find_last_key_block_utime(&storage)
        } else {
            0
        };

        Self {
            inner: Arc::new(Inner {
                block_saver: BlockSaver::new(storage.clone()),
                storage,
                state_subscriber,

                store_persistent_states,
                last_key_block_utime: AtomicU32::new(last_key_block_utime),

                resume_pending_persistent_states: Default::default(),
                prev_state_task: Default::default(),
            }),
        }
    }

    fn find_last_key_block_utime(storage: &CoreStorage) -> u32 {
        storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.gen_utime())
    }

    async fn prepare_block_impl(
        &self,
        cx: &BlockSubscriberContext,
    ) -> Result<StateApplierPrepared> {
        let _histogram = HistogramGuard::begin("tycho_core_state_applier_prepare_block_time");

        self.inner
            .resume_pending_persistent_states
            .get_or_try_init(async move || {
                let mut guard = self.inner.prev_state_task.lock().await;
                anyhow::ensure!(guard.is_none(), "already creating persistent state");
                let resume = self.resume_pending_persistent_states().await?;
                *guard = resume;
                Ok::<_, anyhow::Error>(())
            })
            .await?;

        let handle = self.inner.block_saver.save_block(cx).await?;

        tracing::info!(
            mc_block_id = %cx.mc_block_id.as_short_id(),
            id = %cx.block.id(),
            "preparing block",
        );

        let states = self.inner.storage.shard_state_storage();
        let block_handles = self.inner.storage.block_handle_storage();

        // Load/Apply state
        let state = if handle.has_state() || handle.has_virtual_state() {
            // Fast path when state is already applied.
            states
                .load_state(handle.ref_by_mc_seqno(), handle.id())
                .await
                .context("failed to load applied shard state")?
        } else {
            // Load previous states
            let (prev_id, prev_id_alt) = cx
                .block
                .construct_prev_id()
                .context("failed to construct prev id")?;
            anyhow::ensure!(
                prev_id_alt.is_none(),
                "split/merge is not supported for now"
            );

            let Some(prev_handle) = block_handles.load_handle(&prev_id) else {
                anyhow::bail!("block handle not found for: {prev_id}");
            };

            let merkle_update = cx.block.as_ref().state_update.load()?;
            let hint = StoreStateHint {
                block_data_size: cx.block.data_size(),
                new_cell_count: None, // unknown
                is_top_block: Some(cx.is_top_block),
            };

            states
                .begin_store_next_state(&prev_handle, &handle, &merkle_update, None, hint, None)?
                .wait_reload()
                .await?
        };

        Ok(StateApplierPrepared { state })
    }

    async fn handle_block_impl(
        &self,
        cx: &BlockSubscriberContext,
        prepared: StateApplierPrepared,
    ) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_core_state_applier_handle_block_time");

        tracing::info!(
            mc_block_id = %cx.mc_block_id.as_short_id(),
            id = %cx.block.id(),
            "handling block",
        );

        // Process state
        let _histogram = HistogramGuard::begin("tycho_core_subscriber_handle_state_time");

        let cx = StateSubscriberContext {
            mc_block_id: cx.mc_block_id,
            mc_is_key_block: cx.mc_is_key_block,
            is_key_block: cx.is_key_block,
            block: cx.block.clone(),
            archive_data: cx.archive_data.clone(),
            state: prepared.state,
            delayed: cx.delayed.clone(),
        };

        let subscriber_fut = self.inner.state_subscriber.handle_state(&cx);

        if self.inner.store_persistent_states {
            let applier_fut = self.try_save_persistent_states(&cx);
            match futures_util::future::join(applier_fut, subscriber_fut).await {
                (Err(e), _) | (_, Err(e)) => Err(e),
                _ => Ok(()),
            }
        } else {
            subscriber_fut.await
        }
    }

    async fn try_save_persistent_states(&self, cx: &StateSubscriberContext) -> Result<()> {
        let this = self.inner.as_ref();

        // Check if the previous persistent state save task has finished.
        // This allows us to detect errors early without waiting for the next key block
        let mut prev_task = this.prev_state_task.lock().await;
        if let Some(task) = &mut *prev_task
            && task.is_finished()
        {
            task.join().await?;
        }
        drop(prev_task);

        if cx.is_key_block {
            let block_info = cx.block.load_info()?;

            let prev_utime = this
                .last_key_block_utime
                .swap(block_info.gen_utime, Ordering::Relaxed);

            if BlockStuff::compute_is_persistent(block_info.gen_utime, prev_utime) {
                let mut prev_task = this.prev_state_task.lock().await;
                if let Some(task) = &mut *prev_task {
                    task.join().await?;
                }

                let mc_block_stuff = cx.block.clone();
                let inner = self.inner.clone();
                let deps = inner.begin_persistent_state_store(&mc_block_stuff).await?;

                *prev_task = Some(StorePersistentStateTask {
                    mc_seqno: cx.mc_block_id.seqno,
                    handle: Some(tokio::spawn(async move {
                        inner.save_persistent_states(mc_block_stuff, deps).await
                    })),
                });
            }
        }

        Ok(())
    }

    async fn resume_pending_persistent_states(&self) -> Result<Option<StorePersistentStateTask>> {
        let node_state = self.inner.storage.node_state();
        let blocks = self.inner.storage.block_storage();
        let block_handles = self.inner.storage.block_handle_storage();

        let Some(block_id) = node_state.load_pending_persistent_state_id() else {
            return Ok(None);
        };

        let Some(handle) = block_handles.load_handle(&block_id) else {
            bail!("block handle not found {}", block_id);
        };

        let mc_block_stuff = blocks.load_block_data(&handle).await?;
        let inner = self.inner.clone();
        let deps = inner.begin_persistent_state_store(&mc_block_stuff).await?;

        Ok(Some(StorePersistentStateTask {
            mc_seqno: block_id.seqno,
            handle: Some(tokio::spawn(async move {
                inner.save_persistent_states(mc_block_stuff, deps).await
            })),
        }))
    }
}

impl<S> Clone for ShardStateApplier<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> BlockSubscriber for ShardStateApplier<S>
where
    S: StateSubscriber,
{
    type Prepared = StateApplierPrepared;

    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        Box::pin(self.prepare_block_impl(cx))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(self.handle_block_impl(cx, prepared))
    }
}

pub struct StateApplierPrepared {
    state: ShardStateStuff,
}

struct StorePersistentStateDeps {
    pub state_gc_handles: Vec<BlockHandle>,
    pub blocks_gc_handles: Vec<BlockHandle>,
}

struct Inner<S> {
    storage: CoreStorage,
    state_subscriber: S,
    block_saver: BlockSaver,

    store_persistent_states: bool,
    last_key_block_utime: AtomicU32,

    resume_pending_persistent_states: OnceCell<()>,
    prev_state_task: tokio::sync::Mutex<Option<StorePersistentStateTask>>,
}

impl<S> Inner<S> {
    async fn prepare_persistent_queue_diffs_for_shard(
        &self,
        top_block_handle: &BlockHandle,
    ) -> Result<Vec<BlockHandle>> {
        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let block_connections = self.storage.block_connection_storage();

        // NOTE: The `tail_len` in block is not if fact the length of "tail",
        // it also includes the block diff itself, so we substract 1 here.
        let Some(tail_len) = blocks
            .load_block_data(top_block_handle)
            .await?
            .as_ref()
            .out_msg_queue_updates
            .tail_len
            .checked_sub(1)
        else {
            anyhow::bail!("invalid blocktail len");
        };

        // top block itself
        block_handles.set_skip_blocks_gc(top_block_handle);
        let mut skip_blocks_handles = vec![top_block_handle.clone()];

        // all required blocks
        let mut block_id = *top_block_handle.id();
        for _ in 0..tail_len {
            // TODO: support BlockConnection::Prev2 in the future once split/merge is enabled
            let Some(prev_block_id) =
                block_connections.load_connection(&block_id, BlockConnection::Prev1)
            else {
                bail!("prev block connection not found for: {block_id}");
            };

            let Some(prev_block_handle) = block_handles.load_handle(&prev_block_id) else {
                bail!("prev block handle not found for: {prev_block_id}");
            };

            block_handles.set_skip_blocks_gc(&prev_block_handle);
            skip_blocks_handles.push(prev_block_handle);

            block_id = prev_block_id;
        }

        Ok(skip_blocks_handles)
    }

    async fn begin_persistent_state_store(
        &self,
        mc_block: &BlockStuff,
    ) -> Result<StorePersistentStateDeps> {
        let block_handles = self.storage.block_handle_storage();
        let node_state = self.storage.node_state();

        let mc_block_id = mc_block.id();
        let extra = mc_block.load_custom()?;

        let Some(mc_block_handle) = block_handles.load_handle(mc_block_id) else {
            return Err(anyhow!("mc block handle does not exist {}", mc_block_id));
        };

        node_state.set_pending_persistent_state_id(mc_block_id);

        let mut state_gc_handles = Vec::new();
        let mut blocks_gc_handles = Vec::new();

        for entry in extra.shards.latest_blocks() {
            let block_id = entry?;
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                bail!("top shard block handle not found: {block_id}");
            };

            let queue_deps = self
                .prepare_persistent_queue_diffs_for_shard(&block_handle)
                .await?;
            blocks_gc_handles.extend_from_slice(&queue_deps);

            block_handles.set_skip_states_gc(&block_handle);
            state_gc_handles.push(block_handle);
        }

        let queue_deps = self
            .prepare_persistent_queue_diffs_for_shard(&mc_block_handle)
            .await?;
        blocks_gc_handles.extend_from_slice(&queue_deps);

        block_handles.set_skip_states_gc(&mc_block_handle);
        state_gc_handles.push(mc_block_handle);

        Ok(StorePersistentStateDeps {
            state_gc_handles,
            blocks_gc_handles,
        })
    }

    async fn save_persistent_states(
        &self,
        mc_block: BlockStuff,
        deps: StorePersistentStateDeps,
    ) -> Result<()> {
        let node_state = self.storage.node_state();
        let block_handles = self.storage.block_handle_storage();

        let Some(mc_block_handle) = block_handles.load_handle(mc_block.id()) else {
            bail!("masterchain block handle not found: {}", mc_block.id());
        };
        block_handles.set_block_persistent(&mc_block_handle);

        let (state_result, queue_result) = tokio::join!(
            self.save_persistent_shard_states(&mc_block_handle, &mc_block),
            self.save_persistent_queue_states(&mc_block_handle, &mc_block),
        );
        state_result?;
        queue_result?;

        self.storage
            .persistent_state_storage()
            .rotate_persistent_states(&mc_block_handle)
            .await?;

        metrics::counter!("tycho_core_ps_subscriber_saved_persistent_states_count").increment(1);
        tracing::debug!("saved persistent state for {}", mc_block_handle.id());

        for handle in deps.state_gc_handles {
            block_handles.set_skip_states_gc_finished(&handle);
        }
        for handle in deps.blocks_gc_handles {
            block_handles.set_skip_blocks_gc_finished(&handle);
        }

        node_state.reset_pending_persistent_state_id();
        Ok(())
    }

    async fn save_persistent_shard_states(
        &self,
        mc_block_handle: &BlockHandle,
        mc_block: &BlockStuff,
    ) -> Result<()> {
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();
        let state_storage = self.storage.shard_state_storage();

        let mc_seqno = mc_block_handle.id().seqno;

        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            let block_id = entry?;
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };

            // Ensure skipped state is stored in DB before saving persistent state.
            if !block_handle.has_state() {
                let state = state_storage
                    .load_state(mc_seqno, &block_id)
                    .await
                    .context("failed to load skipped shard state for persistent save")?;

                state_storage
                    .store_state_ignore_cache(&block_handle, &state, StoreStateHint {
                        is_top_block: Some(true),
                        ..Default::default()
                    })
                    .await
                    .context("failed to store skipped shard state for persistent save")?;
            }

            // NOTE: We could have also called the `set_block_persistent` here, but we
            //       only do this in the first part of the `save_persistent_queue_states`.

            persistent_states
                .store_shard_state(mc_seqno, &block_handle)
                .await?;
        }

        // NOTE: We intentionally store the masterchain state last to ensure that
        //       the handle will live long enough. And this way we don't mislead
        //       other nodes with the incomplete set of persistent states.
        // NOTE: Masterchain states are always stored directly so there is no
        //       need to explicitly store them once more.
        persistent_states
            .store_shard_state(mc_seqno, mc_block_handle)
            .await
    }

    async fn save_persistent_queue_states(
        &self,
        mc_block_handle: &BlockHandle,
        mc_block: &BlockStuff,
    ) -> Result<()> {
        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();

        let mut shard_block_handles = Vec::new();

        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            let block_id = entry?;
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };

            // NOTE: We set the flag only here because this part will be executed
            //       first, without waiting for other states or queues to be saved.
            block_handles.set_block_persistent(&block_handle);

            shard_block_handles.push(block_handle);
        }

        // Store queue state for each shard
        let mc_seqno = mc_block_handle.id().seqno;
        for block_handle in shard_block_handles {
            let block = blocks.load_block_data(&block_handle).await?;
            persistent_states
                .store_queue_state(mc_seqno, &block_handle, block)
                .await?;
        }

        persistent_states
            .store_queue_state(mc_seqno, mc_block_handle, mc_block.clone())
            .await?;

        Ok(())
    }
}

struct StorePersistentStateTask {
    mc_seqno: u32,
    handle: Option<JoinHandle<Result<()>>>,
}

impl StorePersistentStateTask {
    async fn join(&mut self) -> Result<()> {
        // NOTE: Await on reference to make sure that the task is cancel safe
        if let Some(handle) = &mut self.handle {
            let result = handle
                .await
                .map_err(|e| {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                    anyhow::Error::from(e)
                })
                .and_then(std::convert::identity);

            self.handle = None;

            if let Err(e) = &result {
                tracing::error!(
                    mc_seqno = self.mc_seqno,
                    "failed to save persistent state: {e:?}"
                );
            }

            return result;
        }

        Ok(())
    }

    fn is_finished(&self) -> bool {
        if let Some(handle) = &self.handle {
            return handle.is_finished();
        }

        false
    }
}
