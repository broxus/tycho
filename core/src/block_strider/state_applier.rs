use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tokio::task::JoinHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_types::cell::{Cell, HashBytes};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use crate::block_strider::{
    BlockSaver, BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use crate::storage::{BlockHandle, CoreStorage, StoreStateHint};

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

        let handle = self.inner.block_saver.save_block(cx).await?;

        tracing::info!(
            mc_block_id = %cx.mc_block_id.as_short_id(),
            id = %cx.block.id(),
            "preparing block",
        );

        let state_storage = self.inner.storage.shard_state_storage();

        // Load/Apply state
        let state = if handle.has_state() {
            // Fast path when state is already applied
            state_storage
                .load_state(handle.ref_by_mc_seqno(), handle.id())
                .await
                .context("failed to load applied shard state")?
        } else {
            // Load previous states
            let (prev_id, prev_id_alt) = cx
                .block
                .construct_prev_id()
                .context("failed to construct prev id")?;

            let (prev_root_cell, handles, old_split_at) = {
                // NOTE: Use zero epoch here since we don't need to reuse these states.
                let prev_state = state_storage
                    .load_state(0, &prev_id)
                    .await
                    .context("failed to load prev shard state")?;

                let old_split_at = split_aug_dict_raw(prev_state.state().load_accounts()?, 5)?
                    .into_keys()
                    .collect::<ahash::HashSet<_>>();

                match &prev_id_alt {
                    Some(prev_id) => {
                        // NOTE: Use zero epoch here since we don't need to reuse these states.
                        let prev_state_alt = state_storage
                            .load_state(0, prev_id)
                            .await
                            .context("failed to load alt prev shard state")?;

                        let cell = ShardStateStuff::construct_split_root(
                            prev_state.root_cell().clone(),
                            prev_state_alt.root_cell().clone(),
                        )?;
                        let left_handle = prev_state.ref_mc_state_handle().clone();
                        let right_handle = prev_state_alt.ref_mc_state_handle().clone();
                        (
                            cell,
                            RefMcStateHandles::Split(left_handle, right_handle),
                            old_split_at,
                        )
                    }
                    None => {
                        let cell = prev_state.root_cell().clone();
                        let handle = prev_state.ref_mc_state_handle().clone();
                        (cell, RefMcStateHandles::Single(handle), old_split_at)
                    }
                }
            };

            // Apply state
            self.compute_and_store_state_update(
                &cx.block,
                &handle,
                prev_root_cell,
                old_split_at,
                handles.min_safe_handle().clone(),
            )
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

    async fn compute_and_store_state_update(
        &self,
        block: &BlockStuff,
        handle: &BlockHandle,
        prev_root: Cell,
        split_at: ahash::HashSet<HashBytes>,
        ref_mc_state_handle: RefMcStateHandle,
    ) -> Result<ShardStateStuff> {
        let labels = [("workchain", block.id().shard.workchain().to_string())];
        let _histogram =
            HistogramGuard::begin_with_labels("tycho_core_apply_block_time_high", &labels);

        let update = block
            .as_ref()
            .load_state_update()
            .context("Failed to load state update")?;

        let apply_in_mem = HistogramGuard::begin("tycho_core_apply_block_in_mem_time_high");

        let new_state = rayon_run(move || update.par_apply(&prev_root, &split_at))
            .await
            .context("Failed to apply state update")?;

        apply_in_mem.finish();

        let state_storage = self.inner.storage.shard_state_storage();

        let new_state = ShardStateStuff::from_root(block.id(), new_state, ref_mc_state_handle)
            .context("Failed to create new state")?;

        state_storage
            .store_state(handle, &new_state, StoreStateHint {
                block_data_size: Some(block.data_size()),
            })
            .await
            .context("Failed to store new state")?;

        Ok(new_state)
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
            let is_persistent = BlockStuff::compute_is_persistent(block_info.gen_utime, prev_utime);

            if is_persistent && cx.block.id().seqno != 0 {
                let mut prev_task = this.prev_state_task.lock().await;
                if let Some(task) = &mut *prev_task {
                    task.join().await?;
                }

                let block = cx.block.clone();
                let inner = self.inner.clone();
                let state_handle = cx.state.ref_mc_state_handle().clone();

                *prev_task = Some(StorePersistentStateTask {
                    mc_seqno: cx.mc_block_id.seqno,
                    handle: Some(tokio::spawn(async move {
                        inner.save_persistent_states(block, state_handle).await
                    })),
                });
            }
        }

        Ok(())
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

enum RefMcStateHandles {
    Split(RefMcStateHandle, RefMcStateHandle),
    Single(RefMcStateHandle),
}

impl RefMcStateHandles {
    fn min_safe_handle(&self) -> &RefMcStateHandle {
        match self {
            Self::Split(left, right) => left.min_safe(right),
            Self::Single(handle) => handle,
        }
    }
}

struct Inner<S> {
    storage: CoreStorage,
    state_subscriber: S,
    block_saver: BlockSaver,

    store_persistent_states: bool,
    last_key_block_utime: AtomicU32,
    prev_state_task: tokio::sync::Mutex<Option<StorePersistentStateTask>>,
}

impl<S> Inner<S> {
    async fn save_persistent_states(
        &self,
        mc_block: BlockStuff,
        mc_state_handle: RefMcStateHandle,
    ) -> Result<()> {
        let block_handles = self.storage.block_handle_storage();

        let Some(mc_block_handle) = block_handles.load_handle(mc_block.id()) else {
            anyhow::bail!("masterchain block handle not found: {}", mc_block.id());
        };
        block_handles.set_block_persistent(&mc_block_handle);

        let (state_result, queue_result) = tokio::join!(
            self.save_persistent_shard_states(
                mc_block_handle.clone(),
                mc_block.clone(),
                mc_state_handle
            ),
            self.save_persistent_queue_states(mc_block_handle.clone(), mc_block),
        );
        state_result?;
        queue_result?;

        self.storage
            .persistent_state_storage()
            .rotate_persistent_states(&mc_block_handle)
            .await?;

        metrics::counter!("tycho_core_ps_subscriber_saved_persistent_states_count").increment(1);

        Ok(())
    }

    async fn save_persistent_shard_states(
        &self,
        mc_block_handle: BlockHandle,
        mc_block: BlockStuff,
        mc_state_handle: RefMcStateHandle,
    ) -> Result<()> {
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();

        let mc_seqno = mc_block_handle.id().seqno;
        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            let block_id = entry?;
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };

            // NOTE: We could have also called the `set_block_persistent` here, but we
            //       only do this in the first part of the `save_persistent_queue_states`.

            persistent_states
                .store_shard_state(mc_seqno, &block_handle, mc_state_handle.clone())
                .await?;
        }

        // NOTE: We intentionally store the masterchain state last to ensure that
        //       the handle will live long enough. And this way we don't mislead
        //       other nodes with the incomplete set of persistent states.
        persistent_states
            .store_shard_state(mc_seqno, &mc_block_handle, mc_state_handle)
            .await?;

        Ok(())
    }

    async fn save_persistent_queue_states(
        &self,
        mc_block_handle: BlockHandle,
        mc_block: BlockStuff,
    ) -> Result<()> {
        if mc_block_handle.id().seqno == 0 {
            // No queue states for zerostate.
            return Ok(());
        }

        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();

        let mut shard_block_handles = Vec::new();

        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            let block_id = entry?;
            if block_id.seqno == 0 {
                // No queue states for zerostate.
                continue;
            }

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
            .store_queue_state(mc_seqno, &mc_block_handle, mc_block)
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
