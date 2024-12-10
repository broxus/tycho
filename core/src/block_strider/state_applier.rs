use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::cell::Cell;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{MinRefMcStateTracker, RefMcStateHandle, ShardStateStuff};
use tycho_storage::{BlockConnection, BlockHandle, NewBlockMeta, Storage, StoreStateHint};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[repr(transparent)]
pub struct ShardStateApplier<S> {
    inner: Arc<Inner<S>>,
}

impl<S> ShardStateApplier<S>
where
    S: StateSubscriber,
{
    pub fn new(
        mc_state_tracker: MinRefMcStateTracker,
        storage: Storage,
        state_subscriber: S,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                mc_state_tracker,
                storage,
                state_subscriber,
            }),
        }
    }

    async fn prepare_block_impl(
        &self,
        cx: &BlockSubscriberContext,
    ) -> Result<ShardApplierPrepared> {
        let _histogram = HistogramGuard::begin("tycho_core_state_applier_prepare_block_time");

        tracing::info!(id = %cx.block.id(), "preparing block");

        let state_storage = self.inner.storage.shard_state_storage();

        // Load handle
        let handle = self
            .get_block_handle(&cx.mc_block_id, &cx.block, &cx.archive_data)
            .await?;

        let (prev_id, prev_id_alt) = cx
            .block
            .construct_prev_id()
            .context("failed to construct prev id")?;

        // Update block connections
        {
            let block_handles = self.inner.storage.block_handle_storage();
            let connections = self.inner.storage.block_connection_storage();

            let block_id = cx.block.id();

            let prev_handle = block_handles.load_handle(&prev_id);

            match prev_id_alt {
                None => {
                    if let Some(handle) = prev_handle {
                        let direction = if block_id.shard != prev_id.shard
                            && prev_id.shard.split().unwrap().1 == block_id.shard
                        {
                            // Special case for the right child after split
                            BlockConnection::Next2
                        } else {
                            BlockConnection::Next1
                        };
                        connections.store_connection(&handle, direction, block_id);
                    }
                    connections.store_connection(&handle, BlockConnection::Prev1, &prev_id);
                }
                Some(ref prev_id_alt) => {
                    if let Some(handle) = prev_handle {
                        connections.store_connection(&handle, BlockConnection::Next1, block_id);
                    }
                    if let Some(handle) = block_handles.load_handle(prev_id_alt) {
                        connections.store_connection(&handle, BlockConnection::Next1, block_id);
                    }
                    connections.store_connection(&handle, BlockConnection::Prev1, &prev_id);
                    connections.store_connection(&handle, BlockConnection::Prev2, prev_id_alt);
                }
            }
        }

        // Load/Apply state
        let (state, handles) = if handle.has_state() {
            // Fast path when state is already applied
            let state = state_storage
                .load_state(handle.id())
                .await
                .context("failed to load applied shard state")?;

            (state, RefMcStateHandles::Skip)
        } else {
            // Load previous states
            let (prev_root_cell, handles) = {
                let prev_state = state_storage
                    .load_state(&prev_id)
                    .await
                    .context("failed to load prev shard state")?;

                match &prev_id_alt {
                    Some(prev_id) => {
                        let prev_state_alt = state_storage
                            .load_state(prev_id)
                            .await
                            .context("failed to load alt prev shard state")?;

                        let cell = ShardStateStuff::construct_split_root(
                            prev_state.root_cell().clone(),
                            prev_state_alt.root_cell().clone(),
                        )?;
                        let left_handle = prev_state.ref_mc_state_handle().clone();
                        let right_handle = prev_state_alt.ref_mc_state_handle().clone();
                        (cell, RefMcStateHandles::Split(left_handle, right_handle))
                    }
                    None => {
                        let cell = prev_state.root_cell().clone();
                        let handle = prev_state.ref_mc_state_handle().clone();
                        (cell, RefMcStateHandles::Single(handle))
                    }
                }
            };

            // Apply state
            let state = self
                .compute_and_store_state_update(
                    &cx.block,
                    &self.inner.mc_state_tracker,
                    &handle,
                    prev_root_cell,
                )
                .await?;

            (state, handles)
        };

        Ok(ShardApplierPrepared {
            handle,
            state,
            _prev_handles: handles,
        })
    }

    async fn handle_block_impl(
        &self,
        cx: &BlockSubscriberContext,
        prepared: ShardApplierPrepared,
    ) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_core_state_applier_handle_block_time");

        tracing::info!(id = %cx.block.id(), "handling block");

        // Update metrics
        let gen_utime = prepared.handle.gen_utime() as f64;
        let seqno = prepared.handle.id().seqno as f64;
        let now = tycho_util::time::now_millis() as f64 / 1000.0;

        if cx.block.id().is_masterchain() {
            metrics::gauge!("tycho_core_last_mc_block_utime").set(gen_utime);
            metrics::gauge!("tycho_core_last_mc_block_seqno").set(seqno);
            metrics::gauge!("tycho_core_last_mc_block_applied").set(now);
        } else {
            // TODO: only store max
            metrics::gauge!("tycho_core_last_sc_block_utime").set(gen_utime);
            metrics::gauge!("tycho_core_last_sc_block_seqno").set(seqno);
            metrics::gauge!("tycho_core_last_sc_block_applied").set(now);
        }

        // Process state
        let started_at = std::time::Instant::now();
        let cx = StateSubscriberContext {
            mc_block_id: cx.mc_block_id,
            mc_is_key_block: cx.mc_is_key_block,
            is_key_block: cx.is_key_block,
            block: cx.block.clone(), // TODO: rewrite without clone
            archive_data: cx.archive_data.clone(), // TODO: rewrite without clone
            state: prepared.state,
        };
        self.inner.state_subscriber.handle_state(&cx).await?;
        metrics::histogram!("tycho_core_subscriber_handle_state_time").record(started_at.elapsed());

        // Mark block as applied
        let applied = self
            .inner
            .storage
            .block_handle_storage()
            .set_block_applied(&prepared.handle);

        if applied && self.inner.storage.config().archives_gc.is_some() {
            tracing::debug!(block_id = %prepared.handle.id(), "saving block into archive");
            self.inner
                .storage
                .block_storage()
                .move_into_archive(&prepared.handle, cx.mc_is_key_block)
                .await?;
        }

        // Done
        Ok(())
    }

    async fn get_block_handle(
        &self,
        mc_block_id: &BlockId,
        block: &BlockStuff,
        archive_data: &ArchiveData,
    ) -> Result<BlockHandle> {
        let block_storage = self.inner.storage.block_storage();

        let info = block.load_info()?;
        let res = block_storage
            .store_block_data(block, archive_data, NewBlockMeta {
                is_key_block: info.key_block,
                gen_utime: info.gen_utime,
                ref_by_mc_seqno: mc_block_id.seqno,
            })
            .await?;

        Ok(res.handle)
    }

    async fn compute_and_store_state_update(
        &self,
        block: &BlockStuff,
        mc_state_tracker: &MinRefMcStateTracker,
        handle: &BlockHandle,
        prev_root: Cell,
    ) -> Result<ShardStateStuff> {
        let _histogram = HistogramGuard::begin("tycho_core_apply_block_time");

        let update = block
            .as_ref()
            .load_state_update()
            .context("Failed to load state update")?;

        let new_state = rayon_run(move || update.apply(&prev_root))
            .await
            .context("Failed to apply state update")?;
        let new_state = ShardStateStuff::from_root(block.id(), new_state, mc_state_tracker)
            .context("Failed to create new state")?;

        let state_storage = self.inner.storage.shard_state_storage();
        state_storage
            .store_state(handle, &new_state, StoreStateHint {
                block_data_size: Some(block.data_size()),
            })
            .await
            .context("Failed to store new state")?;

        Ok(new_state)
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
    type Prepared = ShardApplierPrepared;

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

pub struct ShardApplierPrepared {
    handle: BlockHandle,
    state: ShardStateStuff,
    _prev_handles: RefMcStateHandles,
}

enum RefMcStateHandles {
    Skip,
    Split(
        #[allow(unused)] RefMcStateHandle,
        #[allow(unused)] RefMcStateHandle,
    ),
    Single(#[allow(unused)] RefMcStateHandle),
}

struct Inner<S> {
    mc_state_tracker: MinRefMcStateTracker,
    storage: Storage,
    state_subscriber: S,
}
