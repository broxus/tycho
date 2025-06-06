use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::cell::Cell;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_storage::{BlockHandle, Storage, StoreStateHint};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use crate::block_strider::{
    BlockSaver, BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[repr(transparent)]
pub struct ShardStateApplier<S> {
    inner: Arc<Inner<S>>,
}

impl<S> ShardStateApplier<S>
where
    S: StateSubscriber,
{
    pub fn new(storage: Storage, state_subscriber: S) -> Self {
        Self {
            inner: Arc::new(Inner {
                block_saver: BlockSaver::new(storage.clone()),
                storage,
                state_subscriber,
            }),
        }
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
        let (state, handles) = if handle.has_state() {
            // Fast path when state is already applied
            let state = state_storage
                .load_state(handle.id())
                .await
                .context("failed to load applied shard state")?;

            (state, RefMcStateHandles::Skip)
        } else {
            // Load previous states
            let (prev_id, prev_id_alt) = cx
                .block
                .construct_prev_id()
                .context("failed to construct prev id")?;

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
                .compute_and_store_state_update(&cx.block, &handle, prev_root_cell)
                .await?;

            (state, handles)
        };

        Ok(StateApplierPrepared {
            handle,
            state,
            _prev_handles: handles,
        })
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
        self.inner.state_subscriber.handle_state(&cx).await?;

        // Done
        Ok(())
    }

    async fn compute_and_store_state_update(
        &self,
        block: &BlockStuff,
        handle: &BlockHandle,
        prev_root: Cell,
    ) -> Result<ShardStateStuff> {
        let labels = [("workchain", block.id().shard.workchain().to_string())];
        let _histogram =
            HistogramGuard::begin_with_labels("tycho_core_apply_block_time_high", &labels);

        let update = block
            .as_ref()
            .load_state_update()
            .context("Failed to load state update")?;

        let new_state = rayon_run(move || update.apply(&prev_root))
            .await
            .context("Failed to apply state update")?;

        let state_storage = self.inner.storage.shard_state_storage();

        let new_state =
            ShardStateStuff::from_root(block.id(), new_state, state_storage.min_ref_mc_state())
                .context("Failed to create new state")?;

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
    storage: Storage,
    state_subscriber: S,
    block_saver: BlockSaver,
}
