use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::cell::Cell;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{MinRefMcStateTracker, RefMcStateHandle, ShardStateStuff};
use tycho_storage::{BlockHandle, BlockMetaData, Storage};

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

    async fn handle_block_impl(&self, cx: &BlockSubscriberContext) -> Result<()> {
        enum RefMcStateHandles {
            Split(
                #[allow(unused)] RefMcStateHandle,
                #[allow(unused)] RefMcStateHandle,
            ),
            Single(#[allow(unused)] RefMcStateHandle),
        }

        tracing::info!(id = ?cx.block.id(), "applying block");

        let state_storage = self.inner.storage.shard_state_storage();
        let handle_storage = self.inner.storage.block_handle_storage();

        // Load handle
        let handle = self
            .get_block_handle(&cx.mc_block_id, &cx.block, &cx.archive_data)
            .await?;

        // Load previous states
        let (prev_root_cell, _handles) = {
            let (prev_id, prev_id_alt) = cx
                .block
                .construct_prev_id()
                .context("failed to construct prev id")?;

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
        let started_at = std::time::Instant::now();
        let state = self
            .compute_and_store_state_update(
                &cx.block,
                &self.inner.mc_state_tracker,
                &handle,
                prev_root_cell,
            )
            .await?;
        metrics::histogram!("tycho_apply_block_time").record(started_at.elapsed());

        // Update metrics
        let gen_utime = handle.meta().gen_utime() as f64;
        let seqno = handle.id().seqno as f64;
        let now = tycho_util::time::now_millis() as f64 / 1000.0;

        if cx.block.id().is_masterchain() {
            metrics::gauge!("tycho_last_mc_block_utime").set(gen_utime);
            metrics::gauge!("tycho_last_mc_block_seqno").set(seqno);
            metrics::gauge!("tycho_last_mc_block_applied").set(now);
        } else {
            // TODO: only store max
            metrics::gauge!("tycho_last_shard_block_utime").set(gen_utime);
            metrics::gauge!("tycho_last_shard_block_seqno").set(seqno);
            metrics::gauge!("tycho_last_shard_block_applied").set(now);
        }

        // Process state
        let started_at = std::time::Instant::now();
        let cx = StateSubscriberContext {
            mc_block_id: cx.mc_block_id,
            block: cx.block.clone(), // TODO: rewrite without clone
            archive_data: cx.archive_data.clone(), // TODO: rewrite without clone
            state,
        };
        self.inner.state_subscriber.handle_state(&cx).await?;
        metrics::histogram!("tycho_subscriber_handle_block_seconds").record(started_at.elapsed());

        // Mark block as applied
        handle_storage.store_block_applied(&handle);

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
            .store_block_data(block, archive_data, BlockMetaData {
                is_key_block: info.key_block,
                gen_utime: info.gen_utime,
                mc_ref_seqno: mc_block_id.seqno,
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
        let update = block
            .block()
            .load_state_update()
            .context("Failed to load state update")?;

        let new_state = tokio::task::spawn_blocking(move || update.apply(&prev_root))
            .await
            .context("Failed to join blocking task")?
            .context("Failed to apply state update")?;
        let new_state = ShardStateStuff::from_root(block.id(), new_state, mc_state_tracker)
            .context("Failed to create new state")?;

        let state_storage = self.inner.storage.shard_state_storage();
        state_storage
            .store_state(handle, &new_state)
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
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::HandleBlockFut<'a> {
        Box::pin(self.handle_block_impl(cx))
    }
}

struct Inner<S> {
    mc_state_tracker: MinRefMcStateTracker,
    storage: Storage,
    state_subscriber: S,
}
