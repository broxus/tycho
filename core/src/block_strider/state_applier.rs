use std::sync::Arc;
use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_types::cell::{Cell, HashBytes};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;
use crate::block_strider::{
    BlockSaver, BlockSubscriber, BlockSubscriberContext, StateSubscriber,
    StateSubscriberContext,
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(prepare_block_impl)),
            file!(),
            39u32,
        );
        let cx = cx;
        let _histogram = HistogramGuard::begin(
            "tycho_core_state_applier_prepare_block_time",
        );
        let handle = {
            __guard.end_section(42u32);
            let __result = self.inner.block_saver.save_block(cx).await;
            __guard.start_section(42u32);
            __result
        }?;
        tracing::info!(
            mc_block_id = % cx.mc_block_id.as_short_id(), id = % cx.block.id(),
            "preparing block",
        );
        let state_storage = self.inner.storage.shard_state_storage();
        let state = if handle.has_state() {
            {
                __guard.end_section(57u32);
                let __result = state_storage
                    .load_state(handle.ref_by_mc_seqno(), handle.id())
                    .await;
                __guard.start_section(57u32);
                __result
            }
                .context("failed to load applied shard state")?
        } else {
            let (prev_id, prev_id_alt) = cx
                .block
                .construct_prev_id()
                .context("failed to construct prev id")?;
            let (prev_root_cell, handles, old_split_at) = {
                let prev_state = {
                    __guard.end_section(70u32);
                    let __result = state_storage.load_state(0, &prev_id).await;
                    __guard.start_section(70u32);
                    __result
                }
                    .context("failed to load prev shard state")?;
                let old_split_at = split_aug_dict_raw(
                        prev_state.state().load_accounts()?,
                        5,
                    )?
                    .into_keys()
                    .collect::<ahash::HashSet<_>>();
                match &prev_id_alt {
                    Some(prev_id) => {
                        let prev_state_alt = {
                            __guard.end_section(82u32);
                            let __result = state_storage.load_state(0, prev_id).await;
                            __guard.start_section(82u32);
                            __result
                        }
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
            {
                __guard.end_section(113u32);
                let __result = self
                    .compute_and_store_state_update(
                        &cx.block,
                        &handle,
                        prev_root_cell,
                        old_split_at,
                        handles.min_safe_handle().clone(),
                    )
                    .await;
                __guard.start_section(113u32);
                __result
            }?
        };
        Ok(StateApplierPrepared {
            handle,
            state,
        })
    }
    async fn handle_block_impl(
        &self,
        cx: &BlockSubscriberContext,
        prepared: StateApplierPrepared,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_block_impl)),
            file!(),
            123u32,
        );
        let cx = cx;
        let prepared = prepared;
        let _histogram = HistogramGuard::begin(
            "tycho_core_state_applier_handle_block_time",
        );
        tracing::info!(
            mc_block_id = % cx.mc_block_id.as_short_id(), id = % cx.block.id(),
            "handling block",
        );
        let gen_utime = prepared.handle.gen_utime() as f64;
        let seqno = prepared.handle.id().seqno as f64;
        let now = tycho_util::time::now_millis() as f64 / 1000.0;
        if cx.block.id().is_masterchain() {
            metrics::gauge!("tycho_core_last_mc_block_utime").set(gen_utime);
            metrics::gauge!("tycho_core_last_mc_block_seqno").set(seqno);
            metrics::gauge!("tycho_core_last_mc_block_applied").set(now);
        } else {
            metrics::gauge!("tycho_core_last_sc_block_utime").set(gen_utime);
            metrics::gauge!("tycho_core_last_sc_block_seqno").set(seqno);
            metrics::gauge!("tycho_core_last_sc_block_applied").set(now);
        }
        let _histogram = HistogramGuard::begin(
            "tycho_core_subscriber_handle_state_time",
        );
        let cx = StateSubscriberContext {
            mc_block_id: cx.mc_block_id,
            mc_is_key_block: cx.mc_is_key_block,
            is_key_block: cx.is_key_block,
            block: cx.block.clone(),
            archive_data: cx.archive_data.clone(),
            state: prepared.state,
            delayed: cx.delayed.clone(),
        };
        {
            __guard.end_section(160u32);
            let __result = self.inner.state_subscriber.handle_state(&cx).await;
            __guard.start_section(160u32);
            __result
        }?;
        Ok(())
    }
    async fn compute_and_store_state_update(
        &self,
        block: &BlockStuff,
        handle: &BlockHandle,
        prev_root: Cell,
        split_at: ahash::HashSet<HashBytes>,
        ref_mc_state_handle: RefMcStateHandle,
    ) -> Result<ShardStateStuff> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(compute_and_store_state_update)),
            file!(),
            173u32,
        );
        let block = block;
        let handle = handle;
        let prev_root = prev_root;
        let split_at = split_at;
        let ref_mc_state_handle = ref_mc_state_handle;
        let labels = [("workchain", block.id().shard.workchain().to_string())];
        let _histogram = HistogramGuard::begin_with_labels(
            "tycho_core_apply_block_time_high",
            &labels,
        );
        let update = block
            .as_ref()
            .load_state_update()
            .context("Failed to load state update")?;
        let apply_in_mem = HistogramGuard::begin(
            "tycho_core_apply_block_in_mem_time_high",
        );
        let new_state = {
            __guard.end_section(186u32);
            let __result = rayon_run(move || update.par_apply(&prev_root, &split_at))
                .await;
            __guard.start_section(186u32);
            __result
        }
            .context("Failed to apply state update")?;
        apply_in_mem.finish();
        let state_storage = self.inner.storage.shard_state_storage();
        let new_state = ShardStateStuff::from_root(
                block.id(),
                new_state,
                ref_mc_state_handle,
            )
            .context("Failed to create new state")?;
        {
            __guard.end_section(200u32);
            let __result = state_storage
                .store_state(
                    handle,
                    &new_state,
                    StoreStateHint {
                        block_data_size: Some(block.data_size()),
                    },
                )
                .await;
            __guard.start_section(200u32);
            __result
        }
            .context("Failed to store new state")?;
        Ok(new_state)
    }
}
impl<S> Clone for ShardStateApplier<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}
impl<S> BlockSubscriber for ShardStateApplier<S>
where
    S: StateSubscriber,
{
    type Prepared = StateApplierPrepared;
    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;
    fn prepare_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
    ) -> Self::PrepareBlockFut<'a> {
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
}
