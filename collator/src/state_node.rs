use std::collections::BTreeMap;
use std::sync::Arc;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{broadcast, watch};
use tycho_block_util::block::{BlockProofStuff, BlockStuff, BlockStuffAug};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::{
    BlockHandle, CoreStorage, MaybeExistingHandle, NewBlockMeta, StoreStateHint,
};
use tycho_network::PeerId;
use tycho_types::boc::BocRepr;
use tycho_types::merkle::MerkleProof;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::mem::Reclaimer;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;
use tycho_util::{FastDashMap, FastHashMap};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;
use crate::types::{ArcSignature, BlockStuffForSync};
pub trait StateNodeAdapterFactory {
    type Adapter: StateNodeAdapter;
    fn create(&self, listener: Arc<dyn StateNodeEventListener>) -> Self::Adapter;
}
impl<F, R> StateNodeAdapterFactory for F
where
    F: Fn(Arc<dyn StateNodeEventListener>) -> R,
    R: StateNodeAdapter,
{
    type Adapter = R;
    fn create(&self, listener: Arc<dyn StateNodeEventListener>) -> Self::Adapter {
        self(listener)
    }
}
#[async_trait]
pub trait StateNodeEventListener: Send + Sync {
    /// When our collated block was accepted and applied
    async fn on_block_accepted(&self, state: &ShardStateStuff) -> Result<()>;
    /// When new block was received and applied from blockchain
    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()>;
}
#[async_trait]
pub trait StateNodeAdapter: Send + Sync + 'static {
    /// Return id of last master block that was applied to node local state
    fn load_last_applied_mc_block_id(&self) -> Result<BlockId>;
    /// Return master or shard state on specified block from node local state
    async fn load_state(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff>;
    /// Store shard state root in the storage.
    /// Returns `true` when state was updated in storage.
    async fn store_state_root(
        &self,
        block_id: &BlockId,
        meta: NewBlockMeta,
        state_root: Cell,
        hint: StoreStateHint,
    ) -> Result<bool>;
    /// Return block by its id from node local state
    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>>;
    /// Return block by its handle from node local state
    async fn load_block_by_handle(
        &self,
        handle: &BlockHandle,
    ) -> Result<Option<BlockStuff>>;
    /// Return block handle by its id from node local state
    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>>;
    /// Return `ref_by_mc_seqno` from block handle by its id from node local state
    async fn get_ref_by_mc_seqno(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<BlockSeqno>>;
    /// Accept block:
    /// 1. (TODO) Broadcast block to blockchain network
    /// 2. Provide block to the block strider
    fn accept_block(&self, block: Arc<BlockStuffForSync>) -> Result<()>;
    /// Waits for the specified block to be received and returns it
    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
    /// Waits for the specified block by `prev_id` to be received and returns it
    async fn wait_for_block_next(
        &self,
        prev_id: &BlockId,
    ) -> Option<Result<BlockStuffAug>>;
    /// Handle state after block was applied
    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()>;
    /// Load queue diff
    async fn load_diff(&self, block_id: &BlockId) -> Result<Option<QueueDiffStuff>>;
    /// Handle sync context update
    fn set_sync_context(&self, sync_context: CollatorSyncContext);
    fn load_init_block_id(&self) -> Option<BlockId>;
}
pub struct StateNodeAdapterStdImpl {
    listener: Arc<dyn StateNodeEventListener>,
    blocks: FastDashMap<ShardIdent, BTreeMap<u32, Arc<BlockStuffForSync>>>,
    storage: CoreStorage,
    broadcaster: broadcast::Sender<BlockId>,
    sync_context_tx: watch::Sender<CollatorSyncContext>,
    delayed_state_notifier: DelayedStateNotifier,
}
impl StateNodeAdapterStdImpl {
    pub fn new(
        listener: Arc<dyn StateNodeEventListener>,
        storage: CoreStorage,
        initial_sync_context: CollatorSyncContext,
    ) -> Self {
        let (sync_context_tx, mut sync_context_rx) = watch::channel(
            initial_sync_context,
        );
        let (broadcaster, _) = broadcast::channel(10000);
        let adapter = Self {
            listener,
            storage,
            blocks: Default::default(),
            broadcaster,
            sync_context_tx,
            delayed_state_notifier: DelayedStateNotifier::default(),
        };
        tracing::info!(
            target : tracing_targets::STATE_NODE_ADAPTER,
            "Start watching for sync context updates"
        );
        tokio::spawn({
            let listener = adapter.listener.clone();
            let delayed_state_notifier = adapter.delayed_state_notifier.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    130u32,
                );
                while {
                    __guard.end_section(131u32);
                    let __result = sync_context_rx.changed().await;
                    __guard.start_section(131u32);
                    __result
                }
                    .is_ok()
                {
                    __guard.checkpoint(131u32);
                    let sync_ctx = *sync_context_rx.borrow();
                    {
                        __guard.end_section(156u32);
                        let __result = delayed_state_notifier
                            .send_delayed_if(
                                listener.clone(),
                                |delayed_sync_ctx| {
                                    let check = sync_ctx == CollatorSyncContext::Recent
                                        && delayed_sync_ctx != CollatorSyncContext::Recent;
                                    if check {
                                        tracing::debug!(
                                            target : tracing_targets::STATE_NODE_ADAPTER, sync_ctx = ?
                                            sync_ctx, delayed_sync_ctx = ? delayed_sync_ctx,
                                            "handle_sync_context_update: will process delayed state",
                                        );
                                    } else {
                                        tracing::debug!(
                                            target : tracing_targets::STATE_NODE_ADAPTER, sync_ctx = ?
                                            sync_ctx, delayed_sync_ctx = ? delayed_sync_ctx,
                                            "handle_sync_context_update: will not process delayed state",
                                        );
                                    }
                                    check
                                },
                            )
                            .await;
                        __guard.start_section(156u32);
                        __result
                    }
                        .unwrap();
                }
            }
        });
        tracing::info!(
            target : tracing_targets::STATE_NODE_ADAPTER, "State node adapter created"
        );
        adapter
    }
}
#[async_trait]
impl StateNodeAdapter for StateNodeAdapterStdImpl {
    fn load_last_applied_mc_block_id(&self) -> Result<BlockId> {
        let las_applied_mc_block_id = self
            .storage
            .node_state()
            .load_last_mc_block_id()
            .context("no blocks applied yet")?;
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER,
            "Loaded last applied mc block id {}", las_applied_mc_block_id.as_short_id(),
        );
        Ok(las_applied_mc_block_id)
    }
    async fn load_state(
        &self,
        ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_state)),
            file!(),
            189u32,
        );
        let ref_by_mc_seqno = ref_by_mc_seqno;
        let block_id = block_id;
        let _histogram = HistogramGuard::begin("tycho_collator_state_load_state_time");
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Load state: {}", block_id
            .as_short_id()
        );
        let state = {
            __guard.end_section(198u32);
            let __result = self
                .storage
                .shard_state_storage()
                .load_state(ref_by_mc_seqno, block_id)
                .await;
            __guard.start_section(198u32);
            __result
        }?;
        Ok(state)
    }
    async fn store_state_root(
        &self,
        block_id: &BlockId,
        meta: NewBlockMeta,
        state_root: Cell,
        hint: StoreStateHint,
    ) -> Result<bool> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_state_root)),
            file!(),
            209u32,
        );
        let block_id = block_id;
        let meta = meta;
        let state_root = state_root;
        let hint = hint;
        let labels = [("workchain", block_id.shard.workchain().to_string())];
        let _histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_state_store_state_root_time_high",
            &labels,
        );
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Store state root: {}",
            block_id.as_short_id()
        );
        let (handle, _) = self
            .storage
            .block_handle_storage()
            .create_or_load_handle(block_id, meta);
        let updated = {
            __guard.end_section(227u32);
            let __result = self
                .storage
                .shard_state_storage()
                .store_state_root(&handle, state_root, hint)
                .await;
            __guard.start_section(227u32);
            __result
        }?;
        Ok(updated)
    }
    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block)),
            file!(),
            232u32,
        );
        let block_id = block_id;
        let _histogram = HistogramGuard::begin("tycho_collator_state_load_block_time");
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Load block: {}", block_id
            .as_short_id()
        );
        let handle_storage = self.storage.block_handle_storage();
        match handle_storage.load_handle(block_id) {
            Some(handle) => {
                __guard.end_section(240u32);
                let __result = self.load_block_by_handle(&handle).await;
                __guard.start_section(240u32);
                __result
            }
            _ => Ok(None),
        }
    }
    async fn load_block_by_handle(
        &self,
        handle: &BlockHandle,
    ) -> Result<Option<BlockStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_by_handle)),
            file!(),
            245u32,
        );
        let handle = handle;
        if !handle.has_data() {
            {
                __guard.end_section(247u32);
                return Ok(None);
            };
        }
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Load block by handle: {}",
            handle.id().as_short_id()
        );
        let block_storage = self.storage.block_storage();
        {
            __guard.end_section(253u32);
            let __result = block_storage.load_block_data(handle).await;
            __guard.start_section(253u32);
            __result
        }
            .map(Some)
    }
    async fn load_block_handle(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<BlockHandle>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_handle)),
            file!(),
            256u32,
        );
        let block_id = block_id;
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Load block handle: {}",
            block_id.as_short_id()
        );
        Ok(self.storage.block_handle_storage().load_handle(block_id))
    }
    async fn get_ref_by_mc_seqno(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<BlockSeqno>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_ref_by_mc_seqno)),
            file!(),
            261u32,
        );
        let block_id = block_id;
        Ok(
            {
                __guard.end_section(264u32);
                let __result = self.load_block_handle(block_id).await;
                __guard.start_section(264u32);
                __result
            }?
                .map(|block_handle| block_handle.ref_by_mc_seqno()),
        )
    }
    fn accept_block(&self, block: Arc<BlockStuffForSync>) -> Result<()> {
        let block_id = *block.block_stuff_aug.id();
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Block accepted: {}", block_id
            .as_short_id()
        );
        self.blocks.entry(block_id.shard).or_default().insert(block_id.seqno, block);
        let broadcast_result = self.broadcaster.send(block_id).ok();
        tracing::trace!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Block broadcast_result: {:?}",
            broadcast_result
        );
        Ok(())
    }
    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_block)),
            file!(),
            283u32,
        );
        let block_id = block_id;
        let block_id = BlockIdToWait::Full(block_id);
        {
            __guard.end_section(285u32);
            let __result = self.wait_for_block_ext(block_id).await;
            __guard.start_section(285u32);
            __result
        }
    }
    async fn wait_for_block_next(
        &self,
        prev_block_id: &BlockId,
    ) -> Option<Result<BlockStuffAug>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_block_next)),
            file!(),
            288u32,
        );
        let prev_block_id = prev_block_id;
        let next_block_id_short = BlockIdShort::from((
            prev_block_id.shard,
            prev_block_id.seqno + 1,
        ));
        let block_id = BlockIdToWait::Short(&next_block_id_short);
        {
            __guard.end_section(292u32);
            let __result = self.wait_for_block_ext(block_id).await;
            __guard.start_section(292u32);
            __result
        }
    }
    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_state)),
            file!(),
            295u32,
        );
        let state = state;
        let _histogram = HistogramGuard::begin(
            "tycho_collator_state_adapter_handle_state_time",
        );
        let sync_context = *self.sync_context_tx.borrow();
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "handle_state: block {}", state
            .block_id()
        );
        let block_id = *state.block_id();
        let mut to_split = Vec::new();
        let shard = block_id.shard;
        let seqno = block_id.seqno;
        {
            let has_block = if let Some(shard_blocks) = self.blocks.get(&shard) {
                let has_block = shard_blocks.contains_key(&seqno);
                if shard.is_masterchain() {
                    let prev_mc_block = shard_blocks
                        .range(..=seqno)
                        .rev()
                        .find_map(|(&key, value)| {
                            if key < seqno { Some(value) } else { None }
                        });
                    if let Some(prev_mc_block) = prev_mc_block {
                        for id in &prev_mc_block.top_shard_blocks_ids {
                            __guard.checkpoint(319u32);
                            to_split.push((id.shard, id.seqno + 1));
                        }
                        to_split
                            .push((shard, prev_mc_block.block_stuff_aug.id().seqno + 1));
                    }
                }
                has_block
            } else {
                false
            };
            {
                __guard.end_section(350u32);
                let __result = self
                    .delayed_state_notifier
                    .send_or_delay(
                        self.listener.clone(),
                        state.clone(),
                        !has_block,
                        sync_context,
                        |sync_ctx| {
                            let check = sync_ctx == CollatorSyncContext::Recent;
                            if !check {
                                tracing::debug!(
                                    target : tracing_targets::STATE_NODE_ADAPTER, block_id = %
                                    state.block_id().as_short_id(), sync_ctx = ? sync_context,
                                    "handle_state: will delay state",
                                );
                            }
                            check
                        },
                    )
                    .await;
                __guard.start_section(350u32);
                __result
            }?;
        }
        let mut to_drop = Vec::new();
        for (shard, seqno) in &to_split {
            __guard.checkpoint(354u32);
            if let Some(mut shard_blocks) = self.blocks.get_mut(shard) {
                let retained_blocks = shard_blocks.split_off(seqno);
                to_drop.push(std::mem::replace(&mut *shard_blocks, retained_blocks));
            }
        }
        Reclaimer::instance().drop(to_drop);
        Ok(())
    }
    async fn load_diff(&self, block_id: &BlockId) -> Result<Option<QueueDiffStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_diff)),
            file!(),
            367u32,
        );
        let block_id = block_id;
        let _histogram = HistogramGuard::begin(
            "tycho_collator_state_load_queue_diff_time",
        );
        tracing::debug!(
            target : tracing_targets::STATE_NODE_ADAPTER, "Load queue diff: {}", block_id
            .as_short_id()
        );
        let handle_storage = self.storage.block_handle_storage();
        let block_storage = self.storage.block_storage();
        match handle_storage.load_handle(block_id) {
            Some(handle) if handle.has_queue_diff() => {
                {
                    __guard.end_section(377u32);
                    let __result = block_storage.load_queue_diff(&handle).await;
                    __guard.start_section(377u32);
                    __result
                }
                    .map(Some)
            }
            _ => Ok(None),
        }
    }
    fn set_sync_context(&self, sync_context: CollatorSyncContext) {
        self.sync_context_tx
            .send_if_modified(|curr| {
                if *curr != sync_context {
                    *curr = sync_context;
                    true
                } else {
                    false
                }
            });
    }
    fn load_init_block_id(&self) -> Option<BlockId> {
        self.storage.node_state().load_init_mc_block_id()
    }
}
impl StateNodeAdapterStdImpl {
    async fn wait_for_block_ext(
        &self,
        block_id: BlockIdToWait<'_>,
    ) -> Option<Result<BlockStuffAug>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_block_ext)),
            file!(),
            403u32,
        );
        let block_id = block_id;
        let mut receiver = self.broadcaster.subscribe();
        loop {
            __guard.checkpoint(405u32);
            if let Some(shard_blocks) = self.blocks.get(&block_id.shard()) {
                let block = shard_blocks.get(&block_id.seqno()).cloned();
                drop(shard_blocks);
                if let Some(block) = block {
                    {
                        __guard.end_section(411u32);
                        return match {
                            __guard.end_section(411u32);
                            let __result = self.save_block_proof(&block).await;
                            __guard.start_section(411u32);
                            __result
                        } {
                            Ok(_) => Some(Ok(block.block_stuff_aug.clone())),
                            Err(e) => {
                                Some(Err(anyhow!("failed to save block proof: {e:?}")))
                            }
                        };
                    };
                }
            }
            loop {
                __guard.checkpoint(418u32);
                match {
                    __guard.end_section(419u32);
                    let __result = receiver.recv().await;
                    __guard.start_section(419u32);
                    __result
                } {
                    Ok(received_block_id) if block_id == received_block_id => {
                        {
                            __guard.end_section(421u32);
                            __guard.start_section(421u32);
                            break;
                        };
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(
                            target : tracing_targets::STATE_NODE_ADAPTER,
                            "Broadcast channel lagged: {}", count
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(
                            target : tracing_targets::STATE_NODE_ADAPTER,
                            "Broadcast channel closed"
                        );
                        {
                            __guard.end_section(429u32);
                            return None;
                        };
                    }
                }
            }
        }
    }
    async fn save_block_proof(&self, block: &Arc<BlockStuffForSync>) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(save_block_proof)),
            file!(),
            436u32,
        );
        let block = block;
        let (block_info, archive_data) = {
            __guard.end_section(462u32);
            let __result = rayon_run({
                    let block = block.clone();
                    move || {
                        let PreparedProof { proof, block_info } = prepare_block_proof(
                                &block.block_stuff_aug.data,
                                &block.consensus_info,
                                &block.signatures,
                                block.total_signature_weight,
                            )
                            .unwrap_or_else(|e| {
                                panic!(
                                    "failed to prepare block proof for {:?}: {e:?}", block
                                    .block_stuff_aug.id()
                                )
                            });
                        let block_proof_stuff = BlockProofStuff::from_proof(proof);
                        let proof_boc = BocRepr::encode_rayon(block_proof_stuff.as_ref())
                            .expect("valid block proof must be successfully serialized");
                        let archive_data = block_proof_stuff
                            .with_archive_data(proof_boc);
                        (block_info, archive_data)
                    }
                })
                .await;
            __guard.start_section(462u32);
            __result
        };
        let _histogram = HistogramGuard::begin(
            "tycho_collator_state_adapter_save_block_proof_time_high",
        );
        let block_storage = self.storage.block_storage();
        let result = {
            __guard.end_section(477u32);
            let __result = block_storage
                .store_block_proof(
                    &archive_data,
                    MaybeExistingHandle::New(NewBlockMeta {
                        is_key_block: block_info.key_block,
                        gen_utime: block_info.gen_utime,
                        ref_by_mc_seqno: block.ref_by_mc_seqno,
                    }),
                )
                .await;
            __guard.start_section(477u32);
            __result
        }?;
        let is_new_proof = result.new;
        let is_proof_updated = result.updated;
        let result = {
            __guard.end_section(483u32);
            let __result = block_storage
                .store_queue_diff(&block.queue_diff_aug, result.handle.into())
                .await;
            __guard.start_section(483u32);
            __result
        }?;
        let is_new_diff = result.new;
        let is_diff_updated = result.updated;
        tracing::info!(
            block_id = % result.handle.id(), is_new_proof, is_proof_updated, is_new_diff,
            is_diff_updated, "block saved",
        );
        Ok(())
    }
}
#[derive(Clone)]
struct DelayedStateContext {
    pub state: ShardStateStuff,
    pub is_external: bool,
    pub sync_context: CollatorSyncContext,
}
#[derive(Default, Clone)]
struct DelayedStateNotifier {
    inner: Arc<Mutex<Option<DelayedStateContext>>>,
}
impl DelayedStateNotifier {
    pub async fn send_delayed_if<F>(
        &self,
        listener: Arc<dyn StateNodeEventListener>,
        check_should_send: F,
    ) -> Result<()>
    where
        F: Fn(CollatorSyncContext) -> bool,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(send_delayed_if)),
            file!(),
            519u32,
        );
        let listener = listener;
        let check_should_send = check_should_send;
        let state_cx = {
            let mut guard = self.inner.lock();
            match guard.as_ref() {
                Some(state_cx) if check_should_send(state_cx.sync_context) => {
                    guard.take()
                }
                _ => None,
            }
        };
        {
            __guard.end_section(531u32);
            let __result = Self::send_impl(listener, state_cx).await;
            __guard.start_section(531u32);
            __result
        }
    }
    pub async fn send_or_delay<F>(
        &self,
        listener: Arc<dyn StateNodeEventListener>,
        state: ShardStateStuff,
        is_external: bool,
        sync_context: CollatorSyncContext,
        check_should_send: F,
    ) -> Result<()>
    where
        F: Fn(CollatorSyncContext) -> bool,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(send_or_delay)),
            file!(),
            544u32,
        );
        let listener = listener;
        let state = state;
        let is_external = is_external;
        let sync_context = sync_context;
        let check_should_send = check_should_send;
        let state_cx = DelayedStateContext {
            state,
            is_external,
            sync_context,
        };
        let state_cx = {
            let mut guard = self.inner.lock();
            if check_should_send(state_cx.sync_context) {
                guard.take();
                Some(state_cx)
            } else {
                guard.replace(state_cx);
                None
            }
        };
        {
            __guard.end_section(564u32);
            let __result = Self::send_impl(listener, state_cx).await;
            __guard.start_section(564u32);
            __result
        }
    }
    async fn send_impl(
        listener: Arc<dyn StateNodeEventListener>,
        state_cx: Option<DelayedStateContext>,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(send_impl)),
            file!(),
            570u32,
        );
        let listener = listener;
        let state_cx = state_cx;
        let Some(DelayedStateContext { state, is_external, .. }) = state_cx else {
            {
                __guard.end_section(575u32);
                return Ok(());
            };
        };
        if is_external {
            tracing::info!(
                target : tracing_targets::STATE_NODE_ADAPTER,
                "handle_state: handled external: {}", state.block_id()
            );
            {
                __guard.end_section(580u32);
                let __result = listener.on_block_accepted_external(&state).await;
                __guard.start_section(580u32);
                __result
            }
        } else {
            tracing::info!(
                target : tracing_targets::STATE_NODE_ADAPTER,
                "handle_state: handled own: {}", state.block_id()
            );
            {
                __guard.end_section(583u32);
                let __result = listener.on_block_accepted(&state).await;
                __guard.start_section(583u32);
                __result
            }
        }
    }
}
#[expect(
    clippy::disallowed_methods,
    reason = "We are working with a virtual block here, so `load_extra` and other methods are necessary"
)]
fn prepare_block_proof(
    block_stuff: &BlockStuff,
    consensus_info: &ConsensusInfo,
    signatures: &FastHashMap<PeerId, ArcSignature>,
    total_signature_weight: u64,
) -> Result<PreparedProof> {
    let _histogram = HistogramGuard::begin(
        "tycho_collator_state_adapter_prepare_block_proof_time",
    );
    let usage_tree = UsageTree::new(UsageTreeMode::OnLoad).with_subtrees();
    let tracked_cell = usage_tree.track(block_stuff.root_cell());
    let block = tracked_cell.parse::<Block>()?;
    block.value_flow.inner().as_ref().touch_recursive();
    let block_info = block.load_info()?;
    block_info.load_prev_ref()?;
    block_info.prev_vert_ref.as_ref().map(|x| x.load());
    block_info.master_ref.as_ref().map(|x| x.load());
    let _state_update = block.load_state_update();
    if let Some(custom) = block.load_extra()?.load_custom()? {
        if let Some(root) = custom.shards.as_dict().root() {
            root.touch_recursive();
        }
        if let Some(config) = &custom.config {
            config.get::<ConfigParam28>()?;
            for param_id in 32..=38 {
                if let Some(mut vset) = config.get_raw(param_id)? {
                    ValidatorSet::load_from(&mut vset)?;
                }
            }
        }
    }
    let merkle_proof = MerkleProof::create(block_stuff.root_cell().as_ref(), usage_tree)
        .build()?;
    let root = CellBuilder::build_from(merkle_proof)?;
    let signatures = if block_stuff.id().is_masterchain() {
        Some(
            process_signatures(
                block_info.gen_validator_list_hash_short,
                block_info.gen_catchain_seqno,
                total_signature_weight,
                consensus_info,
                signatures,
            )?,
        )
    } else {
        None
    };
    Ok(PreparedProof {
        proof: Box::new(BlockProof {
            proof_for: *block_stuff.id(),
            root,
            signatures,
        }),
        block_info,
    })
}
fn process_signatures(
    gen_validator_list_hash_short: u32,
    gen_session_seqno: u32,
    total_weight: u64,
    consensus_info: &ConsensusInfo,
    block_signatures: &FastHashMap<PeerId, ArcSignature>,
) -> Result<tycho_types::models::block::BlockSignatures> {
    use tycho_types::dict;
    let signatures = Dict::from_raw(
        dict::build_dict_from_sorted_iter(
            block_signatures
                .iter()
                .enumerate()
                .map(|(i, (key, value))| {
                    let key_hash = tl_proto::hash(tycho_crypto::tl::PublicKey::Ed25519 {
                        key: key.as_bytes(),
                    });
                    (
                        i as u16,
                        BlockSignature {
                            node_id_short: key_hash.into(),
                            signature: Signature(*value.as_ref()),
                        },
                    )
                }),
            Cell::empty_context(),
        )?,
    );
    Ok(tycho_types::models::block::BlockSignatures {
        validator_info: ValidatorBaseInfo {
            validator_list_hash_short: gen_validator_list_hash_short,
            catchain_seqno: gen_session_seqno,
        },
        consensus_info: *consensus_info,
        signature_count: block_signatures.len() as u32,
        total_weight,
        signatures,
    })
}
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CollatorSyncContext {
    Persistent,
    Historical,
    Recent,
}
struct PreparedProof {
    proof: Box<BlockProof>,
    block_info: BlockInfo,
}
enum BlockIdToWait<'a> {
    Short(&'a BlockIdShort),
    Full(&'a BlockId),
}
impl BlockIdToWait<'_> {
    fn shard(&self) -> ShardIdent {
        match self {
            Self::Short(id) => id.shard,
            Self::Full(id) => id.shard,
        }
    }
    fn seqno(&self) -> u32 {
        match self {
            Self::Short(id) => id.seqno,
            Self::Full(id) => id.seqno,
        }
    }
}
impl PartialEq<BlockId> for BlockIdToWait<'_> {
    fn eq(&self, other: &BlockId) -> bool {
        match *self {
            BlockIdToWait::Short(short) => &other.as_short_id() == short,
            BlockIdToWait::Full(full) => full == other,
        }
    }
}
