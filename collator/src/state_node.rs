use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use everscale_types::boc::BocRepr;
use everscale_types::merkle::MerkleProof;
use everscale_types::models::*;
use everscale_types::prelude::*;
use parking_lot::Mutex;
use tokio::sync::{broadcast, watch};
use tycho_block_util::block::{BlockProofStuff, BlockStuff, BlockStuffAug};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_storage::{BlockHandle, MaybeExistingHandle, NewBlockMeta, Storage, StoreStateHint};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;
use tycho_util::{FastDashMap, FastHashMap};

use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;
use crate::types::{ArcSignature, BlockStuffForSync};

// FACTORY

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
    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff>;
    /// Return master or shard state root on specified block from node local state
    async fn load_state_root(&self, block_id: &BlockId) -> Result<Cell>;
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
    async fn load_block_by_handle(&self, handle: &BlockHandle) -> Result<Option<BlockStuff>>;
    /// Return block handle by its id from node local state
    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>>;
    /// Return `ref_by_mc_seqno` from block handle by its id from node local state
    async fn get_ref_by_mc_seqno(&self, block_id: &BlockId) -> Result<Option<BlockSeqno>>;
    /// Accept block:
    /// 1. (TODO) Broadcast block to blockchain network
    /// 2. Provide block to the block strider
    fn accept_block(&self, block: Arc<BlockStuffForSync>) -> Result<()>;
    /// Waits for the specified block to be received and returns it
    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
    /// Waits for the specified block by prev_id to be received and returns it
    async fn wait_for_block_next(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
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
    storage: Storage,
    broadcaster: broadcast::Sender<BlockId>,

    sync_context_tx: watch::Sender<CollatorSyncContext>,

    delayed_state_notifier: DelayedStateNotifier,
}

impl StateNodeAdapterStdImpl {
    pub fn new(
        listener: Arc<dyn StateNodeEventListener>,
        storage: Storage,
        initial_sync_context: CollatorSyncContext,
    ) -> Self {
        let (sync_context_tx, mut sync_context_rx) = watch::channel(initial_sync_context);
        let (broadcaster, _) = broadcast::channel(10000);
        let adapter = Self {
            listener,
            storage,
            blocks: Default::default(),
            broadcaster,
            sync_context_tx,
            delayed_state_notifier: DelayedStateNotifier::default(),
        };

        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Start watching for sync context updates");

        tokio::spawn({
            let listener = adapter.listener.clone();
            let delayed_state_notifier = adapter.delayed_state_notifier.clone();
            async move {
                while sync_context_rx.changed().await.is_ok() {
                    let sync_ctx = *sync_context_rx.borrow();

                    delayed_state_notifier
                        .send_delayed_if(listener.clone(), |delayed_sync_ctx| {
                            // send last delayed Persistent or Historical state when sync switched to recent blocks
                            let check = sync_ctx == CollatorSyncContext::Recent
                                && delayed_sync_ctx != CollatorSyncContext::Recent;
                            if check {
                                tracing::debug!(
                                    target: tracing_targets::STATE_NODE_ADAPTER,
                                    sync_ctx = ?sync_ctx,
                                    delayed_sync_ctx = ?delayed_sync_ctx,
                                    "handle_sync_context_update: will process delayed state",
                                );
                            } else {
                                tracing::debug!(
                                    target: tracing_targets::STATE_NODE_ADAPTER,
                                    sync_ctx = ?sync_ctx,
                                    delayed_sync_ctx = ?delayed_sync_ctx,
                                    "handle_sync_context_update: will not process delayed state",
                                );
                            }
                            check
                        })
                        .await
                        .unwrap();
                }
            }
        });

        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");

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

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER,
            "Loaded last applied mc block id {}",
            las_applied_mc_block_id.as_short_id(),
        );

        Ok(las_applied_mc_block_id)
    }

    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff> {
        let _histogram = HistogramGuard::begin("tycho_collator_state_load_state_time");

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load state: {}", block_id.as_short_id());

        let state = self
            .storage
            .shard_state_storage()
            .load_state(block_id)
            .await?;

        Ok(state)
    }

    async fn load_state_root(&self, block_id: &BlockId) -> Result<Cell> {
        let _histogram = HistogramGuard::begin("tycho_collator_state_load_state_root_time");

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load state root: {}", block_id.as_short_id());

        let cell = self
            .storage
            .shard_state_storage()
            .load_state_root(block_id)?;

        Ok(cell)
    }

    async fn store_state_root(
        &self,
        block_id: &BlockId,
        meta: NewBlockMeta,
        state_root: Cell,
        hint: StoreStateHint,
    ) -> Result<bool> {
        let labels = [("workchain", block_id.shard.workchain().to_string())];
        let _histogram = HistogramGuard::begin_with_labels(
            "tycho_collator_state_store_state_root_time_high",
            &labels,
        );

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Store state root: {}", block_id.as_short_id());

        let (handle, _) = self
            .storage
            .block_handle_storage()
            .create_or_load_handle(block_id, meta);

        let updated = self
            .storage
            .shard_state_storage()
            .store_state_root(&handle, state_root, hint)
            .await?;

        Ok(updated)
    }

    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>> {
        let _histogram = HistogramGuard::begin("tycho_collator_state_load_block_time");

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block: {}", block_id.as_short_id());

        let handle_storage = self.storage.block_handle_storage();

        match handle_storage.load_handle(block_id) {
            Some(handle) => self.load_block_by_handle(&handle).await,
            _ => Ok(None),
        }
    }

    async fn load_block_by_handle(&self, handle: &BlockHandle) -> Result<Option<BlockStuff>> {
        if !handle.has_data() {
            return Ok(None);
        }

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block by handle: {}", handle.id().as_short_id());

        let block_storage = self.storage.block_storage();
        block_storage.load_block_data(handle).await.map(Some)
    }

    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>> {
        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block handle: {}", block_id.as_short_id());
        Ok(self.storage.block_handle_storage().load_handle(block_id))
    }

    async fn get_ref_by_mc_seqno(&self, block_id: &BlockId) -> Result<Option<BlockSeqno>> {
        Ok(self
            .load_block_handle(block_id)
            .await?
            .map(|block_handle| block_handle.ref_by_mc_seqno()))
    }

    fn accept_block(&self, block: Arc<BlockStuffForSync>) -> Result<()> {
        let block_id = *block.block_stuff_aug.id();

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Block accepted: {}", block_id.as_short_id());

        self.blocks
            .entry(block_id.shard)
            .or_default()
            .insert(block_id.seqno, block);

        let broadcast_result = self.broadcaster.send(block_id).ok();
        tracing::trace!(target: tracing_targets::STATE_NODE_ADAPTER, "Block broadcast_result: {:?}", broadcast_result);
        Ok(())
    }

    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        let block_id = BlockIdToWait::Full(block_id);
        self.wait_for_block_ext(block_id).await
    }

    async fn wait_for_block_next(&self, prev_block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        let next_block_id_short =
            BlockIdShort::from((prev_block_id.shard, prev_block_id.seqno + 1));
        let block_id = BlockIdToWait::Short(&next_block_id_short);
        self.wait_for_block_ext(block_id).await
    }

    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_collator_state_adapter_handle_state_time");

        let sync_context = *self.sync_context_tx.borrow();

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "handle_state: block {}", state.block_id());
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
                        .find_map(|(&key, value)| if key < seqno { Some(value) } else { None });

                    if let Some(prev_mc_block) = prev_mc_block {
                        for id in &prev_mc_block.top_shard_blocks_ids {
                            to_split.push((id.shard, id.seqno + 1));
                        }
                        to_split.push((shard, prev_mc_block.block_stuff_aug.id().seqno + 1));
                    }
                }

                has_block
            } else {
                false
            };

            self.delayed_state_notifier
                .send_or_delay(
                    self.listener.clone(),
                    state.clone(),
                    !has_block,
                    sync_context,
                    |sync_ctx| {
                        let check = sync_ctx == CollatorSyncContext::Recent;
                        if !check {
                            tracing::debug!(
                                target: tracing_targets::STATE_NODE_ADAPTER,
                                block_id = %state.block_id().as_short_id(),
                                sync_ctx = ?sync_context,
                                "handle_state: will delay state",
                            );
                        }
                        check
                    },
                )
                .await?;
        }

        for (shard, seqno) in &to_split {
            if let Some(mut shard_blocks) = self.blocks.get_mut(shard) {
                *shard_blocks = shard_blocks.split_off(seqno);
            }
        }

        Ok(())
    }

    async fn load_diff(&self, block_id: &BlockId) -> Result<Option<QueueDiffStuff>> {
        let _histogram = HistogramGuard::begin("tycho_collator_state_load_queue_diff_time");

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load queue diff: {}", block_id.as_short_id());

        let handle_storage = self.storage.block_handle_storage();
        let block_storage = self.storage.block_storage();

        match handle_storage.load_handle(block_id) {
            Some(handle) if handle.has_queue_diff() => {
                block_storage.load_queue_diff(&handle).await.map(Some)
            }
            _ => Ok(None),
        }
    }

    fn set_sync_context(&self, sync_context: CollatorSyncContext) {
        self.sync_context_tx.send_if_modified(|curr| {
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
        let mut receiver = self.broadcaster.subscribe();
        loop {
            if let Some(shard_blocks) = self.blocks.get(&block_id.shard()) {
                let block = shard_blocks.get(&block_id.seqno()).cloned();
                drop(shard_blocks);

                if let Some(block) = block {
                    return match self.save_block_proof(&block).await {
                        Ok(_) => Some(Ok(block.block_stuff_aug.clone())),
                        Err(e) => Some(Err(anyhow!("failed to save block proof: {e:?}"))),
                    };
                }
            }

            loop {
                match receiver.recv().await {
                    Ok(received_block_id) if block_id == received_block_id => {
                        break;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(target: tracing_targets::STATE_NODE_ADAPTER, "Broadcast channel lagged: {}", count);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(target: tracing_targets::STATE_NODE_ADAPTER, "Broadcast channel closed");
                        return None;
                    }
                }
            }
        }
    }

    async fn save_block_proof(&self, block: &Arc<BlockStuffForSync>) -> Result<()> {
        let (block_info, archive_data) = rayon_run({
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
                        "failed to prepare block proof for {:?}: {e:?}",
                        block.block_stuff_aug.id()
                    )
                });

                let block_proof_stuff = BlockProofStuff::from_proof(proof);

                let proof_boc = BocRepr::encode_rayon(block_proof_stuff.as_ref())
                    .expect("valid block proof must be successfully serialized");
                let archive_data = block_proof_stuff.with_archive_data(proof_boc);

                (block_info, archive_data)
            }
        })
        .await;

        let _histogram =
            HistogramGuard::begin("tycho_collator_state_adapter_save_block_proof_time_high");

        let block_storage = self.storage.block_storage();
        let result = block_storage
            .store_block_proof(
                &archive_data,
                MaybeExistingHandle::New(NewBlockMeta {
                    is_key_block: block_info.key_block,
                    gen_utime: block_info.gen_utime,
                    ref_by_mc_seqno: block.ref_by_mc_seqno,
                }),
            )
            .await?;
        let is_new_proof = result.new;
        let is_proof_updated = result.updated;

        let result = block_storage
            .store_queue_diff(&block.queue_diff_aug, result.handle.into())
            .await?;
        let is_new_diff = result.new;
        let is_diff_updated = result.updated;

        tracing::info!(
            block_id = %result.handle.id(),
            is_new_proof,
            is_proof_updated,
            is_new_diff,
            is_diff_updated,
            "block saved",
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
        let state_cx = {
            let mut guard = self.inner.lock();
            match guard.as_ref() {
                Some(state_cx) if check_should_send(state_cx.sync_context) => guard.take(),
                _ => None,
            }
        };

        // do nothing if no delayed state
        // do nothing if should not send according to check

        Self::send_impl(listener, state_cx).await
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

        // send if needed according to check

        Self::send_impl(listener, state_cx).await
    }

    async fn send_impl(
        listener: Arc<dyn StateNodeEventListener>,
        state_cx: Option<DelayedStateContext>,
    ) -> Result<()> {
        let Some(DelayedStateContext {
            state, is_external, ..
        }) = state_cx
        else {
            return Ok(());
        };

        if is_external {
            tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "handle_state: handled external: {}", state.block_id());
            listener.on_block_accepted_external(&state).await
        } else {
            tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "handle_state: handled own: {}", state.block_id());
            listener.on_block_accepted(&state).await
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
    let _histogram = HistogramGuard::begin("tycho_collator_state_adapter_prepare_block_proof_time");

    let mut usage_tree = UsageTree::new(UsageTreeMode::OnLoad).with_subtrees();
    let tracked_cell = usage_tree.track(block_stuff.root_cell());
    let block = tracked_cell.parse::<Block>()?;
    let subtree = block.value_flow.inner().as_ref();
    usage_tree.add_subtree(subtree);

    let block_info = block.load_info()?;

    block_info.load_prev_ref()?;
    block_info.prev_vert_ref.as_ref().map(|x| x.load());
    block_info.master_ref.as_ref().map(|x| x.load());

    // NOTE: Make sure to "visit" the `out_msg_queue_updates` if we add some
    //       child cells to it. For now it is loaded inside `.parse::<Block>()`.

    let _state_update = block.load_state_update();

    if let Some(custom) = block.load_extra()?.load_custom()? {
        if let Some(config) = &custom.config {
            config.get::<ConfigParam28>()?;
            for param_id in 32..=38 {
                if let Some(mut vset) = config.get_raw(param_id)? {
                    ValidatorSet::load_from(&mut vset)?;
                }
            }
        }
    }

    let merkle_proof = MerkleProof::create(block_stuff.root_cell().as_ref(), usage_tree).build()?;

    let root = CellBuilder::build_from(merkle_proof)?;

    let signatures = if block_stuff.id().is_masterchain() {
        Some(process_signatures(
            block_info.gen_validator_list_hash_short,
            block_info.gen_catchain_seqno,
            total_signature_weight,
            consensus_info,
            signatures,
        )?)
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
) -> Result<everscale_types::models::block::BlockSignatures> {
    use everscale_types::dict;

    // TODO: Add helper for owned iter
    let signatures = Dict::from_raw(dict::build_dict_from_sorted_iter(
        block_signatures
            .iter()
            .enumerate()
            .map(|(i, (key, value))| {
                let key_hash = tl_proto::hash(everscale_crypto::tl::PublicKey::Ed25519 {
                    key: key.as_bytes(),
                });

                (i as u16, BlockSignature {
                    node_id_short: key_hash.into(),
                    signature: Signature(*value.as_ref()),
                })
            }),
        Cell::empty_context(),
    )?);

    Ok(everscale_types::models::block::BlockSignatures {
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
