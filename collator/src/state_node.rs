use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use everscale_types::boc::BocRepr;
use everscale_types::merkle::MerkleProof;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tokio::sync::broadcast;
use tycho_block_util::block::{BlockProofStuff, BlockStuff, BlockStuffAug};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_storage::{BlockHandle, BlockMetaData, MaybeExistingHandle, Storage};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastDashMap, FastHashMap};

use crate::tracing_targets;
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
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId>;
    /// Return master or shard state on specified block from node local state
    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff>;
    /// Store shard state root in the storage and make a new `ShardStateStuff` from it.
    async fn store_state_root(
        &self,
        block_id: &BlockId,
        meta: BlockMetaData,
        state_root: Cell,
    ) -> Result<ShardStateStuff>;
    /// Return block by it's id from node local state
    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>>;
    /// Return block handle by it's id from node local state
    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>>;
    /// Accept block:
    /// 1. (TODO) Broadcast block to blockchain network
    /// 2. Provide block to the block strider
    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()>;
    /// Waits for the specified block to be received and returns it
    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
    /// Waits for the specified block by prev_id to be received and returns it
    async fn wait_for_block_next(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
    /// Handle state after block was applied
    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()>;
}

pub struct StateNodeAdapterStdImpl {
    listener: Arc<dyn StateNodeEventListener>,
    blocks: FastDashMap<ShardIdent, BTreeMap<u32, BlockStuffForSync>>,
    storage: Storage,
    broadcaster: broadcast::Sender<BlockId>,
}

impl StateNodeAdapterStdImpl {
    pub fn new(listener: Arc<dyn StateNodeEventListener>, storage: Storage) -> Self {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");

        let (broadcaster, _) = broadcast::channel(10000);
        Self {
            listener,
            storage,
            blocks: Default::default(),
            broadcaster,
        }
    }
}

#[async_trait]
impl StateNodeAdapter for StateNodeAdapterStdImpl {
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load last applied mc block id");
        self.storage
            .node_state()
            .load_last_mc_block_id()
            .context("no blocks applied yet")
    }

    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load state: {}", block_id.as_short_id());
        let state = self
            .storage
            .shard_state_storage()
            .load_state(block_id)
            .await?;
        Ok(state)
    }

    async fn store_state_root(
        &self,
        block_id: &BlockId,
        meta: BlockMetaData,
        state_root: Cell,
    ) -> Result<ShardStateStuff> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Store state root: {}", block_id.as_short_id());

        let (handle, _) = self
            .storage
            .block_handle_storage()
            .create_or_load_handle(block_id, meta);

        self.storage
            .shard_state_storage()
            .store_state_root(&handle, state_root)
            .await?;

        let state = self
            .storage
            .shard_state_storage()
            .load_state(block_id)
            .await?;
        Ok(state)
    }

    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block: {}", block_id.as_short_id());

        let handle_storage = self.storage.block_handle_storage();
        let block_storage = self.storage.block_storage();

        let Some(handle) = handle_storage.load_handle(block_id) else {
            return Ok(None);
        };
        block_storage.load_block_data(&handle).await.map(Some)
    }

    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>> {
        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block handle: {}", block_id.as_short_id());
        Ok(self.storage.block_handle_storage().load_handle(block_id))
    }

    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()> {
        let block_id = *block.block_stuff_aug.id();

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Block accepted: {}", block_id.as_short_id());

        self.blocks
            .entry(block_id.shard)
            .or_insert_with(BTreeMap::new)
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
        let _histogram = HistogramGuard::begin("tycho_collator_adapter_handle_state_time");

        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "Handle block: {}", state.block_id().as_short_id());
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

            match has_block {
                false => {
                    let _histogram =
                        HistogramGuard::begin("tycho_collator_adapter_on_block_accepted_ext_time");

                    tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block handled external: {:?}", block_id);
                    self.listener.on_block_accepted_external(state).await?;
                }
                true => {
                    let _histogram =
                        HistogramGuard::begin("tycho_collator_adapter_on_block_accepted_time");

                    tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block handled: {:?}", block_id);
                    self.listener.on_block_accepted(state).await?;
                }
            }
        }

        for (shard, seqno) in &to_split {
            if let Some(mut shard_blocks) = self.blocks.get_mut(shard) {
                *shard_blocks = shard_blocks.split_off(seqno);
            }
        }

        Ok(())
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
                if let Some(block) = shard_blocks.get(&block_id.seqno()) {
                    return match self.save_block_proof(block).await {
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
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(target: tracing_targets::STATE_NODE_ADAPTER, "Broadcast channel lagged: {}", count);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(target: tracing_targets::STATE_NODE_ADAPTER, "Broadcast channel closed");
                        return None;
                    }
                }
            }
        }
    }

    async fn save_block_proof(&self, block: &BlockStuffForSync) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_collator_save_block_proof_time");

        let PreparedProof { proof, block_info } =
            prepare_block_proof(&block.block_stuff_aug.data, &block.signatures)
                .context("failed to prepare block proof")?;

        let block_proof_stuff = BlockProofStuff::from_proof(proof);

        let proof_boc = BocRepr::encode_rayon(block_proof_stuff.as_ref())?;
        let archive_data = block_proof_stuff.with_archive_data(proof_boc);

        let result = self
            .storage
            .block_storage()
            .store_block_proof(
                &archive_data,
                MaybeExistingHandle::New(BlockMetaData {
                    is_key_block: block_info.key_block,
                    gen_utime: block_info.gen_utime,
                    mc_ref_seqno: None,
                }),
            )
            .await?;

        tracing::info!(
            "Proof saved {:?}. New: {}, Updated: {}",
            result.handle.id(),
            result.new,
            result.updated
        );

        Ok(())
    }
}

// TODO: This should possibly panic on error?
fn prepare_block_proof(
    block_stuff: &BlockStuff,
    signatures: &FastHashMap<PeerId, ArcSignature>,
) -> Result<PreparedProof> {
    let _histogram = HistogramGuard::begin("tycho_collator_prepare_block_proof_time");

    let mut usage_tree = UsageTree::new(UsageTreeMode::OnLoad).with_subtrees();
    let tracked_cell = usage_tree.track(block_stuff.root_cell());
    let block = tracked_cell.parse::<Block>()?;
    let subtree = block.value_flow.inner().as_ref();
    usage_tree.add_subtree(subtree);

    let block_info = block.load_info()?;

    block_info.load_prev_ref()?;
    block_info.prev_vert_ref.as_ref().map(|x| x.load());
    block_info.master_ref.as_ref().map(|x| x.load());
    let extra = block.load_extra().unwrap();

    let _state_update = block.load_state_update();
    extra.load_custom()?;

    let merkle_proof = MerkleProof::create(block_stuff.root_cell().as_ref(), usage_tree).build()?;

    let root = CellBuilder::build_from(merkle_proof)?;

    let signatures = if block_stuff.id().is_masterchain() {
        Some(process_signatures(
            block_info.gen_validator_list_hash_short,
            block_info.gen_catchain_seqno,
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
    gen_catchain_seqno: u32,
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
        16,
        &mut Cell::empty_context(),
    )?);

    let sig_count = block_signatures.len() as u32;

    Ok(everscale_types::models::block::BlockSignatures {
        validator_info: ValidatorBaseInfo {
            validator_list_hash_short: gen_validator_list_hash_short,
            catchain_seqno: gen_catchain_seqno,
        },
        signature_count: sig_count,
        total_weight: sig_count as u64,
        signatures,
    })
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
