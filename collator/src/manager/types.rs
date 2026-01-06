use std::fmt::{Debug, Display};
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tycho_block_util::queue::{QueueDiffStuff, QueuePartitionIdx};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_types::cell::Lazy;
use tycho_types::models::{
    BlockId, BlockIdShort, BlockInfo, OutMsgDescr, ProcessedUptoInfo, ShardIdent,
};
use tycho_util::{FastHashMap, FastHashSet};

use crate::mempool::MempoolAnchorId;
use crate::types::processed_upto::{
    BlockSeqno, ProcessedUptoInfoExtension, ProcessedUptoInfoStuff,
};
use crate::types::{
    ArcSignature, BlockCandidate, BlockStuffForSync, DebugDisplayOpt, ShardDescriptionShortExt,
    ShardHashesExt, TopBlockId, TopBlockIdUpdated,
};
use crate::utils::block::detect_top_processed_to_anchor;

pub(super) type BlockCacheKey = BlockIdShort;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CollatorState {
    Active,
    Waiting,
    Cancelled,
    CancelPending,
}

pub(super) struct ActiveCollator<C> {
    pub collator: C,
    pub state: CollatorState,

    /// For graceful collation cancellation
    pub cancel_collation: Arc<Notify>,
}

#[derive(Default)]
pub(super) struct CollationSyncState {
    /// Latest known chain time for master block: last imported or next to be collated
    pub mc_block_latest_chain_time: u64,
    /// Master block collation is forced for all shards anyway
    pub mc_collation_forced_for_all: bool,
    /// Collation states by shards. Stores collation status and
    /// additional info for manager to detect next collation step of every shard.
    pub states: FastHashMap<ShardIdent, CollationState>,
    /// If we have running sync then stores the target mc block seqno and cancellation token
    pub active_sync_to_applied: Option<ActiveSync>,
    /// Seqno of last received master block which may be not saved to cache yet
    pub last_received_mc_block_seqno: Option<BlockSeqno>,
    /// Last received applied master block id we have synced to
    pub last_synced_to_mc_block_id: Option<BlockId>,
    /// Master block collation forced by no pending messages after block
    /// in any shard on chain time
    pub mc_forced_by_no_pending_msgs_on_ct: Option<u64>,
}

#[derive(Debug, Default, Clone)]
pub(super) struct ImportedAnchorEvent {
    pub ct: u64,
    pub mc_forced: bool,
    pub collated_block_info: Option<CollatedBlockInfo>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CollatedBlockInfo {
    pub prev_mc_block_seqno: BlockSeqno,
    pub has_processed_externals: bool,
}

impl CollatedBlockInfo {
    pub fn new(prev_mc_block_seqno: BlockSeqno, has_processed_externals: bool) -> Self {
        Self {
            prev_mc_block_seqno,
            has_processed_externals,
        }
    }
}

#[derive(Debug)]
pub(super) struct CollationState {
    pub status: CollationStatus,
    pub last_imported_anchor_events: Vec<ImportedAnchorEvent>,
}

impl Default for CollationState {
    fn default() -> Self {
        Self {
            status: CollationStatus::AttemptsInProgress,
            last_imported_anchor_events: vec![],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CollationStatus {
    /// Collator is trying to collate current shard,
    /// may import next anchor
    AttemptsInProgress,
    /// Shard collator is waiting for master collator status,
    /// and not trying to collate current shard,
    /// will not import next anchor
    WaitForMasterStatus,
    /// Master collator is waiting for shard status,
    /// and not trying to collate master,
    /// will not import next anchor
    WaitForShardStatus,
    /// Current shard is ready for master collation,
    /// collator is not trying to collate current shard,
    /// will not import next anchor
    ReadyToCollateMaster,
}

#[derive(Debug)]
pub(super) enum NextCollationStep {
    WaitForMasterStatus,
    WaitForShardStatus,
    ResumeAttemptsIn(Vec<ShardIdent>),
    CollateMaster(u64),
}

pub(super) struct ActiveSync {
    pub target_mc_block_seqno: BlockSeqno,
    pub cancelled: CancellationToken,
}

pub(super) struct BlockCacheStoreResult {
    pub received_and_collated: bool,
    pub block_mismatch: bool,
    /// We need this to manage queue recovery on sync
    pub last_collated_mc_block_id: Option<BlockId>,
    pub applied_mc_queue_range: Option<(BlockSeqno, BlockSeqno)>,
}

impl Debug for BlockCacheStoreResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCacheStoreResult")
            .field("received_and_collated", &self.received_and_collated)
            .field("block_mismatch", &self.block_mismatch)
            .field(
                "last_collated_mc_block_id",
                &DebugDisplayOpt(self.last_collated_mc_block_id),
            )
            .field("applied_mc_queue_range", &self.applied_mc_queue_range)
            .finish()
    }
}

#[derive(Clone)]
pub(super) struct BlockCandidateStuff {
    pub candidate: BlockCandidate,
    pub signatures: FastHashMap<PeerId, ArcSignature>,
    pub total_signature_weight: u64,
}

impl BlockCandidateStuff {
    pub fn into_block_for_sync(self) -> Arc<BlockStuffForSync> {
        let BlockCandidateStuff {
            candidate,
            signatures,
            total_signature_weight,
        } = self;

        let BlockCandidate {
            ref_by_mc_seqno,
            block: block_stuff_aug,
            prev_blocks_ids,
            top_shard_blocks_ids,
            queue_diff_aug,
            consensus_info,
            ..
        } = candidate;

        Arc::new(BlockStuffForSync {
            ref_by_mc_seqno,
            block_stuff_aug,
            queue_diff_aug,
            signatures,
            total_signature_weight,
            prev_blocks_ids,
            top_shard_blocks_ids,
            consensus_info,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub(super) enum CandidateStatus {
    Collated,
    Validated,
    Synced,
}

#[expect(clippy::large_enum_variant)]
pub(super) enum BlockCacheEntryData {
    Collated {
        /// Collated block candidate with signatures
        candidate_stuff: BlockCandidateStuff,

        /// Candidate lifecycle status
        status: CandidateStatus,

        /// Whether the block was received after collation
        received_after_collation: bool,
    },
    Received {
        /// Cached state of the applied master block
        cached_state: Option<ShardStateStuff>,
        /// Applied block queue diff
        queue_diff: QueueDiffStuff,
        /// Applied block out messages
        out_msgs: Lazy<OutMsgDescr>,

        /// Whether the block was collated after receiving
        collated_after_receive: bool,

        /// Additional shard block cache info
        additional_shard_block_cache_info: Option<AdditionalShardBlockCacheInfo>,

        /// Processed to info for every partition
        processed_upto: ProcessedUptoInfoStuff,
    },
}

impl BlockCacheEntryData {
    pub fn get_additional_shard_block_cache_info(
        &self,
    ) -> Result<Option<AdditionalShardBlockCacheInfo>> {
        Ok(match self {
            Self::Collated {
                candidate_stuff, ..
            } => Some(AdditionalShardBlockCacheInfo {
                processed_to_anchor_id: candidate_stuff.candidate.processed_to_anchor_id,
                block_info: candidate_stuff.candidate.block.load_info()?.clone(),
            }),
            Self::Received {
                additional_shard_block_cache_info,
                ..
            } => additional_shard_block_cache_info.clone(),
        })
    }

    pub fn processed_upto(&self) -> &ProcessedUptoInfoStuff {
        match self {
            Self::Received { processed_upto, .. }
            | Self::Collated {
                candidate_stuff:
                    BlockCandidateStuff {
                        candidate: BlockCandidate { processed_upto, .. },
                        ..
                    },
                ..
            } => processed_upto,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdditionalShardBlockCacheInfo {
    pub block_info: BlockInfo,
    pub processed_to_anchor_id: u32,
}

impl Display for BlockCacheEntryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Collated {
                received_after_collation,
                ..
            } => f
                .debug_struct("Collated")
                .field("received_after_collation", received_after_collation)
                .finish(),
            Self::Received {
                collated_after_receive,
                ..
            } => f
                .debug_struct("Received")
                .field("collated_after_receive", collated_after_receive)
                .finish(),
        }
    }
}

pub(super) struct BlockCacheEntry {
    pub block_id: BlockId,

    /// Block cache entry data
    pub data: BlockCacheEntryData,

    /// Ids of 1 (or 2 in case of merge) previous blocks in shard or master chain
    pub prev_blocks_ids: Vec<BlockId>,
    /// List of shard block ids included in current block.
    /// `updated` indicates if `block_id` was updated since previous block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without master.
    pub top_shard_blocks_info: Vec<TopBlockIdUpdated>,
    /// For master block will contain top processed to anchor among master and all shards.
    pub top_processed_to_anchor: Option<MempoolAnchorId>,

    /// Seqno of master block that includes current shard block in his subgraph
    pub ref_by_mc_seqno: u32,
}

impl BlockCacheEntry {
    pub fn from_collated(
        candidate: Box<BlockCandidate>,
        top_shard_blocks_info: Vec<TopBlockIdUpdated>,
        top_processed_to_anchor: Option<MempoolAnchorId>,
    ) -> Result<Self> {
        let block_id = *candidate.block.id();
        let prev_blocks_ids = candidate.prev_blocks_ids.clone();
        let ref_by_mc_seqno = candidate.ref_by_mc_seqno;
        let entry = BlockCandidateStuff {
            candidate: *candidate,
            signatures: Default::default(),
            total_signature_weight: 0,
        };

        Ok(Self {
            block_id,
            data: BlockCacheEntryData::Collated {
                candidate_stuff: entry,
                status: CandidateStatus::Collated,
                received_after_collation: false,
            },
            prev_blocks_ids,
            top_shard_blocks_info,
            top_processed_to_anchor,
            ref_by_mc_seqno,
        })
    }

    pub fn from_received(
        state: ShardStateStuff,
        prev_blocks_ids: Vec<BlockId>,
        queue_diff: QueueDiffStuff,
        out_msgs: Lazy<OutMsgDescr>,
        ref_by_mc_seqno: u32,
        processed_upto: ProcessedUptoInfoStuff,
    ) -> Result<Self> {
        let block_id = *state.block_id();

        let mut top_shard_blocks_info = vec![];
        let mut top_processed_to_anchor = None;
        let cached_state = if block_id.is_masterchain() {
            for item in state.shards()?.iter() {
                let (shard_id, shard_descr) = item?;
                top_shard_blocks_info.push(TopBlockIdUpdated {
                    block: TopBlockId {
                        ref_by_mc_seqno: shard_descr.reg_mc_seqno,
                        block_id: shard_descr.get_block_id(shard_id),
                    },
                    updated: shard_descr.top_sc_block_updated,
                });
            }
            top_processed_to_anchor = Some(detect_top_processed_to_anchor(
                state.shards()?.as_vec()?.iter().map(|(_, d)| *d),
                state
                    .state()
                    .processed_upto
                    .load()?
                    .get_min_externals_processed_to()?
                    .0,
            ));
            Some(state)
        } else {
            None
        };

        Ok(Self {
            block_id,
            data: BlockCacheEntryData::Received {
                cached_state,
                queue_diff,
                out_msgs,
                collated_after_receive: false,
                additional_shard_block_cache_info: None,
                processed_upto,
            },
            prev_blocks_ids,
            top_shard_blocks_info,
            top_processed_to_anchor,
            ref_by_mc_seqno,
        })
    }

    pub fn key(&self) -> BlockCacheKey {
        self.block_id.as_short_id()
    }

    pub fn iter_top_shard_blocks_ids(&self) -> impl Iterator<Item = &BlockId> {
        self.top_shard_blocks_info
            .iter()
            .map(|item| &item.block.block_id)
    }

    pub fn get_top_blocks_keys(&self) -> Result<Vec<BlockCacheKey>> {
        if !self.block_id.is_masterchain() {
            bail!(
                "get_top_blocks_keys can be called only for master block, current block_id is {}",
                self.block_id,
            )
        }

        let mut top_blocks_keys = vec![self.key()];
        top_blocks_keys.extend(self.iter_top_shard_blocks_ids().map(|id| id.as_short_id()));

        Ok(top_blocks_keys)
    }

    pub fn cached_state(&self) -> Result<&ShardStateStuff> {
        if let BlockCacheEntryData::Received { cached_state, .. } = &self.data {
            cached_state.as_ref().ok_or_else(|| {
                anyhow!(
                    "`cached_state` shoul not be None for block ({})",
                    self.block_id
                )
            })
        } else {
            anyhow::bail!(
                "Block should be `Received` to contain `cached_state` ({})",
                self.block_id
            )
        }
    }
}

pub(super) struct McBlockSubgraph {
    pub master_block: BlockCacheEntry,
    pub shard_blocks: Vec<BlockCacheEntry>,
}

impl McBlockSubgraph {
    pub(crate) fn get_partitions(&self) -> FastHashSet<QueuePartitionIdx> {
        let mut partitions = FastHashSet::default();

        for block in self
            .shard_blocks
            .iter()
            .chain(std::iter::once(&self.master_block))
        {
            for partition in block.data.processed_upto().partitions.keys() {
                partitions.insert(*partition);
            }
        }
        partitions
    }
}

#[expect(clippy::large_enum_variant)]
pub(super) enum McBlockSubgraphExtract {
    Extracted(McBlockSubgraph),
    AlreadyExtracted,
}

impl Display for McBlockSubgraphExtract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Extracted(_) => write!(f, "Extracted"),
            Self::AlreadyExtracted => write!(f, "AlreadyExtracted"),
        }
    }
}

pub struct HandledBlockFromBcCtx {
    pub mc_block_id: BlockId,
    pub state: ShardStateStuff,
    pub processed_upto: ProcessedUptoInfo,
}
