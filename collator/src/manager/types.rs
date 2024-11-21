use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use everscale_types::models::{BlockId, BlockIdShort, BlockInfo, Lazy, OutMsgDescr, ShardIdent};
use tokio::sync::Notify;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::types::{
    ArcSignature, BlockCandidate, BlockStuffForSync, DebugDisplayOpt, McData, ShardDescriptionExt,
};

pub(super) type BlockCacheKey = BlockIdShort;
pub(super) type BlockSeqno = u32;

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
    pub states: FastHashMap<ShardIdent, CollationState>,
}

#[derive(Debug)]
pub(super) struct CollationState {
    pub status: CollationStatus,
    pub mc_collation_forced: bool,
    pub last_imported_chain_times: Vec<(u64, bool)>,
}

impl Default for CollationState {
    fn default() -> Self {
        Self {
            status: CollationStatus::AttemptsInProgress,
            mc_collation_forced: false,
            last_imported_chain_times: vec![],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CollationStatus {
    AttemptsInProgress,
    WaitForMasterStatus,
}

#[derive(Debug)]
pub(super) enum NextCollationStep {
    WaitForMasterStatus,
    ResumeAttemptsIn(Vec<ShardIdent>),
    CollateMaster(u64),
}

pub(super) struct BlockCacheStoreResult {
    pub received_and_collated: bool,
    pub block_mismatch: bool,
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

impl From<BlockCandidateStuff> for BlockStuffForSync {
    fn from(stuff: BlockCandidateStuff) -> Self {
        let BlockCandidateStuff {
            candidate,
            signatures,
            total_signature_weight,
        } = stuff;

        let BlockCandidate {
            ref_by_mc_seqno,
            block: block_stuff_aug,
            prev_blocks_ids,
            top_shard_blocks_ids,
            queue_diff_aug,
            consensus_info,
            ..
        } = candidate;

        Self {
            ref_by_mc_seqno,
            block_stuff_aug,
            queue_diff_aug,
            signatures,
            total_signature_weight,
            prev_blocks_ids,
            top_shard_blocks_ids,
            consensus_info,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub(super) enum CandidateStatus {
    Collated,
    Validated,
    Synced,
}

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
    /// List of (`top_block_id`, `to_block_updated`) included in current block.
    /// `to_block_updated` indicates if `top_block_id` was updated since previous block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without master.
    pub top_shard_blocks_info: Vec<(BlockId, bool)>,

    /// Seqno of master block that includes current shard block in his subgraph
    pub ref_by_mc_seqno: u32,
}

impl BlockCacheEntry {
    pub fn from_collated(
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<Self> {
        let block_id = *candidate.block.id();
        let prev_blocks_ids = candidate.prev_blocks_ids.clone();
        let ref_by_mc_seqno = candidate.ref_by_mc_seqno;
        let entry = BlockCandidateStuff {
            candidate: *candidate,
            signatures: Default::default(),
            total_signature_weight: 0,
        };

        let mut top_shard_blocks_info = vec![];
        if let Some(mc_data) = mc_data {
            for (shard_id, shard_descr) in mc_data.shards.iter() {
                top_shard_blocks_info.push((
                    shard_descr.get_block_id(*shard_id),
                    shard_descr.top_sc_block_updated,
                ));
            }
        }

        Ok(Self {
            block_id,
            data: BlockCacheEntryData::Collated {
                candidate_stuff: entry,
                status: CandidateStatus::Collated,
                received_after_collation: false,
            },
            prev_blocks_ids,
            top_shard_blocks_info,
            ref_by_mc_seqno,
        })
    }

    pub fn from_received(
        state: ShardStateStuff,
        prev_blocks_ids: Vec<BlockId>,
        queue_diff: QueueDiffStuff,
        out_msgs: Lazy<OutMsgDescr>,
        ref_by_mc_seqno: u32,
    ) -> Result<Self> {
        let block_id = *state.block_id();

        let mut top_shard_blocks_info = vec![];
        let cached_state = if block_id.is_masterchain() {
            for item in state.shards()?.iter() {
                let (shard_id, shard_descr) = item?;
                top_shard_blocks_info.push((
                    shard_descr.get_block_id(shard_id),
                    shard_descr.top_sc_block_updated,
                ));
            }
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
            },
            prev_blocks_ids,
            top_shard_blocks_info,
            ref_by_mc_seqno,
        })
    }

    pub fn key(&self) -> BlockCacheKey {
        self.block_id.as_short_id()
    }

    pub fn iter_top_block_ids(&self) -> impl Iterator<Item = &BlockId> {
        self.top_shard_blocks_info.iter().map(|(id, _)| id)
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

    pub fn int_processed_to(&self) -> &BTreeMap<ShardIdent, QueueKey> {
        match &self.data {
            BlockCacheEntryData::Collated {
                candidate_stuff, ..
            } => {
                &candidate_stuff
                    .candidate
                    .queue_diff_aug
                    .diff()
                    .processed_upto
            }
            BlockCacheEntryData::Received { queue_diff, .. } => &queue_diff.diff().processed_upto,
        }
    }
}

pub(super) struct McBlockSubgraph {
    pub master_block: BlockCacheEntry,
    pub shard_blocks: Vec<BlockCacheEntry>,
}

pub(super) enum McBlockSubgraphExtract {
    Extracted(McBlockSubgraph),
    AlreadyExtracted,
}

impl std::fmt::Display for McBlockSubgraphExtract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Extracted(_) => write!(f, "Extracted"),
            Self::AlreadyExtracted => write!(f, "AlreadyExtracted"),
        }
    }
}
