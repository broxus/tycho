use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use everscale_types::models::{BlockId, BlockIdShort, Lazy, OutMsgDescr, ShardIdent, ValueFlow};
use parking_lot::Mutex;
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_util::{FastDashMap, FastHashMap};

use crate::types::{
    ArcSignature, BlockCandidate, BlockStuffForSync, McData, ProofFunds, ShardDescriptionExt,
};

pub(super) type BlockCacheKey = BlockIdShort;
pub(super) type BlockSeqno = u32;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum CollatorState {
    Active,
    Cancelled,
}

pub(super) struct ActiveCollator<C> {
    pub collator: C,
    pub state: CollatorState,
}

#[derive(Default)]
pub(super) struct ChainTimesSyncState {
    /// latest known chain time for master block: last imported or next to be collated
    pub mc_block_latest_chain_time: u64,
    pub last_collated_chain_times_by_shards: FastHashMap<ShardIdent, Vec<(u64, bool)>>,
}

#[derive(Debug)]
pub(super) struct BlockCacheStoreResult {
    pub block_id: BlockId,
    pub kind: BlockCacheEntryKind,
    pub send_sync_status: SendSyncStatus,
    pub last_collated_mc_block_id: Option<BlockId>,
    pub applied_mc_queue_range: Option<(BlockSeqno, BlockSeqno)>,
}

pub(super) struct DisplayBlockCacheStoreResult<'a>(pub &'a BlockCacheStoreResult);
impl std::fmt::Debug for DisplayBlockCacheStoreResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayBlockCacheStoreResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "block_id={}, kind={:?}, last_collated_mc_block_id={:?}, send_sync_status={:?}, ",
            self.0.block_id,
            self.0.kind,
            self.0.last_collated_mc_block_id.map(|id| id.to_string()),
            self.0.send_sync_status,
        )?;
        write!(
            f,
            "applied_mc_queue_range={:?}",
            self.0.applied_mc_queue_range,
        )
    }
}

#[derive(Default)]
pub(super) struct BlocksCache {
    pub masters: Mutex<MasterBlocksCache>,
    pub shards: FastDashMap<ShardIdent, ShardBlocksCache>,
}

#[derive(Default)]
pub(super) struct MasterBlocksCache {
    pub blocks: BTreeMap<BlockSeqno, BlockCacheEntry>,
    pub last_known_synced: Option<BlockSeqno>,
    /// id of last master block collated by ourselves
    pub last_collated_mc_block_id: Option<BlockId>,
    pub applied_mc_queue_range: Option<(BlockSeqno, BlockSeqno)>,
}

#[derive(Default)]
pub(super) struct ShardBlocksCache {
    pub blocks: BTreeMap<BlockSeqno, BlockCacheEntry>,
    pub last_known_synced: Option<BlockSeqno>,
    pub value_flow: ValueFlow,
    pub proof_funds: ProofFunds,
    #[cfg(feature = "block-creator-stats")]
    pub creators: Vec<everscale_types::cell::HashBytes>,
}

/// Returns Some(seqno: u32) of last known synced block when it newer than provided
pub(super) fn check_refresh_last_known_synced(
    last_known_seqno: &mut Option<BlockSeqno>,
    block_seqno: BlockSeqno,
) -> Option<BlockSeqno> {
    if let Some(last_known) = last_known_seqno {
        if *last_known >= block_seqno {
            return Some(*last_known);
        }
    }
    *last_known_seqno = Some(block_seqno);
    None
}

pub(super) struct BlockCandidateStuff {
    pub candidate: BlockCandidate,
    pub signatures: FastHashMap<PeerId, ArcSignature>,
}

impl From<BlockCandidateStuff> for BlockStuffForSync {
    fn from(stuff: BlockCandidateStuff) -> Self {
        let BlockCandidateStuff {
            candidate,
            signatures,
        } = stuff;

        let BlockCandidate {
            block: block_stuff_aug,
            prev_blocks_ids,
            top_shard_blocks_ids,
            queue_diff_aug,
            ..
        } = candidate;

        Self {
            block_stuff_aug,
            queue_diff_aug,
            signatures,
            prev_blocks_ids,
            top_shard_blocks_ids,
        }
    }
}

#[derive(Clone)]
pub(super) struct AppliedBlockStuff {
    pub state: Option<ShardStateStuff>,
    pub queue_diff_and_msgs: Option<(QueueDiffStuff, Lazy<OutMsgDescr>)>,
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub(super) enum SendSyncStatus {
    NotReady,
    Ready,
    Sending,
    Sent,
    Synced,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(super) enum BlockCacheEntryKind {
    Collated,
    CollatedAndReceived,
    Received,
    ReceivedAndCollated,
}

pub(super) struct BlockCacheEntry {
    key: BlockCacheKey,
    block_id: BlockId,

    pub kind: BlockCacheEntryKind,

    /// Collated block candidate with signatures
    pub candidate_stuff: Option<BlockCandidateStuff>,
    /// Appliend block data received from bc
    pub applied_block_stuff: Option<AppliedBlockStuff>,

    /// True when the candidate became valid due to the applied validation result
    /// (updated by `set_validation_result()`). Or when received applied block from bc.
    pub is_valid: bool,

    /// * `NotReady` - is not ready to sync (not master block or it is not validated)
    /// * `Ready` - is ready to sync (master block valid and all including shard blocks too)
    /// * `Sending` - block extracted for sending to sync
    /// * `Sent` - block is already sent to sync
    /// * `Synced` - block is already synced
    pub send_sync_status: SendSyncStatus,

    /// Ids of 1 (or 2 in case of merge) previous blocks in shard or master chain
    pub prev_blocks_ids: Vec<BlockId>,
    /// List of (`top_block_id`, `to_block_updated`) included in current block.
    /// `to_block_updated` indicates if `top_block_id` was updated since previous block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without master.
    pub top_shard_blocks_info: Vec<(BlockId, bool)>,

    /// Id of master block that includes current shard block in his subgraph
    pub containing_mc_block: Option<BlockCacheKey>,
}

impl BlockCacheEntry {
    pub fn from_candidate(
        candidate: Box<BlockCandidate>,
        mc_data: Option<Arc<McData>>,
    ) -> Result<Self> {
        let block_id = *candidate.block.id();
        let key = block_id.as_short_id();
        let entry = BlockCandidateStuff {
            candidate: *candidate,
            signatures: Default::default(),
        };

        let mut top_shard_blocks_info = vec![];
        if let Some(mc_data) = mc_data {
            for item in mc_data.shards.iter() {
                let (shard_id, shard_descr) = item?;
                top_shard_blocks_info.push((
                    shard_descr.get_block_id(shard_id),
                    shard_descr.top_sc_block_updated,
                ));
            }
        }

        Ok(Self {
            key,
            block_id,
            kind: BlockCacheEntryKind::Collated,
            prev_blocks_ids: entry.candidate.prev_blocks_ids.clone(),
            top_shard_blocks_info,
            candidate_stuff: Some(entry),
            applied_block_stuff: None,
            is_valid: false,
            containing_mc_block: None,
            send_sync_status: SendSyncStatus::NotReady,
        })
    }

    pub fn from_block_from_bc(
        state: &ShardStateStuff,
        prev_blocks_ids: Vec<BlockId>,
        queue_diff_and_msgs: Option<(QueueDiffStuff, Lazy<OutMsgDescr>)>,
    ) -> Result<Self> {
        let block_id = *state.block_id();
        let key = block_id.as_short_id();

        let mut top_shard_blocks_info = vec![];
        if block_id.is_masterchain() {
            for item in state.shards()?.iter() {
                let (shard_id, shard_descr) = item?;
                top_shard_blocks_info.push((
                    shard_descr.get_block_id(shard_id),
                    shard_descr.top_sc_block_updated,
                ));
            }
        }

        let applied_block_stuff = AppliedBlockStuff {
            state: Some(state.clone()),
            queue_diff_and_msgs,
        };

        Ok(Self {
            key,
            block_id,
            kind: BlockCacheEntryKind::Received,
            candidate_stuff: None,
            applied_block_stuff: Some(applied_block_stuff),
            is_valid: true,
            send_sync_status: SendSyncStatus::Synced,
            prev_blocks_ids,
            top_shard_blocks_info,
            containing_mc_block: None,
        })
    }

    pub fn key(&self) -> &BlockCacheKey {
        &self.key
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    /// Add signatures to block candidate
    /// or mark that it was already synced
    /// and update `is_valid` flag
    pub fn set_validation_result(
        &mut self,
        is_valid: bool,
        already_synced: bool,
        signatures: FastHashMap<PeerId, ArcSignature>,
    ) {
        if let Some(ref mut candidate_stuff) = self.candidate_stuff {
            candidate_stuff.signatures = signatures;
            self.is_valid = is_valid;
            if self.is_valid {
                if already_synced {
                    // already synced block is valid and won't be sent to sync again
                    self.send_sync_status = SendSyncStatus::Synced;
                } else {
                    // block is ready for sync when validated
                    self.send_sync_status = SendSyncStatus::Ready;
                }
            }
        }
    }

    pub fn top_shard_blocks_ids_iter(&self) -> impl Iterator<Item = &BlockId> {
        self.top_shard_blocks_info.iter().map(|(id, _)| id)
    }

    pub fn candidate_stuff(&self) -> Result<&BlockCandidateStuff> {
        self.candidate_stuff
            .as_ref()
            .ok_or_else(|| anyhow!("`candidate_stuff` was extracted"))
    }
}

impl AppliedBlockStuffContainer for BlockCacheEntry {
    fn key(&self) -> &BlockCacheKey {
        &self.key
    }

    fn applied_block_stuff(&self) -> Option<&AppliedBlockStuff> {
        self.applied_block_stuff.as_ref()
    }
}

pub(super) trait AppliedBlockStuffContainer {
    fn key(&self) -> &BlockCacheKey;

    fn applied_block_stuff(&self) -> Option<&AppliedBlockStuff>;

    fn state(&self) -> Result<&ShardStateStuff> {
        let Some(applied_block_stuff) = self.applied_block_stuff() else {
            bail!(
                "applied_block_stuff should not be None for block ({})",
                self.key(),
            )
        };
        let Some(state) = applied_block_stuff.state.as_ref() else {
            bail!("state should not be None for block ({})", self.key(),)
        };
        Ok(state)
    }

    fn queue_diff_and_msgs(&self) -> Result<&(QueueDiffStuff, Lazy<OutMsgDescr>)> {
        let Some(applied_block_stuff) = self.applied_block_stuff() else {
            bail!(
                "applied_block_stuff should not be None for block ({})",
                self.key(),
            )
        };
        let Some(result) = applied_block_stuff.queue_diff_and_msgs.as_ref() else {
            bail!(
                "queue_diff_and_msgs should not be None for block ({})",
                self.key(),
            )
        };
        Ok(result)
    }
}

pub(super) struct McBlockSubgraph {
    pub master_block: BlockCacheEntry,
    pub shard_blocks: Vec<BlockCacheEntry>,
}

pub(super) enum McBlockSubgraphExtract {
    Extracted(McBlockSubgraph),
    NotFullValid,
    AlreadyExtracted,
}

impl std::fmt::Display for McBlockSubgraphExtract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Extracted(_) => write!(f, "Extracted"),
            Self::NotFullValid => write!(f, "NotFullValid"),
            Self::AlreadyExtracted => write!(f, "AlreadyExtracted"),
        }
    }
}
