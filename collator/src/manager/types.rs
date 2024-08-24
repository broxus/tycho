use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, Lazy, OutMsgDescr, ShardIdent, ValueFlow};
use parking_lot::Mutex;
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_util::{FastDashMap, FastHashMap};

use crate::types::{ArcSignature, BlockCandidate, BlockStuffForSync, ProofFunds};

pub(super) type BlockCacheKey = BlockIdShort;
pub(super) type BlockSeqno = u32;

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
    pub creators: Vec<HashBytes>,
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

#[derive(Clone)]
pub(super) struct BlockCandidateStuff {
    pub candidate: Arc<BlockCandidate>,
    pub signatures: FastHashMap<PeerId, ArcSignature>,
}

impl BlockCandidateStuff {
    pub fn as_block_for_sync(&self) -> BlockStuffForSync {
        // TODO: Rework cloning here
        BlockStuffForSync {
            block_stuff_aug: self.candidate.block.clone(),
            queue_diff_aug: self.candidate.queue_diff_aug.clone(),
            signatures: self.signatures.clone(),
            prev_blocks_ids: self.candidate.prev_blocks_ids.clone(),
            top_shard_blocks_ids: self.candidate.top_shard_blocks_ids.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct AppliedBlockStuff {
    pub block_id: BlockId,
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

#[derive(Clone)]
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
    pub prev_blocks_keys: Vec<BlockCacheKey>,
    /// Ids of all top shard blocks of corresponding shard chains, included in current block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without master.
    pub top_shard_blocks_keys: Vec<BlockCacheKey>,
    /// Id of master block that includes current shard block in his subgraph
    pub containing_mc_block: Option<BlockCacheKey>,
}

impl BlockCacheEntry {
    pub fn from_candidate(candidate: Box<BlockCandidate>) -> Self {
        let block_id = *candidate.block.id();
        let key = block_id.as_short_id();
        let entry = BlockCandidateStuff {
            candidate: Arc::new(*candidate),
            signatures: Default::default(),
        };

        Self {
            key,
            block_id,
            kind: BlockCacheEntryKind::Collated,
            prev_blocks_keys: entry
                .candidate
                .prev_blocks_ids
                .iter()
                .map(|id| id.as_short_id())
                .collect(),
            top_shard_blocks_keys: entry
                .candidate
                .top_shard_blocks_ids
                .iter()
                .map(|id| id.as_short_id())
                .collect(),
            candidate_stuff: Some(entry),
            applied_block_stuff: None,
            is_valid: false,
            containing_mc_block: None,
            send_sync_status: SendSyncStatus::NotReady,
        }
    }

    pub fn from_block_from_bc(
        state: ShardStateStuff,
        prev_block_ids: &[BlockId],
        queue_diff_and_msgs: Option<(QueueDiffStuff, Lazy<OutMsgDescr>)>,
    ) -> Result<Self> {
        let block_id = *state.block_id();
        let key = block_id.as_short_id();

        let mut top_shard_blocks_ids = vec![];
        if block_id.is_masterchain() {
            for item in state.shards()?.latest_blocks() {
                top_shard_blocks_ids.push(item?);
            }
            // for item in state.shards()?.iter() {
            //     let (shard, shard_descr) = item?;
            //     if shard_descr.top_sc_block_updated {
            //         top_shard_blocks_ids.push(BlockId {
            //             shard,
            //             seqno: shard_descr.seqno,
            //             root_hash: shard_descr.root_hash,
            //             file_hash: shard_descr.file_hash,
            //         });
            //     }
            // }
        }

        let applied_block_stuff = AppliedBlockStuff {
            block_id,
            state: Some(state),
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
            prev_blocks_keys: prev_block_ids.iter().map(|id| id.as_short_id()).collect(),
            top_shard_blocks_keys: top_shard_blocks_ids
                .iter()
                .map(|id| id.as_short_id())
                .collect(),
            containing_mc_block: None,
        })
    }

    pub fn key(&self) -> &BlockIdShort {
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

    pub fn prev_blocks_keys(&self) -> &[BlockCacheKey] {
        &self.prev_blocks_keys
    }

    pub fn top_shard_blocks_keys(&self) -> &[BlockCacheKey] {
        &self.top_shard_blocks_keys
    }

    pub fn extract_entry_stuff_for_sync(&mut self) -> Result<BlockCandidateStuffToSend> {
        let send_sync_status = self.send_sync_status;
        match self.send_sync_status {
            SendSyncStatus::NotReady => {
                bail!(
                    "Block is not ready for sync: ({})",
                    self.block_id().as_short_id()
                );
            }
            SendSyncStatus::Ready => {
                self.send_sync_status = SendSyncStatus::Sending;
            }
            _ => {
                // do not update send_sync_status
            }
        }
        Ok(self.extract_entry_stuff_with_status(send_sync_status))
    }

    fn extract_entry_stuff_with_status(
        &mut self,
        send_sync_status: SendSyncStatus,
    ) -> BlockCandidateStuffToSend {
        BlockCandidateStuffToSend {
            key: self.key,
            kind: self.kind,
            candidate_stuff: self.candidate_stuff.take(),
            applied_block_stuff: self.applied_block_stuff.take(),
            send_sync_status,
        }
    }

    pub fn restore_entry_stuff(&mut self, entry_stuff: BlockCandidateStuffToSend) -> Result<()> {
        // if block was not sent or synced then return cache entry status to Ready
        let new_send_sync_status = match entry_stuff.send_sync_status {
            SendSyncStatus::NotReady => {
                bail!("incorrect send_sync_status on restore: NotReady")
            }
            SendSyncStatus::Sending => SendSyncStatus::Ready,
            val => val,
        };
        self.send_sync_status = new_send_sync_status;
        self.candidate_stuff = entry_stuff.candidate_stuff;
        self.applied_block_stuff = entry_stuff.applied_block_stuff;
        Ok(())
    }

    pub fn candidate_stuff(&self) -> Result<&BlockCandidateStuff> {
        self.candidate_stuff
            .as_ref()
            .ok_or_else(|| anyhow!("`candidate_stuff` was extracted"))
    }
}

pub(super) struct BlockCandidateStuffToSend {
    pub key: BlockCacheKey,
    pub kind: BlockCacheEntryKind,
    pub candidate_stuff: Option<BlockCandidateStuff>,
    pub applied_block_stuff: Option<AppliedBlockStuff>,
    pub send_sync_status: SendSyncStatus,
}

pub(super) struct McBlockSubgraphToSend {
    pub master_block: Option<BlockCandidateStuffToSend>,
    pub shard_blocks: Vec<BlockCandidateStuffToSend>,
}

pub(super) enum McBlockSubgraphExtract {
    Extracted(McBlockSubgraphToSend),
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
