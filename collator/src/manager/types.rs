use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::{Block, BlockId, BlockIdShort, ShardIdent, ValueFlow};
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

#[derive(Default)]
pub(super) struct BlocksCache {
    pub master: FastDashMap<BlockCacheKey, BlockCandidateContainer>,
    pub shards: FastDashMap<ShardIdent, ShardBlocksCache>,
}

#[derive(Default)]
pub(super) struct ShardBlocksCache {
    pub blocks: BTreeMap<BlockSeqno, BlockCandidateContainer>,
    pub value_flow: ValueFlow,
    pub proof_funds: ProofFunds,
    pub creators: Vec<HashBytes>,
}

pub struct BlockCandidateEntry {
    pub key: BlockCacheKey,
    pub candidate: Box<BlockCandidate>,
    pub signatures: FastHashMap<PeerId, ArcSignature>,
}

impl BlockCandidateEntry {
    pub fn as_block_for_sync(&self) -> BlockStuffForSync {
        // TODO: Rework cloning here
        BlockStuffForSync {
            block_stuff_aug: self.candidate.block.clone(),
            signatures: self.signatures.clone(),
            prev_blocks_ids: self.candidate.prev_blocks_ids.clone(),
            top_shard_blocks_ids: self.candidate.top_shard_blocks_ids.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum SendSyncStatus {
    NotReady,
    Ready,
    Sending,
    Sent,
    Synced,
}

pub struct BlockCandidateContainer {
    key: BlockCacheKey,
    block_id: BlockId,
    /// Current block candidate entry with signatures
    pub entry: Option<BlockCandidateEntry>,
    /// True when the candidate became valid due to the applied validation result.
    /// Updates by `set_validation_result()`
    pub is_valid: bool,
    /// * `NotReady` - is not ready to send to sync (no master block or it is not validated)
    /// * `Ready` - is ready to send to sync (containing master block validated and all including shard blocks too)
    /// * `Sending` - block candidate extracted for sending to sync
    /// * `Sent` - block cadidate is already sent to sync
    pub send_sync_status: SendSyncStatus,
    /// Hash ids of 1 or 2 (in case of merge) previous blocks in the shard or master chain
    pub prev_blocks_keys: Vec<BlockCacheKey>,
    /// Hash ids of all top shard blocks of corresponding shard chains, included in current block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without a master.
    pub top_shard_blocks_keys: Vec<BlockCacheKey>,
    /// Hash id of master block that includes current shard block in his subgraph
    pub containing_mc_block: Option<BlockCacheKey>,
}

impl BlockCandidateContainer {
    pub fn new(candidate: Box<BlockCandidate>) -> Self {
        let block_id = *candidate.block.id();
        let key = block_id.as_short_id();
        let entry = BlockCandidateEntry {
            key,
            candidate,
            signatures: Default::default(),
        };

        Self {
            key,
            block_id,
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
            entry: Some(entry),
            is_valid: false,
            containing_mc_block: None,
            send_sync_status: SendSyncStatus::NotReady,
        }
    }

    pub fn key(&self) -> &BlockCacheKey {
        &self.key
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    /// True when the candidate became valid due to the applied validation result.
    /// Updates by `set_validation_result()`
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    /// Add signatures to containing block candidate entry
    /// or mark that it was already synced
    /// and update `is_valid` flag
    pub fn set_validation_result(
        &mut self,
        is_valid: bool,
        already_synced: bool,
        signatures: FastHashMap<PeerId, ArcSignature>,
    ) {
        if let Some(ref mut entry) = self.entry {
            entry.signatures = signatures;
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

    pub fn extract_entry_for_sending(&mut self) -> Result<Option<BlockCandidateToSend>> {
        let entry_opt = match self.send_sync_status {
            SendSyncStatus::NotReady => {
                bail!(
                    "Block is not ready for sync: ({})",
                    self.block_id.as_short_id()
                );
            }
            SendSyncStatus::Ready => {
                let entry = std::mem::take(&mut self.entry).ok_or_else(|| {
                    anyhow!(
                        "Block ({}) entry already extracted from cache for sending to sync",
                        self.block_id.as_short_id(),
                    )
                })?;
                self.send_sync_status = SendSyncStatus::Sending;
                Some(entry)
            }
            SendSyncStatus::Sending => {
                // Already extracted and sending now
                return Ok(None);
            }
            SendSyncStatus::Sent | SendSyncStatus::Synced => None,
        };
        Ok(Some(BlockCandidateToSend {
            key: self.key,
            entry: entry_opt,
            send_sync_status: self.send_sync_status,
        }))
    }

    pub fn restore_entry(
        &mut self,
        entry_opt: Option<BlockCandidateEntry>,
        send_sync_status: SendSyncStatus,
    ) -> Result<()> {
        // if block was not sent or synced then return cache entry status to Ready
        let new_send_sync_status = if matches!(
            send_sync_status,
            SendSyncStatus::Sent | SendSyncStatus::Synced
        ) {
            send_sync_status
        } else {
            SendSyncStatus::Ready
        };
        if self.entry.is_some() {
            bail!(
                "Block ({}) entry was not extracted before. Unable to restore!",
                self.block_id.as_short_id(),
            )
        } else {
            self.entry = entry_opt;
            self.send_sync_status = new_send_sync_status;
        }
        Ok(())
    }

    pub fn get_block(&self) -> Result<&Block> {
        let entry = self
            .entry
            .as_ref()
            .ok_or_else(|| anyhow!("`entry` was extracted"))?;
        Ok(entry.candidate.block.as_ref())
    }

    pub fn create_synced_from_bc(block_id: BlockId) -> Self {
        Self {
            key: block_id.as_short_id(),
            block_id,
            is_valid: true,
            send_sync_status: SendSyncStatus::Synced,
            entry: None,
            containing_mc_block: None,
            prev_blocks_keys: vec![],
            top_shard_blocks_keys: vec![],
        }
    }
}

pub struct BlockCandidateToSend {
    pub key: BlockCacheKey,
    pub entry: Option<BlockCandidateEntry>,
    pub send_sync_status: SendSyncStatus,
}

pub(super) struct McBlockSubgraphToSend {
    pub mc_block: BlockCandidateToSend,
    pub shard_blocks: Vec<BlockCandidateToSend>,
}

pub(super) enum McBlockSubgraphExtract {
    Extracted(McBlockSubgraphToSend),
    NotFullValid,
    AlreadyExtracted,
}
