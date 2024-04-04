use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::Result;
use everscale_types::cell::HashBytes;

use everscale_types::{
    cell::{Cell, CellBuilder, CellFamily, Store},
    models::{BlockId, BlockIdShort, ShardIdent, ShardStateUnsplit, Signature},
};

use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};

use crate::types::BlockCandidate;

pub(super) type BlockCacheKey = BlockIdShort;
pub(super) type BlockSeqno = u32;
#[derive(Default)]
pub(super) struct BlocksCache {
    pub master: BTreeMap<BlockCacheKey, BlockCandidateContainer>,
    pub shards: HashMap<ShardIdent, BTreeMap<BlockSeqno, BlockCandidateContainer>>,
}

pub struct BlockCandidateEntry {
    pub key: BlockCacheKey,
    pub candidate: BlockCandidate,
    pub signatures: HashMap<HashBytes, Signature>,
}

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
    entry: Option<BlockCandidateEntry>,
    /// True when the candidate became valid due to the applied validation result.
    /// Updates by `set_validation_result()`
    is_valid: bool,
    /// * NotReady - is not ready to send to sync (no master block or it is not validated)
    /// * Ready - is ready to send to sync (containing master block validated and all including shard blocks too)
    /// * Sending - block candidate extracted for sending to sync
    /// * Sent - block cadidate is already sent to sync
    pub send_sync_status: SendSyncStatus,
    /// Hash ids of 1 or 2 (in case of merge) previous blocks in the shard or master chain
    prev_blocks_keys: Vec<BlockCacheKey>,
    /// Hash ids of all top shard blocks of corresponding shard chains, included in current block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without a master.
    top_shard_blocks_keys: Vec<BlockCacheKey>,
    /// Hash id of master block that includes current shard block in his subgraph
    pub containing_mc_block: Option<BlockCacheKey>,
}
impl BlockCandidateContainer {
    pub fn new(candidate: BlockCandidate) -> Self {
        let block_id = *candidate.block_id();
        let key = candidate.block_id().as_short_id();
        let entry = BlockCandidateEntry {
            key,
            candidate,
            signatures: HashMap::default(),
        };
        Self {
            key,
            block_id,
            prev_blocks_keys: entry
                .candidate
                .prev_blocks_ids()
                .iter()
                .map(|id| id.as_short_id())
                .collect(),
            top_shard_blocks_keys: entry
                .candidate
                .top_shard_blocks_ids()
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

    /// Add signatures to containing block candidate entry and update `is_valid` flag
    pub fn set_validation_result(
        &mut self,
        is_valid: bool,
        signatures: HashMap<HashBytes, Signature>,
    ) {
        if let Some(ref mut entry) = self.entry {
            entry.signatures = signatures;
            self.is_valid = is_valid;
        }
    }

    pub fn prev_blocks_keys(&self) -> &[BlockCacheKey] {
        &self.prev_blocks_keys
    }

    pub fn top_shard_blocks_keys(&self) -> &[BlockCacheKey] {
        &self.top_shard_blocks_keys
    }
}

pub struct BlockCandidateToSend {
    pub entry: BlockCandidateEntry,
    pub send_sync_status: SendSyncStatus,
}

pub struct McBlockSubgraphToSend {
    pub mc_block: BlockCandidateToSend,
    pub shard_blocks: Vec<BlockCandidateToSend>,
}

pub(in crate::manager) trait ShardStateStuffExt {
    fn from_state(
        block_id: BlockId,
        shard_state: ShardStateUnsplit,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Arc<Self>>;
}
impl ShardStateStuffExt for ShardStateStuff {
    fn from_state(
        block_id: BlockId,
        shard_state: ShardStateUnsplit,
        tracker: &MinRefMcStateTracker,
    ) -> Result<Arc<Self>> {
        let mut builder = CellBuilder::new();
        let mut cell_context = Cell::empty_context();
        shard_state.store_into(&mut builder, &mut cell_context)?;
        let root = builder.build_ext(&mut cell_context)?;

        Ok(Arc::new(ShardStateStuff::from_state_and_root(
            block_id,
            shard_state,
            root,
            tracker,
        )?))
    }
}
