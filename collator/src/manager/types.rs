use std::sync::Arc;

use anyhow::Result;

use tycho_block_util::state::ShardStateStuff;

use crate::types::ext_types::{BlockHashId, BlockIdExt, ShardStateUnsplit};
use crate::types::{BlockCandidate, BlockSignatures};

pub struct BlockCandidateEntry {
    pub key: BlockHashId,
    pub candidate: BlockCandidate,
    pub signatures: BlockSignatures,
}

pub enum SendSyncStatus {
    NotReady,
    Ready,
    Sending,
    Sent,
    Synced,
}

pub struct BlockCandidateContainer {
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
    /// Hash ids of 1 or 2 (in case of merge) previous blcoks in the shard or master chain
    pub prev_blocks_keys: Vec<BlockHashId>,
    /// Hash ids of all top shard blocks of corresponding shard chains, included in current block.
    /// It must be filled for master block.
    /// It could be filled for shard blocks if shards can exchange shard blocks with each other without a master.
    pub top_shard_blocks_keys: Vec<BlockHashId>,
    /// Hash id of master block that includes current shard block in his subgraph
    pub containing_mc_block: Option<BlockHashId>,
}
impl BlockCandidateContainer {
    fn new(candidate: BlockCandidate) -> Self {
        todo!()
    }

    /// True when the candidate became valid due to the applied validation result.
    /// Updates by `set_validation_result()`
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    /// Add signatures to containing block candidate entry and update `is_valid` flag
    fn set_validation_result(&mut self, signatures: BlockSignatures) {
        if let Some(ref mut entry) = self.entry {
            entry.signatures = signatures;
            self.is_valid = entry.signatures.is_valid();
        }
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
    fn from_state(block_id: BlockIdExt, shard_state: ShardStateUnsplit) -> Result<Arc<Self>>;
}
impl ShardStateStuffExt for ShardStateStuff {
    fn from_state(block_id: BlockIdExt, shard_state: ShardStateUnsplit) -> Result<Arc<Self>> {
        todo!()
    }
}
