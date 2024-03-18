use everscale_crypto::ed25519::KeyPair;
use everscale_types::{
    cell::HashBytes,
    models::{BlockId, ShardIdent, ShardStateUnsplit, Signature},
};

use tycho_block_util::block::{BlockStuff, ValidatorSubsetInfo};

pub struct CollationConfig {
    pub key_pair: KeyPair,
    pub mc_block_min_interval_ms: u64,
}

pub(crate) struct BlockCollationResult {
    pub candidate: BlockCandidate,
    pub new_state: ShardStateUnsplit,
}

#[derive(Clone)]
pub(crate) struct BlockCandidate {
    block_id: BlockId,
    prev_blocks_ids: Vec<BlockId>,
    data: Vec<u8>,
    collated_data: Vec<u8>,
    collated_file_hash: HashBytes,
}
impl BlockCandidate {
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
    pub fn shard_id(&self) -> &ShardIdent {
        &self.block_id.shard
    }
    pub fn chain_time(&self) -> u64 {
        todo!()
    }
}

pub(crate) struct BlockSignatures {
    good_sigs: Vec<(HashBytes, Signature)>,
    bad_sigs: Vec<(HashBytes, Signature)>,
}
impl BlockSignatures {
    pub fn is_valid(&self) -> bool {
        todo!()
    }
}

pub(crate) struct ValidatedBlock {
    block_id: BlockId,
    signatures: BlockSignatures,
}
impl ValidatedBlock {
    pub fn id(&self) -> &BlockId {
        &self.block_id
    }
    pub fn is_valid(&self) -> bool {
        self.signatures.is_valid()
    }
}

pub(crate) struct BlockStuffForSync {
    pub block_stuff: BlockStuff,
    pub signatures: BlockSignatures,
    pub prev_blocks_ids: Vec<BlockId>,
}

/// (ShardIdent, seqno)
pub(crate) type CollationSessionId = (ShardIdent, u32);

pub(crate) struct CollationSessionInfo {
    /// Sequence number of the collation session
    seqno: u32,
    collators: ValidatorSubsetInfo,
}
impl CollationSessionInfo {
    pub fn new(seqno: u32, collators: ValidatorSubsetInfo) -> Self {
        Self { seqno, collators }
    }
    pub fn seqno(&self) -> u32 {
        self.seqno
    }
    pub fn collators(&self) -> &ValidatorSubsetInfo {
        &self.collators
    }
}

pub(crate) mod ext_types {
    pub use stubs::*;
    pub mod stubs {
        pub struct BlockHandle;
    }
}
