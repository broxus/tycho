use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::ShardIdent;

use tycho_block_util::block::ValidatorSubsetInfo;

use self::ext_types::{
    Block, BlockIdExt, BlockProof, BlockSignature, KeyId, ShardStateUnsplit, UInt256,
};

pub struct CollationConfig {
    pub key_pair: KeyPair,
    pub mc_block_min_interval_ms: u64,
}

pub struct BlockCollationResult {
    pub candidate: BlockCandidate,
    pub new_state: ShardStateUnsplit,
}

#[derive(Clone)]
pub(crate) struct BlockCandidate {
    block_id: BlockIdExt,
    prev_blocks_ids: Vec<BlockIdExt>,
    data: Vec<u8>,
    collated_data: Vec<u8>,
    collated_file_hash: UInt256,
}
impl BlockCandidate {
    pub fn block_id(&self) -> &BlockIdExt {
        &self.block_id
    }
    pub fn shard_id(&self) -> &ShardIdent {
        &self.block_id.shard_id
    }
    pub fn own_signature(&self) -> BlockSignature {
        todo!()
    }
    pub fn chain_time(&self) -> u64 {
        todo!()
    }
}

pub struct BlockSignatures {
    good_sigs: Vec<(KeyId, BlockSignature)>,
    bad_sigs: Vec<(KeyId, BlockSignature)>,
}
impl BlockSignatures {
    pub fn is_valid(&self) -> bool {
        todo!()
    }
}

pub struct ValidatedBlock {
    block_id: BlockIdExt,
    signatures: BlockSignatures,
}
impl ValidatedBlock {
    pub fn id(&self) -> &BlockIdExt {
        &self.block_id
    }
    pub fn is_valid(&self) -> bool {
        self.signatures.is_valid()
    }
}

pub struct BlockStuff {
    id: BlockIdExt,
    block: Option<Block>,
    // other stuff...
}

pub struct BlockProofStuff {
    id: BlockIdExt,
    proof: BlockProof,
    // other stuff...
}

pub struct BlockStuffForSync {
    pub block_stuff: BlockStuff,
    pub signatures: BlockSignatures,
    pub prev_blocks_ids: Vec<BlockIdExt>,
}

/// (ShardIdent, seqno)
pub type CollationSessionId = (ShardIdent, u32);

pub struct CollationSessionInfo {
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
        use everscale_types::{cell::HashBytes, models::ShardIdent};

        pub struct KeyId([u8; 32]);
        pub struct BlockSignature(pub Vec<u8>);
        #[derive(Clone)]
        pub struct UInt256([u8; 32]);
        #[derive(Clone)]
        pub struct BlockHashId;
        pub struct Block;
        pub struct BlockProof;
        #[derive(Clone)]
        pub struct BlockIdExt {
            pub shard_id: ShardIdent,
            pub seq_no: u32,
            pub root_hash: HashBytes,
            pub file_hash: HashBytes,
        }
        impl BlockIdExt {
            pub const fn with_params(
                shard_id: ShardIdent,
                seq_no: u32,
                root_hash: HashBytes,
                file_hash: HashBytes,
            ) -> Self {
                BlockIdExt {
                    shard_id,
                    seq_no,
                    root_hash,
                    file_hash,
                }
            }
        }
        pub struct ShardAccounts;
        pub struct Cell;
        pub struct CurrencyCollection;
        pub struct ShardStateUnsplit;
        pub struct McStateExtra;
        pub struct BlockHandle;

        pub struct ValidatorId;
        pub struct ValidatorDescr;
        impl ValidatorDescr {
            pub fn id(&self) -> ValidatorId {
                todo!()
            }
        }
        pub struct ValidatorSet;
    }
}
