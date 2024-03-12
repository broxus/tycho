use std::sync::Arc;

use anyhow::Result;

use self::ext_types::{
    Block, BlockIdExt, BlockProof, BlockSignature, Cell, KeyId, McStateExtra, ShardIdent,
    ShardStateUnsplit, UInt256, ValidatorDescr, ValidatorSet,
};

pub struct CollationConfig {
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

pub struct ShardStateStuff {
    block_id: BlockIdExt,
    shard_state: Option<ShardStateUnsplit>,
    shard_state_extra: Option<McStateExtra>,
    root_cell: Cell,
}
impl ShardStateStuff {
    pub fn shard_id(&self) -> &ShardIdent {
        &self.block_id.shard_id
    }
    pub fn from_state(block_id: BlockIdExt, shard_state: ShardStateUnsplit) -> Result<Arc<Self>> {
        todo!()
    }
}

pub struct CollationSessionInfo {
    /// Sequence number of the collation session
    seq_no: u32,
    collators: CollatorSubset,
}
impl CollationSessionInfo {
    pub fn new(seq_no: u32, collators: CollatorSubset) -> Self {
        Self { seq_no, collators }
    }
    pub fn collators(&self) -> &CollatorSubset {
        &self.collators
    }
}

pub struct CollatorSubset {
    /// The list of collators in the subset
    subset: Vec<ValidatorDescr>,
    /// Hash from collators subset list
    hash_short: u32,
}
impl CollatorSubset {
    #[deprecated(note = "should replace stub")]
    pub fn create(full_set: ValidatorSet, shard_id: &ShardIdent, seq_no: u32) -> Self {
        CollatorSubset {
            subset: vec![],
            hash_short: 0,
        }
    }
    pub fn subset_iterator<'a>(&'a self) -> core::slice::Iter<'a, ValidatorDescr> {
        self.subset.iter()
    }
}

pub(crate) mod ext_types {
    pub use stubs::*;
    pub mod stubs {
        pub struct KeyId([u8; 32]);
        pub struct BlockSignature(pub Vec<u8>);
        #[derive(Clone)]
        pub struct UInt256([u8; 32]);

        #[derive(Clone, PartialEq, Eq, Hash)]
        pub struct ShardIdent;
        impl ShardIdent {
            pub const fn new_full(workchain: u32) -> Self {
                Self {}
            }
            pub const fn masterchain() -> Self {
                Self {}
            }
            pub fn is_masterchain(&self) -> bool {
                todo!()
            }
        }
        #[derive(Clone)]
        pub struct BlockHashId;
        pub struct Block;
        pub struct BlockProof;
        #[derive(Clone)]
        pub struct BlockIdExt {
            pub shard_id: ShardIdent,
            pub seq_no: u32,
            pub root_hash: UInt256,
            pub file_hash: UInt256,
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
