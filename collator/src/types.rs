use self::ext_types::{
    BlockIdExt, BlockSignature, Cell, KeyId, McStateExtra, ShardIdent, ShardStateUnsplit,
    ValidatorDescr, ValidatorSet,
};

pub struct CollationConfig {}

#[derive(Clone)]
pub struct BlockCandidate {
    block_id: BlockIdExt,
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
}

pub struct ValidatedBlock {
    block: BlockCandidate,
    signatures: Vec<(KeyId, BlockSignature)>,
}
impl ValidatedBlock {
    pub fn is_valid(&self) -> bool {
        todo!()
    }
    pub fn is_master(&self) -> bool {
        self.block.block_id.shard_id.is_masterchain()
    }
}

pub struct BlockStuff {}

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
