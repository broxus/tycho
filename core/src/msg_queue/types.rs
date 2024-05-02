pub type Lt = u64;
pub type MessageHash = UInt256;

pub type SeqNo = u32;

pub enum BlockIdent {
    Id(BlockId),
    ShardAndSeqNo(ShardIdent, SeqNo),
}

#[derive(PartialEq, Eq, Clone)]
pub struct QueueDiffKey {}

pub struct QueueDiff {
    key: QueueDiffKey,
}
impl QueueDiff {
    fn key(&self) -> &QueueDiffKey {
        &self.key
    }
}

pub struct PersistentStateData {}

// Actually, we may already have [MsgEnvelope] and [EnqueuedMsg] types.
// They little bit different from current declarations. Possibly we'll use existing,
// but own types may be more efficient
pub struct MessageEnvelope {
    message: Message,
    from_contract: Address,
    to_contract: Address,
}
pub struct EnqueuedMessage {
    created_lt: Lt,
    enqueued_lt: Lt,
    hash: MessageHash,
    env: MessageEnvelope,
}

// STUBS FOR EXTERNAL TYPES
// further we should use types crate

pub(super) mod ext_types_stubs {
    pub struct Message {}
    pub type Address = String;
    pub type ShardIdent = i64;
    pub type UInt256 = String;
    pub type BlockId = UInt256;

    pub struct BlockIdExt {
        pub shard_id: ShardIdent,
        pub seq_no: u32,
        pub root_hash: UInt256,
        pub file_hash: UInt256,
    }
}
use ext_types_stubs::*;
