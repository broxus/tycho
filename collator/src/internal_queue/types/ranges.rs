use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::ShardIdent;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Bound<T> {
    Included(T),
    Excluded(T),
}

#[derive(Debug, Clone)]
pub struct QueueShardRange {
    pub shard_ident: ShardIdent,
    pub from: QueueKey,
    pub to: QueueKey,
}

#[derive(Debug, Clone)]
pub struct QueueShardBoundedRange {
    pub shard_ident: ShardIdent,
    pub from: Bound<QueueKey>,
    pub to: Bound<QueueKey>,
}

impl From<QueueShardBoundedRange> for QueueShardRange {
    fn from(value: QueueShardBoundedRange) -> Self {
        let from = match value.from {
            Bound::Included(value) => value,
            Bound::Excluded(value) => value.next_value(),
        };

        let to = match value.to {
            Bound::Included(value) => value.next_value(),
            Bound::Excluded(value) => value,
        };

        QueueShardRange {
            shard_ident: value.shard_ident,
            from,
            to,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueueRange {
    pub partition: QueuePartitionIdx,
    pub shard_ident: ShardIdent,
    pub from: QueueKey,
    pub to: QueueKey,
}

#[derive(Debug, Clone)]
pub struct CommitPointer {
    pub shard_ident: ShardIdent,
    pub queue_key: QueueKey,
}

pub struct PartitionQueueKey {
    pub partition: QueuePartitionIdx,
    pub key: QueueKey,
}
