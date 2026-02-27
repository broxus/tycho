use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::ShardIdent;
use tycho_util::FastHashMap;

use crate::types::ProcessedToByPartitions;
use crate::types::processed_upto::{Lt, find_min_processed_to_by_shards};

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

pub fn compute_cumulative_stats_ranges(
    current_shard: &ShardIdent,
    all_shards_processed_to_by_partitions: &FastHashMap<
        ShardIdent,
        (bool, ProcessedToByPartitions),
    >,
    prev_state_gen_lt: Lt,
    mc_state_gen_lt: Lt,
    mc_top_shards_end_lts: &FastHashMap<ShardIdent, Lt>,
) -> Vec<QueueShardBoundedRange> {
    let mut ranges = vec![];

    let from_ranges = find_min_processed_to_by_shards(all_shards_processed_to_by_partitions);

    for (shard_ident, from) in from_ranges {
        let to_lt = if shard_ident.is_masterchain() {
            mc_state_gen_lt
        } else if shard_ident == *current_shard {
            prev_state_gen_lt
        } else {
            *mc_top_shards_end_lts.get(&shard_ident).unwrap()
        };

        let to = Bound::Included(QueueKey::max_for_lt(to_lt));

        ranges.push(QueueShardBoundedRange {
            shard_ident,
            from: Bound::Excluded(from),
            to,
        });
    }

    ranges
}
