use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::*;
use tycho_util::FastHashMap;

use crate::collator::do_collate::calculate_min_internals_processed_to_for_shard;
use crate::types::ProcessedToByPartitions;

#[test]
fn test_calculate_min_processed_to_masterchain() {
    // Mock data for masterchain test
    let shard_id = ShardIdent::MASTERCHAIN;

    let current_processed_to = Some(QueueKey::max_for_lt(5));
    let mc_processed_to = Some(QueueKey::max_for_lt(5));

    let updated_shard = ShardIdent::new_full(0);
    let not_updated_shard = ShardIdent::new_full(1);

    let mut mc_data_shards_processed_to_by_partitions = FastHashMap::default();

    let mut processed_to_by_partitions = ProcessedToByPartitions::default();
    processed_to_by_partitions.insert(
        QueuePartitionIdx(0),
        [(shard_id, QueueKey::max_for_lt(4))].into_iter().collect(),
    );
    processed_to_by_partitions.insert(
        QueuePartitionIdx(1),
        [(shard_id, QueueKey::max_for_lt(3))].into_iter().collect(),
    );

    // check updated
    mc_data_shards_processed_to_by_partitions
        .insert(updated_shard, (true, processed_to_by_partitions.clone()));

    let result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards_processed_to_by_partitions,
    );

    // updated shard should override current_processed_to
    assert_eq!(result, Some(QueueKey::max_for_lt(3)));

    // check not updated.
    mc_data_shards_processed_to_by_partitions.clear();
    mc_data_shards_processed_to_by_partitions
        .insert(not_updated_shard, (false, processed_to_by_partitions));

    let result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards_processed_to_by_partitions,
    );

    // not updated shard should override current_processed_to too
    assert_eq!(result, Some(QueueKey::max_for_lt(3)));
}

#[test]
fn test_calculate_min_processed_to_shard() {
    // Mock data for shard test
    let shard_id = ShardIdent::new_full(2);

    let current_processed_to = Some(QueueKey::max_for_lt(11));

    let updated_shard = ShardIdent::new_full(3);
    let not_updated_shard = ShardIdent::new_full(4);

    let mut mc_data_shards_processed_to_by_partitions = FastHashMap::default();

    // Check updated shard
    let mut processed_to_by_partitions = ProcessedToByPartitions::default();
    processed_to_by_partitions.insert(
        QueuePartitionIdx(0),
        [(shard_id, QueueKey::max_for_lt(8))].into_iter().collect(),
    );
    processed_to_by_partitions.insert(
        QueuePartitionIdx(1),
        [(shard_id, QueueKey::max_for_lt(7))].into_iter().collect(),
    );

    mc_data_shards_processed_to_by_partitions
        .insert(updated_shard, (true, processed_to_by_partitions.clone()));

    let mc_processed_to = Some(QueueKey::max_for_lt(9));

    let result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards_processed_to_by_partitions,
    );

    // Updated shard should override current_processed_to
    assert_eq!(result, Some(QueueKey::max_for_lt(7)));

    // Check not updated shard
    mc_data_shards_processed_to_by_partitions.clear();
    mc_data_shards_processed_to_by_partitions
        .insert(not_updated_shard, (false, processed_to_by_partitions));

    let mc_processed_to = Some(QueueKey::max_for_lt(12));

    let result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards_processed_to_by_partitions,
    );

    // Not updated shard should override current_processed_to too
    assert_eq!(result, Some(QueueKey::max_for_lt(7)));

    // Verify combination with masterchain value
    let mut processed_to_by_partitions = ProcessedToByPartitions::default();
    processed_to_by_partitions.insert(
        QueuePartitionIdx(0),
        [(shard_id, QueueKey::max_for_lt(12))].into_iter().collect(),
    );
    processed_to_by_partitions.insert(
        QueuePartitionIdx(1),
        [(shard_id, QueueKey::max_for_lt(10))].into_iter().collect(),
    );

    mc_data_shards_processed_to_by_partitions.clear();
    mc_data_shards_processed_to_by_partitions
        .insert(updated_shard, (true, processed_to_by_partitions.clone()));

    let mc_processed_to = Some(QueueKey::max_for_lt(9));

    let result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards_processed_to_by_partitions,
    );

    // Minimum value from master should be returned
    assert_eq!(result, Some(QueueKey::max_for_lt(9)));
}
