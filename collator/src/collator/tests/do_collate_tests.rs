use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::models::*;
use tycho_util::FastHashMap;

use crate::types::ProcessedToByPartitions;

fn calculate_min_internals_processed_to_for_shard(
    shard_id: &ShardIdent,
    shard_min_processed_to: Option<QueueKey>,
    mc_data_min_processed_to: Option<QueueKey>,
    mc_data_shards_processed_to: &FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
) -> Option<QueueKey> {
    fn find_min_processed_to(
        mc_data_shards_processed_to: &FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
        shard_id: &ShardIdent,
        min_processed_to: &mut Option<QueueKey>,
        skip_condition: impl Fn(&ShardIdent) -> bool,
    ) {
        for (shard, (_, processed_to_by_partitions)) in mc_data_shards_processed_to {
            if skip_condition(shard) {
                continue;
            }

            for partition_processed_to in processed_to_by_partitions.values() {
                if let Some(to_key) = partition_processed_to.get(shard_id) {
                    *min_processed_to = match *min_processed_to {
                        Some(min) => Some(min.min(*to_key)),
                        None => Some(*to_key),
                    };
                }
            }
        }
    }

    let mut min_processed_to: Option<QueueKey> = None;

    if shard_id.is_masterchain() {
        find_min_processed_to(
            mc_data_shards_processed_to,
            shard_id,
            &mut min_processed_to,
            |_| false,
        );

        // Combine with current and masterchain values
        min_processed_to = [shard_min_processed_to, min_processed_to]
            .into_iter()
            .flatten()
            .min();
    } else {
        find_min_processed_to(
            mc_data_shards_processed_to,
            shard_id,
            &mut min_processed_to,
            |shard| shard == shard_id || shard.is_masterchain(),
        );

        // Combine with current and masterchain values and shard values
        min_processed_to = [
            shard_min_processed_to,
            min_processed_to,
            mc_data_min_processed_to,
        ]
        .into_iter()
        .flatten()
        .min();
    }

    min_processed_to
}

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

#[test]
fn test_unified_min_processed_to_calculation() {
    use crate::types::ShardDescriptionShort;
    use crate::types::processed_upto::{
        build_all_shards_processed_to_by_partitions, find_min_processed_to_by_shards,
    };

    // Test masterchain case
    let shard_id = ShardIdent::MASTERCHAIN;
    let next_block_id_short = BlockIdShort {
        shard: shard_id,
        seqno: 100,
    };

    let current_processed_to_by_partitions = {
        let mut map = ProcessedToByPartitions::default();
        map.insert(
            QueuePartitionIdx(0),
            [(shard_id, QueueKey::max_for_lt(5))].into_iter().collect(),
        );
        map
    };

    let mc_processed_to_by_partitions = current_processed_to_by_partitions.clone();

    let updated_shard = ShardIdent::new_full(0);
    let mut mc_data_shards_processed_to = FastHashMap::default();
    let mut test_processed_to = ProcessedToByPartitions::default();
    test_processed_to.insert(
        QueuePartitionIdx(0),
        [(shard_id, QueueKey::max_for_lt(4))].into_iter().collect(),
    );
    test_processed_to.insert(
        QueuePartitionIdx(1),
        [(shard_id, QueueKey::max_for_lt(3))].into_iter().collect(),
    );
    mc_data_shards_processed_to.insert(updated_shard, (true, test_processed_to));

    let mc_shards = vec![(updated_shard, ShardDescriptionShort::default())];

    // Test new approach
    let all_shards_processed_to = build_all_shards_processed_to_by_partitions(
        next_block_id_short,
        current_processed_to_by_partitions.clone(),
        mc_processed_to_by_partitions,
        mc_data_shards_processed_to.clone(),
        &mc_shards,
    );

    let min_by_shards = find_min_processed_to_by_shards(&all_shards_processed_to);
    let unified_result = min_by_shards.get(&shard_id).cloned();

    // Test legacy approach
    let legacy_result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        Some(QueueKey::max_for_lt(5)),
        Some(QueueKey::max_for_lt(5)),
        &mc_data_shards_processed_to,
    );

    // Both should produce the same result
    assert_eq!(unified_result, legacy_result);
    assert_eq!(unified_result, Some(QueueKey::max_for_lt(3)));
}

#[test]
fn test_unified_min_processed_to_calculation_shard() {
    use crate::types::ShardDescriptionShort;
    use crate::types::processed_upto::{
        build_all_shards_processed_to_by_partitions, find_min_processed_to_by_shards,
    };

    // Test shard case shard
    let shard_id = ShardIdent::new_full(0);
    let next_block_id_short = BlockIdShort {
        shard: shard_id,
        seqno: 100,
    };

    // Create test data for shard block
    let current_processed_to_by_partitions = {
        let mut map = ProcessedToByPartitions::default();
        map.insert(
            QueuePartitionIdx(0),
            [(shard_id, QueueKey::max_for_lt(11))].into_iter().collect(),
        );
        map
    };

    let mc_processed_to_by_partitions = {
        let mut map = ProcessedToByPartitions::default();
        map.insert(
            QueuePartitionIdx(0),
            [(shard_id, QueueKey::max_for_lt(9))].into_iter().collect(),
        );
        map
    };

    let updated_shard = ShardIdent::new_full(3);
    let mut mc_data_shards_processed_to = FastHashMap::default();
    let mut test_processed_to = ProcessedToByPartitions::default();
    test_processed_to.insert(
        QueuePartitionIdx(0),
        [(shard_id, QueueKey::max_for_lt(8))].into_iter().collect(),
    );
    test_processed_to.insert(
        QueuePartitionIdx(1),
        [(shard_id, QueueKey::max_for_lt(7))].into_iter().collect(),
    );
    mc_data_shards_processed_to.insert(updated_shard, (true, test_processed_to));

    let mc_shards = vec![
        (updated_shard, ShardDescriptionShort::default()),
        (shard_id, ShardDescriptionShort {
            seqno: 99,
            top_sc_block_updated: true,
            ..Default::default()
        }),
    ];

    // Test new approach
    let all_shards_processed_to = build_all_shards_processed_to_by_partitions(
        next_block_id_short,
        current_processed_to_by_partitions.clone(),
        mc_processed_to_by_partitions.clone(),
        mc_data_shards_processed_to.clone(),
        &mc_shards,
    );

    let min_by_shards = find_min_processed_to_by_shards(&all_shards_processed_to);
    let unified_result = min_by_shards.get(&shard_id).cloned();

    // Test legacy approach
    let legacy_result = calculate_min_internals_processed_to_for_shard(
        &shard_id,
        Some(QueueKey::max_for_lt(11)), // current_processed_to
        Some(QueueKey::max_for_lt(9)),  // mc_data_min_processed_to
        &mc_data_shards_processed_to,
    );

    // Both should produce the same result
    assert_eq!(unified_result, legacy_result);
    // For shard blocks, minimum should be from updated shard partitions (7)
    assert_eq!(unified_result, Some(QueueKey::max_for_lt(7)));
}
