use std::collections::BTreeMap;

use everscale_types::models::*;
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use crate::collator::do_collate::calculate_min_internals_processed_to;
use crate::types::ShardDescriptionShort;

#[test]
fn test_calculate_min_processed_to_masterchain() {
    // Mock data for masterchain test
    let shard_id = ShardIdent::MASTERCHAIN;

    let current_processed_to = Some(QueueKey::max_for_lt(5));
    let mc_processed_to = Some(QueueKey::max_for_lt(5));

    let updated_shard = ShardIdent::new_full(0);
    let not_updated_shard = ShardIdent::new_full(1);

    let mc_data_shards = vec![
        (updated_shard, ShardDescriptionShort {
            top_sc_block_updated: true,
            ..Default::default()
        }),
        (not_updated_shard, ShardDescriptionShort {
            top_sc_block_updated: false,
            ..Default::default()
        }),
    ];

    let mut mc_data_shards_processed_to = FastHashMap::default();
    let mut processed_to = BTreeMap::new();
    processed_to.insert(shard_id, QueueKey::max_for_lt(4));

    // check updated
    mc_data_shards_processed_to.insert(updated_shard, processed_to);

    let result = calculate_min_internals_processed_to(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards,
        &mc_data_shards_processed_to,
    );

    // updated shard should override current_processed_to
    assert_eq!(result, Some(QueueKey::max_for_lt(4)));

    let mut mc_data_shards_processed_to = FastHashMap::default();
    let mut processed_to = BTreeMap::new();
    processed_to.insert(shard_id, QueueKey::max_for_lt(4));

    // check updated
    mc_data_shards_processed_to.insert(not_updated_shard, processed_to);

    let result = calculate_min_internals_processed_to(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards,
        &mc_data_shards_processed_to,
    );

    // not updated shard should not override current_processed_to
    assert_eq!(result, Some(QueueKey::max_for_lt(5)));
}

#[test]
fn test_calculate_min_processed_to_shard() {
    // Mock data for shard test
    let shard_id = ShardIdent::new_full(2);

    let current_processed_to = Some(QueueKey::max_for_lt(10));

    let updated_shard = ShardIdent::new_full(3);
    let not_updated_shard = ShardIdent::new_full(4);

    let mc_data_shards = vec![
        (updated_shard, ShardDescriptionShort {
            top_sc_block_updated: true,
            ..Default::default()
        }),
        (not_updated_shard, ShardDescriptionShort {
            top_sc_block_updated: false,
            ..Default::default()
        }),
    ];

    let mut mc_data_shards_processed_to = FastHashMap::default();
    let mut processed_to = BTreeMap::new();
    processed_to.insert(shard_id, QueueKey::max_for_lt(8));
    let mc_processed_to = Some(QueueKey::max_for_lt(9));

    // Check updated shard
    mc_data_shards_processed_to.insert(updated_shard, processed_to);

    let result = calculate_min_internals_processed_to(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards,
        &mc_data_shards_processed_to,
    );

    // Updated shard should override current_processed_to
    assert_eq!(result, Some(QueueKey::max_for_lt(8)));

    // Reset processed_to for not-updated shard
    let mut mc_data_shards_processed_to = FastHashMap::default();
    let mut processed_to = BTreeMap::new();
    processed_to.insert(shard_id, QueueKey::max_for_lt(8));
    let mc_processed_to = Some(QueueKey::max_for_lt(11));
    // Check not updated shard
    mc_data_shards_processed_to.insert(not_updated_shard, processed_to);

    let result = calculate_min_internals_processed_to(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards,
        &mc_data_shards_processed_to,
    );

    // Not updated shard should not override current_processed_to
    assert_eq!(result, Some(QueueKey::max_for_lt(10)));

    // Verify combination with masterchain value
    let mc_data_shards_processed_to = FastHashMap::default();

    let mc_processed_to = Some(QueueKey::max_for_lt(9));

    let result = calculate_min_internals_processed_to(
        &shard_id,
        current_processed_to,
        mc_processed_to,
        &mc_data_shards,
        &mc_data_shards_processed_to,
    );

    // Minimum value should be returned
    assert_eq!(result, Some(QueueKey::max_for_lt(9)));
}
