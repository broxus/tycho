use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardIdent};

use super::MasterBlocksCacheData;

#[test]
fn test_apllied_range_update() {
    let mut cache = MasterBlocksCacheData::default();

    // first we collated block 3405
    let block_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 3405,
        root_hash: HashBytes::default(),
        file_hash: HashBytes::default(),
    };
    cache.insert_last_collated_block_id(&block_id);
    println!();
    println!("update_last_collated_block_id(3405)");
    println!(
        "last collated mc seqno: {:?}, applied_mc_queue_range: {:?}",
        cache
            .get_last_collated_block_id()
            .map(|id| id.as_short_id()),
        cache.applied_mc_queue_range
    );

    assert_eq!(cache.get_last_collated_block_id(), Some(&block_id));
    assert_eq!(cache.applied_mc_queue_range, None);

    // then we received block 3405 from bc
    cache.move_range_end(3405);
    println!();
    println!("move_range_end(3405)");
    println!(
        "last collated mc seqno: {:?}, applied_mc_queue_range: {:?}",
        cache
            .get_last_collated_block_id()
            .map(|id| id.as_short_id()),
        cache.applied_mc_queue_range
    );

    assert_eq!(cache.get_last_collated_block_id(), Some(&block_id));
    assert_eq!(cache.applied_mc_queue_range, Some((3405, 3405)));

    // then we extracted block 3405 from cache and committed
    cache.move_range_start(3405);
    println!();
    println!("move_range_start(3405)");
    println!(
        "last collated mc seqno: {:?}, applied_mc_queue_range: {:?}",
        cache
            .get_last_collated_block_id()
            .map(|id| id.as_short_id()),
        cache.applied_mc_queue_range
    );

    assert_eq!(cache.get_last_collated_block_id(), Some(&block_id));
    assert_eq!(cache.applied_mc_queue_range, None);

    // then we received block 3406 from bc
    cache.move_range_end(3406);
    println!();
    println!("move_range_end(3406)");
    println!(
        "last collated mc seqno: {:?}, applied_mc_queue_range: {:?}",
        cache
            .get_last_collated_block_id()
            .map(|id| id.as_short_id()),
        cache.applied_mc_queue_range
    );

    assert_eq!(cache.get_last_collated_block_id(), Some(&block_id));
    assert_eq!(cache.applied_mc_queue_range, Some((3406, 3406)));

    // then we received block 3407 from bc
    cache.move_range_end(3407);
    println!();
    println!("move_range_end(3407)");
    println!(
        "last collated mc seqno: {:?}, applied_mc_queue_range: {:?}",
        cache
            .get_last_collated_block_id()
            .map(|id| id.as_short_id()),
        cache.applied_mc_queue_range
    );

    assert_eq!(cache.get_last_collated_block_id(), Some(&block_id));
    assert_eq!(cache.applied_mc_queue_range, Some((3406, 3407)));

    // then we received block 3408 from bc
    cache.move_range_end(3408);
    println!();
    println!("move_range_end(3408)");
    println!(
        "last collated mc seqno: {:?}, applied_mc_queue_range: {:?}",
        cache
            .get_last_collated_block_id()
            .map(|id| id.as_short_id()),
        cache.applied_mc_queue_range
    );

    assert_eq!(cache.get_last_collated_block_id(), Some(&block_id));
    assert_eq!(cache.applied_mc_queue_range, Some((3406, 3408)));
}
