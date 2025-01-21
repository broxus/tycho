use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, IntAddr, MsgsExecutionParams, ShardIdent};
use tycho_block_util::queue::RouterDirection;

use crate::collator::messages_buffer::{MessageGroup, MessagesBufferLimits};
use crate::collator::messages_reader::{
    CollectExternalsResult, DebugExternalsRangeReaderState, DisplayMessageGroup, ExternalKey,
    ExternalsReader, FinalizedExternalsReader, GetNextMessageGroupMode,
    InternalsPartitionReaderState, ReaderState,
};
use crate::collator::types::AnchorsCache;
use crate::internal_queue::types::PartitionRouter;
use crate::mempool::make_stub_anchor;
use crate::test_utils::try_init_test_tracing;
use crate::types::processed_upto::PartitionId;
use crate::types::DisplayIter;

pub(crate) fn fill_test_anchors_cache(
    anchors_cache: &mut AnchorsCache,
    shard_id: ShardIdent,
) -> Vec<IntAddr> {
    let mut dst_addrs = vec![];
    let mut prev_anchor_id = 0;
    for anchor_id in 1..=48 {
        if anchor_id % 4 != 0 {
            continue;
        }
        let anchor = Arc::new(make_stub_anchor(anchor_id, prev_anchor_id));
        let mut curr_dst_addrs = anchor
            .iter_externals(0)
            .map(|ext_msg| ext_msg.info.dst.clone())
            .collect::<Vec<_>>();
        prev_anchor_id = anchor_id;
        let our_exts_count = anchor.count_externals_for(&shard_id, 0);
        let has_externals = our_exts_count > 0;
        if has_externals {
            tracing::trace!(
                "anchor (id: {}, chain_time: {}, externals_count: {}): has_externals for shard {}: {}, externals dst: {}",
                anchor_id,
                anchor.chain_time,
                anchor.externals.len(),
                shard_id,
                has_externals,
                DisplayIter(curr_dst_addrs.iter().map(|addr| addr.to_string())),
            );
        }
        anchors_cache.insert(anchor, our_exts_count);
        dst_addrs.append(&mut curr_dst_addrs);
    }

    dst_addrs.sort();
    dst_addrs.dedup();
    dst_addrs
}

#[test]
fn test_read_externals() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let shard_id = ShardIdent::new_full(0);
    let next_block_id_short = BlockIdShort {
        shard: shard_id,
        seqno: 1,
    };

    let mut anchors_cache = AnchorsCache::default();
    let dst_addrs = fill_test_anchors_cache(&mut anchors_cache, shard_id);
    println!(
        "dst_addrs: {}",
        DisplayIter(dst_addrs.iter().map(|addr| addr.to_string())),
    );

    let mut partition_router = PartitionRouter::new();
    partition_router
        .insert(RouterDirection::Dest, dst_addrs[3].clone(), 1)
        .unwrap();

    let msgs_exec_params = Arc::new(MsgsExecutionParams {
        externals_expire_timeout: 60,
        ..Default::default()
    });

    let mut buffer_limits_by_partitions = BTreeMap::new();
    buffer_limits_by_partitions.insert(0 as PartitionId, MessagesBufferLimits {
        max_count: 10,
        slots_count: 5,
        slot_vert_size: 4,
    });
    buffer_limits_by_partitions.insert(1 as PartitionId, MessagesBufferLimits {
        max_count: 10,
        slots_count: 1,
        slot_vert_size: 4,
    });

    let mut reader_state = ReaderState::default();
    reader_state
        .internals
        .partitions
        .insert(0, InternalsPartitionReaderState::default());
    reader_state
        .internals
        .partitions
        .insert(1, InternalsPartitionReaderState::default());

    let internals_reader_state = reader_state.internals;
    let externals_reader_state = reader_state.externals;

    let (_, anchor_44) = anchors_cache.get(8).unwrap();
    let next_chain_time = anchor_44.chain_time;

    let mut externals_reader = ExternalsReader::new(
        next_block_id_short.shard,
        next_block_id_short.seqno,
        next_chain_time,
        msgs_exec_params.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    let print_state = |externals_reader: &ExternalsReader| {
        println!(
            "externals_reader_state.by_partitions: {:?}",
            externals_reader.reader_state.by_partitions,
        );

        for (seqno, range_reader) in &externals_reader.range_readers {
            println!(
                "range_reader (seqno={}): fully_read={}, state={:?}",
                seqno,
                range_reader.fully_read,
                DebugExternalsRangeReaderState(&range_reader.reader_state),
            );
        }
        println!("");
    };

    // read for block 1
    println!("read for block  1");
    let metrics =
        externals_reader.read_into_buffers(GetNextMessageGroupMode::Continue, &partition_router);
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    assert_eq!(metrics.read_ext_msgs_count, 14);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((0, 0))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((28, 5))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 10);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 4);

    // finalize reader
    let FinalizedExternalsReader {
        externals_reader_state,
        ..
    } = externals_reader.finalize().unwrap();
    let reader_state = ReaderState {
        externals: externals_reader_state,
        internals: internals_reader_state,
    };
    let processed_upto = reader_state.get_updated_processed_upto();

    // emulate restart
    // read state from processed_upto
    let reader_state = ReaderState::new(&processed_upto);
    let internals_reader_state = reader_state.internals;
    let externals_reader_state = reader_state.externals;

    // next block
    let next_block_id_short = BlockIdShort {
        shard: next_block_id_short.shard,
        seqno: next_block_id_short.seqno + 1,
    };

    let next_chain_time = anchors_cache
        .last_imported_anchor()
        .map(|a| a.ct)
        .unwrap_or_default();

    // create new reader
    let mut externals_reader = ExternalsReader::new(
        next_block_id_short.shard,
        next_block_id_short.seqno,
        next_chain_time,
        msgs_exec_params.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    // refill after restart on block 2
    println!("refill after restart on block 2");
    let metrics =
        externals_reader.read_into_buffers(GetNextMessageGroupMode::Refill, &partition_router);
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    // the read result should be the same as after previous block
    assert_eq!(metrics.read_ext_msgs_count, 14);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((0, 0))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((28, 5))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 10);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 4);

    // continue reading on block 2
    println!("continue reading on block 2");
    let metrics =
        externals_reader.read_into_buffers(GetNextMessageGroupMode::Continue, &partition_router);
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    assert_eq!(metrics.read_ext_msgs_count, 15);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 0);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 0);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((0, 0))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((28, 5))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 10);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 4);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((28, 5))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 10);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 5);

    let par_ids = externals_reader.get_partition_ids();

    // collect messages 1
    println!("collect messages 1");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(par_id, &mut msg_group, &BTreeMap::new(), &msg_groups)
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&0).unwrap();
    assert_eq!(msg_group.messages_count(), 14);
    let msg_group = msg_groups.get(&1).unwrap();
    assert_eq!(msg_group.messages_count(), 4);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 1);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 1);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 6);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 5);

    // collect messages 2
    println!("collect messages 2");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(par_id, &mut msg_group, &BTreeMap::new(), &msg_groups)
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&0).unwrap();
    assert_eq!(msg_group.messages_count(), 6);
    let msg_group = msg_groups.get(&1).unwrap();
    assert_eq!(msg_group.messages_count(), 4);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 2);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 2);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);

    // collect messages 3
    println!("collect messages 3");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(par_id, &mut msg_group, &BTreeMap::new(), &msg_groups)
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&0).unwrap();
    assert_eq!(msg_group.messages_count(), 0);
    let msg_group = msg_groups.get(&1).unwrap();
    assert_eq!(msg_group.messages_count(), 1);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);

    println!("finalize reader state after all read messages collected");
    externals_reader.retain_only_last_range_reader().unwrap();
    for par_id in &par_ids {
        externals_reader
            .set_processed_to_current_position(par_id)
            .unwrap();
        externals_reader.set_skip_offset_to_current(par_id).unwrap();
    }
    externals_reader
        .set_from_to_current_position_in_last_range_reader()
        .unwrap();
    print_state(&externals_reader);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert_eq!(
        range_reader.reader_state.range.from,
        range_reader.reader_state.range.current_position
    );

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    // finalize reader
    let FinalizedExternalsReader {
        externals_reader_state,
        ..
    } = externals_reader.finalize().unwrap();

    // next block
    let next_block_id_short = BlockIdShort {
        shard: next_block_id_short.shard,
        seqno: next_block_id_short.seqno + 1,
    };

    // create new reader
    let mut externals_reader = ExternalsReader::new(
        next_block_id_short.shard,
        next_block_id_short.seqno,
        next_chain_time,
        msgs_exec_params.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    // continue reading on block 3
    println!("continue reading on block 3");
    let metrics =
        externals_reader.read_into_buffers(GetNextMessageGroupMode::Continue, &partition_router);
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    assert_eq!(metrics.read_ext_msgs_count, 3);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 8))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 2);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    // finalize reader
    let FinalizedExternalsReader {
        externals_reader_state,
        ..
    } = externals_reader.finalize().unwrap();
    let reader_state = ReaderState {
        externals: externals_reader_state,
        internals: internals_reader_state,
    };
    let processed_upto = reader_state.get_updated_processed_upto();

    // emulate restart
    // read state from processed_upto
    let reader_state = ReaderState::new(&processed_upto);
    let externals_reader_state = reader_state.externals;

    // next block
    let next_block_id_short = BlockIdShort {
        shard: next_block_id_short.shard,
        seqno: next_block_id_short.seqno + 1,
    };

    // create new reader
    let mut externals_reader = ExternalsReader::new(
        next_block_id_short.shard,
        next_block_id_short.seqno,
        next_chain_time,
        msgs_exec_params.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 0);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 0);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 8))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.from,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    // refill after restart on block 4
    println!("refill after restart on block 4");
    let metrics =
        externals_reader.read_into_buffers(GetNextMessageGroupMode::Refill, &partition_router);
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    // collect 3 times
    for i in 1..=3 {
        println!("collect messages after refill {}", i);
        let mut msg_groups = BTreeMap::new();
        for par_id in &par_ids {
            externals_reader
                .increment_curr_processed_offset(par_id)
                .unwrap();
            let mut msg_group = MessageGroup::default();
            let CollectExternalsResult { metrics: _ } = externals_reader
                .collect_messages(par_id, &mut msg_group, &BTreeMap::new(), &msg_groups)
                .unwrap();
            println!(
                "par {} msg_group: {}",
                par_id,
                DisplayMessageGroup(&msg_group)
            );
            assert_eq!(msg_group.messages_count(), 0);
            msg_groups.insert(*par_id, msg_group);
        }
        print_state(&externals_reader);
    }

    // after refill and collect the state should be the same as after block 3
    assert_eq!(metrics.read_ext_msgs_count, 3);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((48, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((48, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((48, 8))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&0)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 2);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader
        .reader_state
        .get_state_by_partition(&1)
        .unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);
}
