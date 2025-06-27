use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::models::{BlockIdShort, IntAddr, MsgsExecutionParams, ShardIdent};
use tycho_block_util::queue::QueuePartitionIdx;

use crate::collator::messages_buffer::{MessageGroup, MessagesBufferLimits};
use crate::collator::messages_reader::{
    CollectExternalsResult, DebugExternalsRangeReaderState, DisplayMessageGroup, ExternalKey,
    ExternalsReader, FinalizedExternalsReader, GetNextMessageGroupMode, InternalsPartitionReader,
    InternalsPartitionReaderState, ReaderState,
};
use crate::collator::types::AnchorsCache;
use crate::collator::MsgsExecutionParamsStuff;
use crate::internal_queue::types::{EnqueuedMessage, PartitionRouter};
use crate::mempool::make_stub_anchor;
use crate::test_utils::try_init_test_tracing;
use crate::types::DisplayIter;

pub(crate) fn fill_test_anchors_cache(
    anchors_cache: &mut AnchorsCache,
    shard_id: ShardIdent,
) -> Vec<IntAddr> {
    let mut dst_addrs = vec![];
    let mut prev_anchor_id = 0;
    for anchor_id in 1..=82 {
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
        .insert_dst(&dst_addrs[3], QueuePartitionIdx(1))
        .unwrap();

    let msgs_exec_params = MsgsExecutionParams {
        externals_expire_timeout: 100,
        ..Default::default()
    };

    let msgs_exec_params_stuff =
        MsgsExecutionParamsStuff::create(Some(msgs_exec_params.clone()), msgs_exec_params);

    let mut buffer_limits_by_partitions = BTreeMap::new();
    buffer_limits_by_partitions.insert(QueuePartitionIdx(0), MessagesBufferLimits {
        max_count: 12,
        slots_count: 5,
        slot_vert_size: 4,
    });
    buffer_limits_by_partitions.insert(QueuePartitionIdx(1), MessagesBufferLimits {
        max_count: 12,
        slots_count: 1,
        slot_vert_size: 4,
    });

    let mut reader_state = ReaderState::default();
    reader_state.internals.partitions.insert(
        QueuePartitionIdx(0),
        InternalsPartitionReaderState::default(),
    );
    reader_state.internals.partitions.insert(
        QueuePartitionIdx(1),
        InternalsPartitionReaderState::default(),
    );

    let internals_reader_state = reader_state.internals;
    let externals_reader_state = reader_state.externals;

    let (_, anchor_44) = anchors_cache.get(8).unwrap();
    let next_chain_time = anchor_44.chain_time;

    let mut externals_reader = ExternalsReader::new(
        next_block_id_short.shard,
        next_block_id_short.seqno,
        next_chain_time,
        msgs_exec_params_stuff.clone(),
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

    // read 1 for block 1
    println!("read 1 for block  1");
    let metrics = externals_reader
        .read_into_buffers(GetNextMessageGroupMode::Continue, &partition_router)
        .unwrap();
    let metrics = metrics.get_total();
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    assert_eq!(metrics.read_ext_msgs_count, 17);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((0, 0))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((24, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 12);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 0);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 5);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 0);

    let par_ids = externals_reader.get_partition_ids();

    let prev_partitions_readers =
        BTreeMap::<QueuePartitionIdx, InternalsPartitionReader<EnqueuedMessage>>::new();

    // collect messages 1 for block 1
    println!("collect messages 1 for block 1");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(
                *par_id,
                &mut msg_group,
                &prev_partitions_readers,
                &msg_groups,
            )
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&QueuePartitionIdx(0)).unwrap();
    assert_eq!(msg_group.messages_count(), 11);
    let msg_group = msg_groups.get(&QueuePartitionIdx(1)).unwrap();
    assert_eq!(msg_group.messages_count(), 4);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 1);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 1);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 0);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 0);

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

    let range_reader_state = reader_state.externals.ranges.get(&1).unwrap();
    let by_par = range_reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

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
        msgs_exec_params_stuff.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    // refill after restart on block 2
    println!("refill after restart on block 2");
    let metrics = externals_reader
        .read_into_buffers(GetNextMessageGroupMode::Refill, &partition_router)
        .unwrap();
    let metrics = metrics.get_total();
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    // the read result should be the same as at the last loop in previous block
    assert_eq!(metrics.read_ext_msgs_count, 17);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((0, 0))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((24, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 12);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 5);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    // collect messages 1 on refill on block 2
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(
                *par_id,
                &mut msg_group,
                &prev_partitions_readers,
                &msg_groups,
            )
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    assert!(externals_reader.last_range_offsets_reached_in_all_partitions());

    // reset read state after refill
    externals_reader.reset_read_state();

    // continue reading on block 2
    println!("continue reading 1 on block 2");
    let metrics = externals_reader
        .read_into_buffers(GetNextMessageGroupMode::Continue, &partition_router)
        .unwrap();
    let metrics = metrics.get_total();
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    assert_eq!(metrics.read_ext_msgs_count, 18);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 1);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 1);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((0, 0))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((24, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((24, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 12);
    assert_eq!(by_par.skip_offset, 1);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 6);
    assert_eq!(by_par.skip_offset, 1);
    assert_eq!(by_par.processed_offset, 1);

    // collect messages 1 for block 2
    println!("collect messages 1 for block 2");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(
                *par_id,
                &mut msg_group,
                &prev_partitions_readers,
                &msg_groups,
            )
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&QueuePartitionIdx(0)).unwrap();
    assert_eq!(msg_group.messages_count(), 12);
    let msg_group = msg_groups.get(&QueuePartitionIdx(1)).unwrap();
    assert_eq!(msg_group.messages_count(), 4);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 2);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 2);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 1);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 2); // 1 ext msg expired in buffer
    assert_eq!(by_par.skip_offset, 1);
    assert_eq!(by_par.processed_offset, 1);

    // all internals collected, reader entered the FinishCurrentExternals stage, so we do not read more externals

    // collect messages 2 for block 2
    println!("collect messages 2 for block 2");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(
                *par_id,
                &mut msg_group,
                &prev_partitions_readers,
                &msg_groups,
            )
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&QueuePartitionIdx(0)).unwrap();
    assert_eq!(msg_group.messages_count(), 1);
    let msg_group = msg_groups.get(&QueuePartitionIdx(1)).unwrap();
    assert_eq!(msg_group.messages_count(), 2);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((0, 0)));
    assert_eq!(by_par.curr_processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&1).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 0);
    assert_eq!(by_par.processed_offset, 1);

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 1);
    assert_eq!(by_par.processed_offset, 1);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 1);
    assert_eq!(by_par.processed_offset, 1);

    assert!(!externals_reader.range_readers.contains_key(&3));

    println!("finalize reader state after all read messages collected");
    externals_reader.retain_only_last_range_reader().unwrap();
    for &par_id in &par_ids {
        externals_reader
            .set_processed_to_current_position(par_id)
            .unwrap();
        externals_reader
            .set_skip_processed_offset_to_current(par_id)
            .unwrap();
    }
    externals_reader
        .set_from_to_current_position_in_last_range_reader()
        .unwrap();
    print_state(&externals_reader);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert_eq!(
        range_reader.reader_state.range.from,
        range_reader.reader_state.range.current_position
    );

    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
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
        msgs_exec_params_stuff.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    // continue reading 1 on block 3
    println!("continue reading on block 3");
    let metrics = externals_reader
        .read_into_buffers(GetNextMessageGroupMode::Continue, &partition_router)
        .unwrap();
    let metrics = metrics.get_total();
    println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);
    print_state(&externals_reader);

    assert_eq!(metrics.read_ext_msgs_count, 17);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 3);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((68, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 12);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 5);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    // collect messages 1 for block 3
    println!("collect messages 1 for block 3");
    let mut msg_groups = BTreeMap::new();
    for par_id in &par_ids {
        externals_reader
            .increment_curr_processed_offset(par_id)
            .unwrap();
        let mut msg_group = MessageGroup::default();
        let CollectExternalsResult { metrics: _ } = externals_reader
            .collect_messages(
                *par_id,
                &mut msg_group,
                &prev_partitions_readers,
                &msg_groups,
            )
            .unwrap();
        println!(
            "par {} msg_group: {}",
            par_id,
            DisplayMessageGroup(&msg_group)
        );
        msg_groups.insert(*par_id, msg_group);
    }
    print_state(&externals_reader);

    let msg_group = msg_groups.get(&QueuePartitionIdx(0)).unwrap();
    assert_eq!(msg_group.messages_count(), 11);
    let msg_group = msg_groups.get(&QueuePartitionIdx(1)).unwrap();
    assert_eq!(msg_group.messages_count(), 4);

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 4);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 4);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
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

    let range_reader_state = reader_state.externals.ranges.get(&3).unwrap();
    let by_par = range_reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 4);

    let by_par = range_reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 4);

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
        msgs_exec_params_stuff.clone(),
        buffer_limits_by_partitions.clone(),
        anchors_cache.clone(),
        externals_reader_state,
    );

    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 0);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 0);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    assert!(!range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((68, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.from,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 4);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 4);

    // refill after restart on block 4
    println!("refill after restart on block 4");

    // read and collect 4 times
    for i in 1..=4 {
        println!("read messages on refill {i}");
        let metrics = externals_reader
            .read_into_buffers(GetNextMessageGroupMode::Refill, &partition_router)
            .unwrap();
        let metrics = metrics.get_total();
        println!("read_ext_msgs_count: {}", metrics.read_ext_msgs_count);

        println!("collect messages on refill {i}");
        let mut msg_groups = BTreeMap::new();
        for par_id in &par_ids {
            externals_reader
                .increment_curr_processed_offset(par_id)
                .unwrap();
            let mut msg_group = MessageGroup::default();
            let CollectExternalsResult { metrics: _ } = externals_reader
                .collect_messages(
                    *par_id,
                    &mut msg_group,
                    &prev_partitions_readers,
                    &msg_groups,
                )
                .unwrap();
            println!(
                "par {} msg_group: {}",
                par_id,
                DisplayMessageGroup(&msg_group)
            );
            msg_groups.insert(*par_id, msg_group);
        }

        print_state(&externals_reader);
    }

    // after refill and collect the state should be the same as after block 3
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(0)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 4);
    let by_par = externals_reader
        .reader_state
        .get_state_by_partition(1)
        .unwrap();
    assert_eq!(by_par.processed_to, ExternalKey::from((44, 3)));
    assert_eq!(by_par.curr_processed_offset, 4);

    assert!(!externals_reader.range_readers.contains_key(&1));

    let range_reader = externals_reader.range_readers.get(&2).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 0);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 3);

    let range_reader = externals_reader.range_readers.get(&3).unwrap();
    assert!(range_reader.fully_read);
    assert_eq!(
        range_reader.reader_state.range.from,
        ExternalKey::from((44, 3))
    );
    assert_eq!(
        range_reader.reader_state.range.to,
        ExternalKey::from((68, 1))
    );
    assert_eq!(
        range_reader.reader_state.range.current_position,
        range_reader.reader_state.range.to,
    );
    let by_par = range_reader.reader_state.get_state_by_partition(0).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 4);

    let by_par = range_reader.reader_state.get_state_by_partition(1).unwrap();
    assert_eq!(by_par.buffer.msgs_count(), 1);
    assert_eq!(by_par.skip_offset, 3);
    assert_eq!(by_par.processed_offset, 4);
}
