use std::time::Instant;

use tycho_types::cell::{CellBuilder, HashBytes};
use tycho_types::models::{IntAddr, IntMsgInfo, MsgInfo, ShardIdent, StdAddr};

use super::{DebugMessageGroupDetailed, DebugMessagesBuffer, MessageGroup, MessagesBuffer};
use crate::collator::messages_buffer::IncludeAllMessages;
use crate::collator::types::ParsedMessage;
use crate::internal_queue::types::EnqueuedMessage;
use crate::mempool::{MempoolAnchorId, make_stub_external};

pub(crate) fn make_stub_internal_parsed_message(
    src_shard: ShardIdent,
    dst: IntAddr,
    created_lt: u64,
    is_new: bool,
) -> Box<ParsedMessage> {
    let dst_wc = dst.workchain();
    let info = IntMsgInfo {
        dst,
        created_lt,
        ..Default::default()
    };
    let cell = CellBuilder::build_from(&info).unwrap();
    let enq_msg = EnqueuedMessage { info, cell };
    let msg = ParsedMessage {
        info: MsgInfo::Int(enq_msg.info),
        dst_in_current_shard: true,
        cell: enq_msg.cell,
        special_origin: None,
        block_seqno: None,
        from_same_shard: (!is_new).then(|| dst_wc == src_shard.workchain()),
        ext_msg_chain_time: None,
    };
    Box::new(msg)
}

pub(crate) fn make_stub_external_parsed_message(
    anchor_id: MempoolAnchorId,
    chain_time: u64,
    msg_idx: u32,
    dst: IntAddr,
) -> Box<ParsedMessage> {
    let ext_msg = make_stub_external(anchor_id, chain_time, msg_idx, dst);
    Box::new(ParsedMessage {
        info: MsgInfo::ExtIn(ext_msg.info),
        dst_in_current_shard: true,
        cell: ext_msg.cell,
        special_origin: None,
        block_seqno: None,
        from_same_shard: None,
        ext_msg_chain_time: Some(chain_time),
    })
}

fn fill_test_buffers(
    mc_shard: ShardIdent,
    dst_shard: ShardIdent,
) -> (
    MessagesBuffer,
    MessagesBuffer,
    MessagesBuffer,
    MessagesBuffer,
) {
    let mut dst_addrs = vec![];
    for i in 1..=20 {
        dst_addrs.push(IntAddr::Std(StdAddr::new(
            dst_shard.workchain() as i8,
            HashBytes([i; 32]),
        )));
    }

    let add_int_msgs = |buffer: &mut MessagesBuffer,
                        src_shard: ShardIdent,
                        dst_addr_num: usize,
                        lt: u64,
                        is_new: bool,
                        amount: usize| {
        (0..amount).for_each(|_| {
            buffer.add_message(make_stub_internal_parsed_message(
                src_shard,
                dst_addrs[dst_addr_num - 1].clone(),
                lt,
                is_new,
            ));
        });
    };

    let add_ext_msgs = |buffer: &mut MessagesBuffer,
                        anchor_id: MempoolAnchorId,
                        chain_time: u64,
                        msg_idx: &mut u32,
                        dst_addr_num: usize,
                        amount: usize| {
        (0..amount).for_each(|_| {
            buffer.add_message(make_stub_external_parsed_message(
                anchor_id,
                chain_time,
                *msg_idx,
                dst_addrs[dst_addr_num - 1].clone(),
            ));
            *msg_idx += 1;
        });
    };

    // internals partition 0:
    //  addr:   1   3   4   6   8   10  12  13  14  15  16
    //  count:  10  3   2   7   10  2   1       2       10
    //          10      2   2       1       2   5   10  1
    let mut int_buffer_par_0 = MessagesBuffer::default();
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 1, 1000, false, 10);
    add_int_msgs(&mut int_buffer_par_0, mc_shard, 3, 1000, false, 3);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 4, 1000, false, 2);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 6, 1000, false, 7);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 8, 1000, false, 10);
    add_int_msgs(&mut int_buffer_par_0, mc_shard, 10, 1000, false, 2);
    add_int_msgs(&mut int_buffer_par_0, mc_shard, 12, 1000, false, 1);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 14, 1000, false, 2);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 16, 1000, false, 10);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 1, 1100, false, 10);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 4, 1100, false, 2);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 6, 1100, false, 2);
    add_int_msgs(&mut int_buffer_par_0, mc_shard, 10, 1100, false, 1);
    add_int_msgs(&mut int_buffer_par_0, mc_shard, 13, 1100, false, 2);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 14, 1100, false, 5);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 15, 1100, false, 10);
    add_int_msgs(&mut int_buffer_par_0, dst_shard, 16, 1100, false, 1);

    println!(
        "\r\nint_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );

    // internals partition 1:
    //  addr:   1   2
    //  count:      10
    //              20
    //          30  20
    let mut int_buffer_par_1 = MessagesBuffer::default();
    add_int_msgs(&mut int_buffer_par_1, dst_shard, 2, 1000, false, 10);
    add_int_msgs(&mut int_buffer_par_1, dst_shard, 2, 1100, false, 20);
    add_int_msgs(&mut int_buffer_par_1, dst_shard, 2, 1200, false, 20);
    add_int_msgs(&mut int_buffer_par_1, dst_shard, 1, 1200, false, 30);

    println!(
        "int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );

    // externals range 1:
    //  addr:   1   2   4   5   6   7   17  19
    //  count:  5   2   2   2       8   2
    //          5       1   2   10      5   14
    let mut msg_idx;
    let mut ext_buffer_range_1 = MessagesBuffer::default();
    msg_idx = 0;
    add_ext_msgs(&mut ext_buffer_range_1, 1, 11000, &mut msg_idx, 1, 5);
    add_ext_msgs(&mut ext_buffer_range_1, 1, 11000, &mut msg_idx, 2, 2);
    add_ext_msgs(&mut ext_buffer_range_1, 1, 11000, &mut msg_idx, 4, 2);
    add_ext_msgs(&mut ext_buffer_range_1, 1, 11000, &mut msg_idx, 5, 2);
    add_ext_msgs(&mut ext_buffer_range_1, 1, 11000, &mut msg_idx, 7, 8);
    add_ext_msgs(&mut ext_buffer_range_1, 1, 11000, &mut msg_idx, 17, 2);
    msg_idx = 0;
    add_ext_msgs(&mut ext_buffer_range_1, 2, 12000, &mut msg_idx, 6, 10);
    add_ext_msgs(&mut ext_buffer_range_1, 2, 12000, &mut msg_idx, 1, 5);
    add_ext_msgs(&mut ext_buffer_range_1, 2, 12000, &mut msg_idx, 4, 1);
    add_ext_msgs(&mut ext_buffer_range_1, 2, 12000, &mut msg_idx, 5, 2);
    add_ext_msgs(&mut ext_buffer_range_1, 2, 12000, &mut msg_idx, 17, 5);
    add_ext_msgs(&mut ext_buffer_range_1, 2, 12000, &mut msg_idx, 19, 14);

    println!(
        "ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );

    // externals range 2:
    //  addr:   9   11  18  20  1   6   17
    //  count:      7   5       4       1
    //          2   3       3       8   2
    //              3       1           3
    let mut ext_buffer_range_2 = MessagesBuffer::default();
    msg_idx = 0;
    add_ext_msgs(&mut ext_buffer_range_2, 3, 13000, &mut msg_idx, 11, 7);
    add_ext_msgs(&mut ext_buffer_range_2, 3, 13000, &mut msg_idx, 18, 5);
    add_ext_msgs(&mut ext_buffer_range_2, 3, 13000, &mut msg_idx, 1, 4);
    add_ext_msgs(&mut ext_buffer_range_2, 3, 13000, &mut msg_idx, 17, 1);
    msg_idx = 0;
    add_ext_msgs(&mut ext_buffer_range_2, 4, 14000, &mut msg_idx, 9, 2);
    add_ext_msgs(&mut ext_buffer_range_2, 4, 14000, &mut msg_idx, 11, 3);
    add_ext_msgs(&mut ext_buffer_range_2, 4, 14000, &mut msg_idx, 20, 3);
    add_ext_msgs(&mut ext_buffer_range_2, 4, 14000, &mut msg_idx, 6, 8);
    add_ext_msgs(&mut ext_buffer_range_2, 4, 14000, &mut msg_idx, 17, 2);
    msg_idx = 0;
    add_ext_msgs(&mut ext_buffer_range_2, 5, 15000, &mut msg_idx, 11, 3);
    add_ext_msgs(&mut ext_buffer_range_2, 5, 15000, &mut msg_idx, 20, 1);
    add_ext_msgs(&mut ext_buffer_range_2, 5, 15000, &mut msg_idx, 17, 3);

    println!(
        "ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );

    (
        int_buffer_par_0,
        int_buffer_par_1,
        ext_buffer_range_1,
        ext_buffer_range_2,
    )
}

struct TimerHelper(Instant);
impl TimerHelper {
    fn start() -> Self {
        Self(Instant::now())
    }

    fn print_elapsed_and_restart(&mut self) {
        println!("elapsed: {} ns", self.0.elapsed().as_nanos());
        self.0 = Instant::now();
    }
}

#[test]
fn test_message_group_filling_from_buffers() {
    let dst_shard = ShardIdent::new_full(0);
    let (
        mut int_buffer_par_0,
        mut int_buffer_par_1,
        mut ext_buffer_range_1,
        mut ext_buffer_range_2,
    ) = fill_test_buffers(ShardIdent::MASTERCHAIN, dst_shard);

    let group_limit = 8;
    let group_vert_size = 4;

    let mut timer = TimerHelper::start();

    // dummy msg check
    let do_not_check_skip_msg = IncludeAllMessages;
    let check_skip_msg = IncludeAllMessages;

    // 1. ==========
    // fill first message group
    let mut msg_group = MessageGroup::default();

    // first fill group with internals from partition 0
    // all 5 normal slots should be fully filled
    int_buffer_par_0.fill_message_group(
        &mut msg_group,
        group_limit - 3,
        group_vert_size,
        |_| (false, 0),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n1.1: int_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );
    println!(
        "1.1: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append internals from the low-priority partition 1
    // additional 1 more slot should be fully filled for account 2
    int_buffer_par_1.fill_message_group(
        &mut msg_group,
        group_limit - 2,
        group_vert_size,
        |account_id| (int_buffer_par_0.msgs.contains_key(account_id), 1),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n1.2: int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );
    println!(
        "1.2: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append externals from range 1
    // should fully fill slots 7 and 8
    // should append messages to slots 1-4, 6
    // should not append messages to slot 5
    ext_buffer_range_1.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |_| (false, 0),
        check_skip_msg,
    );
    println!(
        "\r\n1.3: ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );
    println!(
        "1.3: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then try to append externals from range 2
    // should fully fill all slots
    ext_buffer_range_2.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |account_id| (ext_buffer_range_1.msgs.contains_key(account_id), 1),
        check_skip_msg,
    );
    println!(
        "\r\n1.4: ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );
    println!(
        "1.4: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // 2. ==========
    // fill next message group
    msg_group = MessageGroup::default();

    // first fill group with internals from partition 0
    // all 5 normal slots should be fully filled
    int_buffer_par_0.fill_message_group(
        &mut msg_group,
        group_limit - 3,
        group_vert_size,
        |_| (false, 0),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n2.1: int_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );
    println!(
        "2.1: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append internals from the low-priority partition 1
    // additional 1 more slot should be fully filled for account 2
    int_buffer_par_1.fill_message_group(
        &mut msg_group,
        group_limit - 2,
        group_vert_size,
        |account_id| (int_buffer_par_0.msgs.contains_key(account_id), 1),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n2.2: int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );
    println!(
        "2.2: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append externals from range 1
    // should fully fill slots 7 and 8
    // should append messages to slots 1, 2, 6
    // should not append messages to slot 3-5
    ext_buffer_range_1.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |_| (false, 0),
        check_skip_msg,
    );
    println!(
        "\r\n2.3: ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );
    println!(
        "2.3: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then try to append externals from range 2
    // should fully fill all slots
    ext_buffer_range_2.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |account_id| (ext_buffer_range_1.msgs.contains_key(account_id), 1),
        check_skip_msg,
    );
    println!(
        "\r\n2.4: ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );
    println!(
        "2.4: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // 3. ==========
    // fill next message group
    msg_group = MessageGroup::default();

    // first fill group with internals from partition 0
    // should fully fill slots 1, 3-5
    // should not fully fill slot 2
    int_buffer_par_0.fill_message_group(
        &mut msg_group,
        group_limit - 3,
        group_vert_size,
        |_| (false, 0),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n3.1: int_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );
    println!(
        "3.1: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append internals from the low-priority partition 1
    // additional 1 more slot should be fully filled for account 2
    int_buffer_par_1.fill_message_group(
        &mut msg_group,
        group_limit - 2,
        group_vert_size,
        |account_id| (int_buffer_par_0.msgs.contains_key(account_id), 1),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n3.2: int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );
    println!(
        "3.2: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append externals from range 1
    // should fully fill slot 8
    // should not fully fill slot 7
    // should append messages to slots 1, 2
    // should not append messages to slots 3-6
    ext_buffer_range_1.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |_| (false, 0),
        check_skip_msg,
    );
    println!(
        "\r\n3.3: ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );
    println!(
        "3.3: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then try to append externals from range 2
    // should fully fill all slots
    ext_buffer_range_2.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |account_id| (ext_buffer_range_1.msgs.contains_key(account_id), 1),
        check_skip_msg,
    );
    println!(
        "\r\n3.4: ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );
    println!(
        "3.4: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // 4. ==========
    // fill next message group
    msg_group = MessageGroup::default();

    // first fill group with internals from partition 0
    // should fully fill slots 1, 3, 4
    // should not fully fill slot 2
    // should not fill slot 5
    int_buffer_par_0.fill_message_group(
        &mut msg_group,
        group_limit - 3,
        group_vert_size,
        |_| (false, 0),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n4.1: int_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );
    println!(
        "4.1: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append internals from the low-priority partition 1
    // should fully fill slot 5 for account 2
    int_buffer_par_1.fill_message_group(
        &mut msg_group,
        group_limit - 2,
        group_vert_size,
        |account_id| (int_buffer_par_0.msgs.contains_key(account_id), 1),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n4.2: int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );
    println!(
        "4.2: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append externals from range 1
    // should fully fill slots 6, 7
    // should not fill slot 8
    // should append messages to slots 1, 6
    // should not append messages to slots 2-5
    ext_buffer_range_1.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |_| (false, 0),
        check_skip_msg,
    );
    println!(
        "\r\n4.3: ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );
    println!(
        "4.3: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then try to append externals from range 2
    // should fully fill slot 8
    // should append messages to slots 2, 3
    // should not append messages to slots 4, 5
    ext_buffer_range_2.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |account_id| (ext_buffer_range_1.msgs.contains_key(account_id), 1),
        check_skip_msg,
    );
    println!(
        "\r\n4.4: ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );
    println!(
        "4.4: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // 5. ==========
    // fill next message group
    msg_group = MessageGroup::default();

    // first fill group with internals from partition 0
    // should fully fill slots 1, 2
    // should not fill slots 3-5
    int_buffer_par_0.fill_message_group(
        &mut msg_group,
        group_limit - 3,
        group_vert_size,
        |_| (false, 0),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n5.1: int_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );
    println!(
        "5.1: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append internals from the low-priority partition 1
    // should fully fill slot 3 for account 2
    int_buffer_par_1.fill_message_group(
        &mut msg_group,
        group_limit - 2,
        group_vert_size,
        |account_id| (int_buffer_par_0.msgs.contains_key(account_id), 1),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n5.2: int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );
    println!(
        "5.2: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append externals from range 1
    // should not fully fill slots 4, 5
    // should not fill slots 6-8
    // should append messages to slot 1
    // should not append messages to slots 2, 3
    ext_buffer_range_1.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |_| (false, 0),
        check_skip_msg,
    );
    println!(
        "\r\n5.3: ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );
    println!(
        "5.3: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then try to append externals from range 2
    // should fully fill slot 6
    // should not fully fill slots 7, 8
    // should append messages to slot 4
    // should not append messages to slots 2, 3, 5
    ext_buffer_range_2.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |account_id| (ext_buffer_range_1.msgs.contains_key(account_id), 1),
        check_skip_msg,
    );
    println!(
        "\r\n5.4: ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );
    println!(
        "5.4: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // 6. ==========
    // refill partition 0 buffer
    let (refilled_int_buffer_par_0, _, _, _) =
        fill_test_buffers(ShardIdent::MASTERCHAIN, dst_shard);
    int_buffer_par_0 = refilled_int_buffer_par_0;
    timer.print_elapsed_and_restart();

    // fill next message group
    msg_group = MessageGroup::default();

    // first fill group with internals from partition 0
    // should fully fill 5 normal slots
    int_buffer_par_0.fill_message_group(
        &mut msg_group,
        group_limit - 3,
        group_vert_size,
        |_| (false, 0),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n6.1: int_buffer_par_0: {:?}",
        DebugMessagesBuffer(&int_buffer_par_0)
    );
    println!(
        "6.1: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append internals from the low-priority partition 1
    // should fully fill slot 6 for account 2
    int_buffer_par_1.fill_message_group(
        &mut msg_group,
        group_limit - 2,
        group_vert_size,
        |account_id| (int_buffer_par_0.msgs.contains_key(account_id), 1),
        do_not_check_skip_msg,
    );
    println!(
        "\r\n6.2: int_buffer_par_1: {:?}",
        DebugMessagesBuffer(&int_buffer_par_1)
    );
    println!(
        "6.2: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then append externals from range 1
    // should append to slot 1
    ext_buffer_range_1.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |_| (false, 0),
        check_skip_msg,
    );
    println!(
        "\r\n6.3: ext_buffer_range_1: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_1)
    );
    println!(
        "6.3: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();

    // then try to append externals from range 2
    // should append to slot 4
    ext_buffer_range_2.fill_message_group(
        &mut msg_group,
        group_limit,
        group_vert_size + 1,
        |account_id| (ext_buffer_range_1.msgs.contains_key(account_id), 1),
        check_skip_msg,
    );
    println!(
        "\r\n6.4: ext_buffer_range_2: {:?}",
        DebugMessagesBuffer(&ext_buffer_range_2)
    );
    println!(
        "6.4: message_group: {:?}",
        DebugMessageGroupDetailed(&msg_group)
    );
    timer.print_elapsed_and_restart();
}
