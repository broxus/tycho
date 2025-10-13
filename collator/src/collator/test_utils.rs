use tycho_types::cell::CellBuilder;
use tycho_types::models::{IntAddr, IntMsgInfo, MsgInfo, ShardIdent};

use super::types::ParsedMessage;
use crate::internal_queue::types::message::EnqueuedMessage;

pub fn make_stub_internal_parsed_message(
    src_shard: ShardIdent,
    dst: IntAddr,
    created_lt: u64,
    is_new: bool,
) -> ParsedMessage {
    let dst_wc = dst.workchain();
    let info = IntMsgInfo {
        dst,
        created_lt,
        ..Default::default()
    };
    let cell = CellBuilder::build_from(&info).unwrap();
    let enq_msg = EnqueuedMessage { info, cell };
    ParsedMessage::new(
        MsgInfo::Int(enq_msg.info),
        true,
        enq_msg.cell,
        None,
        None,
        (!is_new).then(|| dst_wc == src_shard.workchain()),
        None,
    )
}

#[cfg(test)]
pub fn make_stub_external_parsed_message(
    anchor_id: crate::mempool::MempoolAnchorId,
    chain_time: u64,
    msg_idx: u32,
    dst: IntAddr,
) -> ParsedMessage {
    let ext_msg = crate::mempool::make_stub_external(anchor_id, chain_time, msg_idx, dst);
    ParsedMessage::new(
        MsgInfo::ExtIn(ext_msg.info),
        true,
        ext_msg.cell,
        None,
        None,
        None,
        Some(chain_time),
    )
}
