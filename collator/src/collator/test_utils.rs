use tycho_types::cell::CellBuilder;
use tycho_types::models::{IntAddr, IntMsgInfo, MsgInfo, ShardIdent};

use super::types::ParsedMessage;
use crate::internal_queue::types::EnqueuedMessage;

pub fn make_stub_internal_parsed_message(
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

#[cfg(test)]
pub fn make_stub_external_parsed_message(
    anchor_id: crate::mempool::MempoolAnchorId,
    chain_time: u64,
    msg_idx: u32,
    dst: IntAddr,
) -> Box<ParsedMessage> {
    let ext_msg = crate::mempool::make_stub_external(anchor_id, chain_time, msg_idx, dst);
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
