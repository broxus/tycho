use anyhow::Result;
use everscale_types::cell::CellBuilder;
use everscale_types::models::{IntAddr, Message, MsgInfo};
use explorer_models::schema::sql_types::MessageType;
use explorer_models::{
    Hash, Message as MessageDb, MessageInfo, ProcessingContext, TransactionMessage,
};

#[allow(clippy::too_many_arguments)]
pub fn process_message(
    ctx: &mut ProcessingContext,
    block_hash: Hash,
    transaction_hash: Hash,
    is_out: bool,
    index_in_transaction: u16,
    message: &Message<'_>,
    account: Hash,
    lt: u64,
    wc: i8,
    transaction_time: u32,
) -> Result<()> {
    let msg_cell = CellBuilder::build_from(message)?;
    let msg_hash = msg_cell.repr_hash().0.into();

    let (src, dst, message_type) = match &message.info {
        MsgInfo::Int(i) => (map_addr(&i.src)?, map_addr(&i.dst)?, MessageType::Internal),
        MsgInfo::ExtIn(m) => ((None, 0), map_addr(&m.dst)?, MessageType::ExternalIn),
        MsgInfo::ExtOut(m) => (map_addr(&m.src)?, (None, 0), MessageType::ExternalOut),
    };

    let info = message_info(message);

    ctx.messages.push(MessageDb {
        message_hash: msg_hash,
        src_workchain: src.1,
        src_address: src.0,
        dst_workchain: dst.1,
        dst_address: dst.0,
        message_type,
        message_value: info.value,
        ihr_fee: info.ihr_fee,
        fwd_fee: info.fwd_fee,
        import_fee: info.import_fee,
        created_lt: info.created_lt,
        created_at: info.created_at,
        bounced: info.bounced,
        bounce: info.bounce,
    });

    ctx.transaction_messages.push(TransactionMessage {
        transaction_hash,
        index_in_transaction,
        is_out,
        transaction_lt: lt,
        transaction_account_workchain: wc,
        transaction_account_address: account,
        block_hash,
        dst_workchain: dst.1,
        dst_address: dst.0,
        src_workchain: src.1,
        src_address: src.0,
        message_hash: msg_hash,
        message_type,
        transaction_time,
        message_value: info.value,
        bounced: info.bounced,
        bounce: info.bounce,
    });

    Ok(())
}

fn message_info(message: &Message<'_>) -> MessageInfo {
    match &message.info {
        MsgInfo::Int(m) => MessageInfo {
            value: m.value.tokens.into_inner() as u64,
            ihr_fee: m.ihr_fee.into_inner() as u64,
            fwd_fee: m.fwd_fee.into_inner() as u64,
            bounce: m.bounce,
            bounced: m.bounced,
            created_lt: m.created_lt,
            created_at: m.created_at,
            ..Default::default()
        },
        MsgInfo::ExtIn(m) => MessageInfo {
            import_fee: m.import_fee.into_inner() as u64,
            ..Default::default()
        },
        MsgInfo::ExtOut(m) => MessageInfo {
            created_lt: m.created_lt,
            created_at: m.created_at,
            ..Default::default()
        },
    }
}

pub fn map_addr(addr: &IntAddr) -> Result<(Option<Hash>, i8)> {
    match addr {
        IntAddr::Std(add) => Ok((Some(add.address.0.into()), add.workchain)),
        IntAddr::Var(_) => anyhow::bail!("Unsupported address type"),
    }
}
