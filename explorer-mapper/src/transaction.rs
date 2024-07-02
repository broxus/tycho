use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::cell::CellBuilder;
use everscale_types::models::{
    AccountStatus, BlockId, ComputePhase, Message, MsgInfo, OrdinaryTxInfo, Transaction, TxInfo,
};
use explorer_models::schema::sql_types::{State, TransactionType};
use explorer_models::{
    AccountUpdate, CreatorInfo, ProcessingContext, RawTransaction, Transaction as DbTransaction,
};

use crate::message::process_message;

pub fn process_transaction(
    ctx: &mut ProcessingContext,
    account_update: &mut AccountUpdate,
    block_id: &BlockId,
    tx: &Transaction,
) -> Result<()> {
    let tx_cell = CellBuilder::build_from(tx)?;
    let hash = *tx_cell.repr_hash();

    let description = tx.load_info()?;

    let mut balance_change = 0;

    let in_message = tx.load_in_msg()?;
    if let Some(in_msg) = &in_message {
        if let MsgInfo::Int(info) = &in_msg.info {
            balance_change += info.value.tokens.into_inner() as i128;
        }
    }

    for out_msg in tx.out_msgs.values() {
        let out_msg = out_msg?;
        let msg: Message<'_> = out_msg.parse()?;
        if let MsgInfo::Int(info) = msg.info {
            balance_change -= info.value.tokens.into_inner() as i128;
        }
    }

    let (tx_type, aborted, exit_code, result_code) = match &description {
        TxInfo::Ordinary(info) => {
            let exit_code = match &info.compute_phase {
                ComputePhase::Skipped(_) => None,
                ComputePhase::Executed(res) => Some(res.exit_code),
            };
            let result_code = info.action_phase.as_ref().map(|phase| phase.result_code);
            balance_change -= compute_total_transaction_fees(tx, info) as i128;
            (
                TransactionType::Ordinary,
                info.aborted,
                exit_code,
                result_code,
            )
        }
        TxInfo::TickTock(tt) => {
            let exit_code = match &tt.compute_phase {
                ComputePhase::Skipped(_) => None,
                ComputePhase::Executed(res) => Some(res.exit_code),
            };
            let result_code = tt.action_phase.as_ref().map(|phase| phase.result_code);
            (
                TransactionType::TickTock,
                tt.aborted,
                exit_code,
                result_code,
            )
        }
    };

    let raw_transaction = RawTransaction {
        wc: block_id.shard.workchain() as i8,
        account_id: tx.account.0.into(),
        lt: tx.lt,
        data: Boc::encode(&tx_cell),
    };

    ctx.raw_transactions.push(raw_transaction);
    ctx.transactions.push(DbTransaction {
        workchain: block_id.shard.workchain() as i8,
        account_id: tx.account.0.into(),
        lt: tx.lt,
        time: tx.now,
        hash: hash.0.into(),
        block_shard: block_id.shard.prefix(),
        block_seqno: block_id.seqno,
        block_hash: block_id.root_hash.0.into(),
        tx_type,

        aborted,
        balance_change: balance_change.try_into().unwrap_or({
            if balance_change < 0 {
                i64::MIN
            } else {
                i64::MAX
            }
        }),
        exit_code,
        result_code,
    });

    let account = tx.account.0.into();
    let wc = block_id.shard.workchain() as i8;

    if let Some(ref in_msg) = in_message {
        process_message(
            ctx,
            block_id.root_hash.0.into(),
            hash.0.into(),
            false,
            0,
            in_msg,
            account,
            tx.lt,
            wc,
            tx.now,
        )
        .context("Failed to process transaction in_msg")?;
    }

    for (i, out_msg) in tx.out_msgs.values().enumerate() {
        let out_msg = out_msg?;
        let msg: Message<'_> = out_msg.parse()?;
        process_message(
            ctx,
            block_id.root_hash.0.into(),
            hash.0.into(),
            true,
            i as u16,
            &msg,
            account,
            tx.lt,
            wc,
            tx.now,
        )?;
    }

    account_update.last_transaction_time = tx.now;
    account_update.last_transaction_lt = tx.lt;

    account_update.state = match (&tx.orig_status, &tx.end_status) {
        (AccountStatus::NotExists, AccountStatus::NotExists)
            if account_update.state != State::Deleted =>
        {
            State::NonExist
        }
        (_, AccountStatus::NotExists) => State::Deleted,
        (_, AccountStatus::Uninit) => State::Uninit,
        (_, AccountStatus::Active) => State::Active,
        (_, AccountStatus::Frozen) => State::Frozen,
    };

    if tx.orig_status == AccountStatus::NotExists
        && tx.end_status != AccountStatus::NotExists
        && account_update.creator.is_none()
    {
        let creator = tx.load_in_msg()?.and_then(|msg| match &msg.info {
            MsgInfo::Int(info) => Some(info.src.clone()),
            _ => None,
        });

        if let Some(address) = creator {
            let addr = address
                .as_std()
                .context("Failed to convert address")?
                .clone();
            account_update.creator = Some(CreatorInfo {
                created_at: tx.now,
                creator_address: addr.address.0.into(),
                creator_wc: addr.workchain,
            });
        }
    }

    Ok(())
}

pub fn compute_total_transaction_fees(
    transaction: &Transaction,
    description: &OrdinaryTxInfo,
) -> u128 {
    let mut total_fees = transaction.total_fees.tokens.into_inner();
    if let Some(phase) = &description.action_phase {
        total_fees += phase
            .total_fwd_fees
            .as_ref()
            .map(|tokens| tokens.into_inner())
            .unwrap_or_default();
        total_fees -= phase
            .total_action_fees
            .as_ref()
            .map(|tokens| tokens.into_inner())
            .unwrap_or_default();
    };
    total_fees
}
