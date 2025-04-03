use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::Json;
use everscale_types::boc::Boc;
use everscale_types::cell::CellBuilder;
use everscale_types::crc::crc_16;
use everscale_types::models::{
    AccountState, ComputePhase, IntAddr, MsgInfo, OwnedMessage, StateInit, StdAddr, Transaction,
    TxInfo,
};
use num_bigint::BigInt;
use tycho_vm::{GasParams, NaN, OwnedCellSlice, RcStackValue, SmcInfoBase, VmState};

use super::requests::{ExecMethodArgs, Pagination, SendMessageRequest};
use super::responses::{
    bounce_phase_to_string, parse_tvm_stack, status_to_string, AccountResponse,
    ExecGetMethodResponse, LibraryResponse, TransactionAccountResponse, TransactionResponse,
    TransactionsResponse,
};
use crate::endpoint::error::{Error, Result};
use crate::state::LoadedAccountState;
use crate::RpcState;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .route("/accounts/:account", get(get_blockchain_raw_account))
        .route(
            "/accounts/:account/transactions",
            get(get_blockchain_account_transactions),
        )
        .route(
            "/accounts/:account/methods/:method_name",
            get(exec_get_method_for_blockchain_account),
        )
        .route("/message", post(send_blockchain_message))
}

async fn get_blockchain_raw_account(
    Path(address): Path<StdAddr>,
    State(state): State<RpcState>,
) -> Result<Json<AccountResponse>> {
    let item = state.get_account_state(&address)?;

    match &item {
        &LoadedAccountState::NotFound { .. } => Err(Error::NotFound("account not found")),
        LoadedAccountState::Found { state, .. } => {
            let account = state.load_account()?;
            match account {
                Some(loaded) => {
                    let status = loaded.state.status();
                    let (code, data, libraries, frozen_hash) = match loaded.state {
                        AccountState::Active(StateInit {
                            code,
                            data,
                            libraries,
                            ..
                        }) => (
                            code.map(Boc::encode_hex),
                            data.map(Boc::encode_hex),
                            Some(
                                libraries
                                    .iter()
                                    .filter_map(|res| res.ok())
                                    .map(|(_, lib)| LibraryResponse {
                                        public: lib.public,
                                        root: Boc::encode_hex(lib.root),
                                    })
                                    .collect(),
                            ),
                            None,
                        ),
                        AccountState::Uninit => (None, None, None, None),
                        AccountState::Frozen(hash_bytes) => {
                            (None, None, None, Some(hash_bytes.to_string()))
                        }
                    };

                    Ok(Json(AccountResponse {
                        address: address.to_string(),
                        balance: loaded.balance.tokens.into_inner() as u64,
                        extra_balance: None, // TODO: fill with correct extra balance
                        status: status_to_string(status),
                        code,
                        data,
                        last_transaction_lt: loaded.last_trans_lt,
                        last_transaction_hash: Some(state.last_trans_hash.to_string()),
                        frozen_hash,
                        libraries,
                        storage: loaded.storage_stat.into(),
                    }))
                }
                None => Err(Error::NotFound("account not found")),
            }
        }
    }
}

async fn get_blockchain_account_transactions(
    Path(address): Path<StdAddr>,
    Query(pagination): Query<Pagination>,
    State(state): State<RpcState>,
) -> Result<Json<TransactionsResponse>> {
    let limit = pagination.limit.unwrap_or(TransactionsResponse::MAX_LIMIT);

    if limit == 0 {
        return Ok(Json(TransactionsResponse {
            transactions: vec![],
        }));
    }

    if limit > TransactionsResponse::MAX_LIMIT {
        return Err(Error::BadRequest("limit is too large"));
    }

    let list = state.get_transactions(&address, pagination.after_lt)?;
    let transactions = list
        .map(|item| {
            let root = Boc::decode(item).map_err(|e| anyhow::format_err!(e.to_string()))?;
            let hash = *root.repr_hash();
            let t = root.parse::<Transaction>()?;

            let in_msg = if let Some(in_msg) = &t.in_msg {
                let hash = *in_msg.repr_hash();
                let in_msg = in_msg.parse::<OwnedMessage>()?;
                Some((in_msg.info, Some(in_msg.body.0), hash).into())
            } else {
                None
            };

            let mut out_msgs = vec![];

            for out_msg_cell in t.out_msgs.values() {
                let out_msg_cell = out_msg_cell?;
                let out_msg_hash = *out_msg_cell.repr_hash();
                let out_msg_info = out_msg_cell.parse::<MsgInfo>()?;
                out_msgs.push((out_msg_info, None, out_msg_hash).into());
            }

            let info = t.load_info()?;

            let transaction_type;
            let compute_phase;
            let credit_phase;
            let storage_phase;
            let action_phase;
            let aborted;
            let bounce_phase;
            let destroyed;
            let mut success = true;

            match info {
                TxInfo::Ordinary(ordinary_tx_info) => {
                    transaction_type = "TransOrd".to_string();

                    compute_phase = match ordinary_tx_info.compute_phase {
                        ComputePhase::Skipped(skipped_compute_phase) => {
                            Some(skipped_compute_phase.into())
                        }
                        ComputePhase::Executed(executed_compute_phase) => {
                            success = executed_compute_phase.success;
                            Some(executed_compute_phase.into())
                        }
                    };
                    storage_phase = ordinary_tx_info.storage_phase.map(From::from);
                    credit_phase = ordinary_tx_info.credit_phase.map(From::from);
                    action_phase = ordinary_tx_info.action_phase.map(From::from);
                    aborted = ordinary_tx_info.aborted;
                    bounce_phase = ordinary_tx_info.bounce_phase.map(bounce_phase_to_string);
                    destroyed = ordinary_tx_info.destroyed;
                }
                TxInfo::TickTock(tick_tock_tx_info) => {
                    transaction_type = "TransTickTock".to_string();
                    compute_phase = match tick_tock_tx_info.compute_phase {
                        ComputePhase::Skipped(skipped_compute_phase) => {
                            Some(skipped_compute_phase.into())
                        }
                        ComputePhase::Executed(executed_compute_phase) => {
                            success = executed_compute_phase.success;
                            Some(executed_compute_phase.into())
                        }
                    };
                    storage_phase = Some(tick_tock_tx_info.storage_phase.into());
                    credit_phase = None;
                    action_phase = tick_tock_tx_info.action_phase.map(From::from);
                    aborted = tick_tock_tx_info.aborted;
                    bounce_phase = None;
                    destroyed = tick_tock_tx_info.destroyed;
                }
            }

            let state_update = t.state_update.load()?;

            let block_id = state.get_transaction_block_id(&hash)?;

            Ok(TransactionResponse {
                hash: hash.to_string(),
                lt: t.lt,
                account: TransactionAccountResponse {
                    address: address.to_string(),
                    name: None,
                    is_scam: false, // TODO: fill with correct is_scam
                    icon: None,
                    is_wallet: true, // TODO: fill with correct is_wallet
                },
                success,
                utime: t.now,
                orig_status: status_to_string(t.orig_status),
                end_status: status_to_string(t.end_status),
                total_fees: t.total_fees.tokens.into_inner() as u64,
                end_balance: 0, // TODO: fill with correct end_balance
                transaction_type,
                state_update_old: state_update.old.to_string(),
                state_update_new: state_update.new.to_string(),
                in_msg,
                out_msgs,
                block: block_id.map(|b| {
                    format!(
                        "({},{},{:016x})",
                        b.shard.workchain(),
                        b.seqno,
                        b.shard.prefix()
                    )
                }),
                prev_trans_hash: Some(t.prev_trans_hash.to_string()),
                prev_trans_lt: Some(t.prev_trans_lt),
                compute_phase,
                storage_phase,
                credit_phase,
                action_phase,
                bounce_phase,
                aborted,
                destroyed,
                raw: Boc::encode_hex(root),
            }) as Result<TransactionResponse, Error>
        })
        .take(limit as _)
        .collect::<Result<Vec<TransactionResponse>, _>>()?;

    Ok(Json(TransactionsResponse { transactions }))
}

async fn exec_get_method_for_blockchain_account(
    Path((address, method_name)): Path<(StdAddr, String)>,
    Query(args): Query<ExecMethodArgs>,
    State(state): State<RpcState>,
) -> Result<Json<ExecGetMethodResponse>> {
    let item = state.get_account_state(&address)?;

    match &item {
        &LoadedAccountState::NotFound { .. } => Err(Error::NotFound("account not found")),
        LoadedAccountState::Found { state, .. } => {
            let account = state.load_account()?;
            match account {
                Some(loaded) => {
                    match loaded.state {
                        AccountState::Active(StateInit { code, data, .. }) => {
                            let smc_info = SmcInfoBase::new()
                                // .with_now(1733142533) // TODO: check if needed?
                                // .with_block_lt(50899537000013) // TODO: check if needed?
                                // .with_tx_lt(50899537000013) // TODO: check if needed?
                                .with_account_balance(loaded.balance)
                                .with_account_addr(IntAddr::Std(address.clone()))
                                .require_ton_v4();

                            let crc = crc_16(method_name.as_bytes());
                            let method_id = crc as u32 | 0x10000;

                            let mut stack = vec![RcStackValue::new_dyn_value(
                                OwnedCellSlice::new_allow_exotic(CellBuilder::build_from(
                                    &address,
                                )?),
                            )];

                            for arg in args.args {
                                if arg == "NaN" {
                                    stack.push(RcStackValue::new_dyn_value(NaN));
                                    continue;
                                }

                                if arg == "Null" {
                                    stack.push(RcStackValue::new_dyn_value(()));
                                    continue;
                                }

                                if let Ok(v) = BigInt::from_str(&arg) {
                                    stack.push(RcStackValue::new_dyn_value(v));
                                    continue;
                                }

                                if let Ok(v) = hex::decode(&arg) {
                                    if let Some(v) = BigInt::parse_bytes(&v, 16) {
                                        stack.push(RcStackValue::new_dyn_value(v));
                                        continue;
                                    }
                                }

                                if let Ok(cell) = Boc::decode_base64(&arg) {
                                    stack.push(RcStackValue::new_dyn_value(
                                        OwnedCellSlice::new_allow_exotic(cell),
                                    ));
                                    continue;
                                }
                                if let Ok(cell) = Boc::decode(&arg) {
                                    stack.push(RcStackValue::new_dyn_value(
                                        OwnedCellSlice::new_allow_exotic(cell),
                                    ));
                                    continue;
                                }
                                if let Ok(address) = IntAddr::from_str(&arg) {
                                    stack.push(RcStackValue::new_dyn_value(
                                        OwnedCellSlice::new_allow_exotic(CellBuilder::build_from(
                                            &address,
                                        )?),
                                    ));
                                    continue;
                                }
                            }

                            stack.push(RcStackValue::new_dyn_value(BigInt::from(method_id)));

                            let mut vm_state = VmState::builder()
                                .with_smc_info(smc_info)
                                .with_stack(stack)
                                .with_code(code.unwrap_or_default())
                                .with_data(data.unwrap_or_default())
                                .with_gas(GasParams::getter())
                                .build();

                            let exit_code = vm_state.run();
                            let stack = vm_state.stack;

                            let stack = parse_tvm_stack(stack)?;

                            let success = exit_code == 0;

                            Ok(Json(ExecGetMethodResponse {
                                success,
                                exit_code,
                                stack,
                                decoded: None,
                            }))
                        }
                        _ => Err(Error::BadRequest("account has wrong state")),
                    }
                }
                None => Err(Error::NotFound("account not found")),
            }
        }
    }
}

async fn send_blockchain_message(
    State(state): State<RpcState>,
    Json(input): Json<SendMessageRequest>,
) -> Result<Json<()>> {
    let data =
        hex::decode(input.boc).map_err(|_e| Error::BadRequest("can not parse boc from hex"))?;
    state.broadcast_external_message(&data).await;
    Ok(Json(()))
}
