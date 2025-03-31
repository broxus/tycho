use std::str::FromStr;
use std::vec;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::Json;
use everscale_types::boc::Boc;
use everscale_types::cell::CellBuilder;
use everscale_types::crc::crc_16;
use everscale_types::models::{
    AccountState, IntAddr, MsgInfo, OwnedMessage, StateInit, StdAddr, Transaction,
};
use num_bigint::BigInt;
use requests::{AddressInformationQuery, ExecMethodArgs, GetTransactionsQuery, SendMessageRequest};
use responses::{
    parse_tvm_stack, status_to_string, AddressInformation, AddressResponse, BlockId,
    TonCenterResponse, TransactionId, TransactionResponse, TransactionsResponse, TvmStackRecord,
};
use tycho_vm::{GasParams, NaN, OwnedCellSlice, RcStackValue, SmcInfoBase, VmState};

use crate::endpoint::error::{Error, Result};
use crate::state::LoadedAccountState;
use crate::RpcState;

mod requests;
mod responses;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .route("/getAddressInformation", get(get_address_information))
        .route("/getTransactions", get(get_blockchain_account_transactions))
        .route("/runGetMethod", get(exec_get_method_for_blockchain_account))
        .route("/sendBoc", post(send_blockchain_message))
}

async fn get_address_information(
    Query(address): Query<AddressInformationQuery>,
    State(rpc_state): State<RpcState>,
) -> Result<Json<TonCenterResponse<AddressInformation>>> {
    let Ok(item) = rpc_state.get_account_state(&address.address) else {
        return Ok(Json(TonCenterResponse {
            ok: false,
            result: None,
            error: Some("account not found".to_string()),
            code: None,
        }));
    };

    match &item {
        &LoadedAccountState::NotFound { .. } => Err(Error::NotFound("account not found")),
        LoadedAccountState::Found { state, .. } => {
            let Ok(account) = state.load_account() else {
                return Ok(Json(TonCenterResponse {
                    ok: false,
                    result: None,
                    error: Some("account not found".to_string()),
                    code: None,
                }));
            };
            match account {
                Some(loaded) => {
                    let status = loaded.state.status();
                    let (code, data, frozen_hash) = match loaded.state {
                        AccountState::Active(StateInit { code, data, .. }) => {
                            (code.map(Boc::encode_hex), data.map(Boc::encode_hex), None)
                        }
                        AccountState::Uninit => (None, None, None),
                        AccountState::Frozen(hash_bytes) => {
                            (None, None, Some(hash_bytes.to_string()))
                        }
                    };

                    let list = rpc_state.get_transactions(&address.address, None)?;

                    let last_transaction_hash = list
                        .map(|item| Boc::decode(item).ok().map(|root| *root.repr_hash()))
                        .last()
                        .flatten();

                    let block_id = if let Some(hash) = last_transaction_hash {
                        let block_id = rpc_state.get_transaction_block_id(&hash)?.unwrap();
                        Some(BlockId {
                            type_field: "ton.blockIdExt".to_string(),
                            workchain: block_id.shard.workchain(),
                            shard: block_id.shard.prefix().to_string(),
                            seqno: block_id.seqno,
                            root_hash: block_id.root_hash.to_string(),
                            file_hash: block_id.file_hash.to_string(),
                        })
                    } else {
                        None
                    };

                    Ok(Json(TonCenterResponse {
                        ok: true,
                        result: Some(AddressInformation {
                            type_field: "raw.fullAccountState".to_string(),
                            balance: loaded.balance.tokens.into_inner() as u64,
                            code,
                            data,
                            last_transaction_id: TransactionId {
                                type_field: "internal.transactionId".to_string(),
                                lt: loaded.last_trans_lt.to_string(),
                                hash: Some(state.last_trans_hash.to_string()),
                            },
                            block_id,
                            frozen_hash,
                            sync_utime: 0, // TODO: fix sync utime
                            extra: "1739453311.547493:11:0.8618085632029536".to_string(), /* TODO: fix extra */
                            state: status_to_string(status),
                        }),
                        error: None,
                        code: None,
                    }))
                }
                None => Ok(Json(TonCenterResponse {
                    ok: false,
                    result: None,
                    error: Some("account not found".to_string()),
                    code: None,
                })),
            }
        }
    }
}

async fn get_blockchain_account_transactions(
    Path(address): Path<StdAddr>,
    Query(pagination): Query<GetTransactionsQuery>,
    State(state): State<RpcState>,
) -> Result<Json<TonCenterResponse<Vec<TransactionResponse>>>> {
    let limit = pagination
        .limit
        .unwrap_or(TransactionsResponse::DEFAULT_LIMIT);

    if limit == 0 {
        return Ok(Json(TonCenterResponse {
            ok: true,
            result: Some(vec![]),
            error: None,
            code: None,
        }));
    }

    if limit > TransactionsResponse::MAX_LIMIT {
        return Err(Error::BadRequest("limit is too large"));
    }

    let list = state.get_transactions(&address, pagination.lt)?;
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

            Ok(TransactionResponse {
                type_field: "raw.transaction".to_string(),
                address: AddressResponse {
                    type_field: "accountAddress".to_string(),
                    account_address: pagination.address.to_string(),
                },
                data: Boc::encode_hex(root),
                transaction_id: TransactionId {
                    type_field: "internal.transactionId".to_string(),
                    lt: t.lt.to_string(),
                    hash: Some(hash.to_string()),
                },
                fee: (t.total_fees.tokens.into_inner() as u64).to_string(), // CHECK
                storage_fee: 0.to_string(), // TODO: fill with correct storage_fee
                other_fee: 0.to_string(),   // TODO: fill with correct other_fee
                utime: t.now,
                in_msg,
                out_msgs,
            }) as Result<TransactionResponse, Error>
        })
        .take(limit as _)
        .collect::<Result<Vec<TransactionResponse>, _>>()?;

    Ok(Json(TonCenterResponse {
        ok: true,
        result: Some(transactions),
        error: None,
        code: None,
    }))
}

async fn exec_get_method_for_blockchain_account(
    Query(args): Query<ExecMethodArgs>,
    State(state): State<RpcState>,
) -> Result<Json<TonCenterResponse<Vec<TvmStackRecord>>>> {
    let item = state.get_account_state(&args.address)?;

    match &item {
        &LoadedAccountState::NotFound { .. } => Ok(Json(TonCenterResponse {
            ok: false,
            result: None,
            error: Some("account not found".to_string()),
            code: None,
        })),
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
                                .with_account_addr(IntAddr::Std(args.address.clone()))
                                .require_ton_v4();

                            let crc = crc_16(args.method.as_bytes());
                            let method_id = crc as u32 | 0x10000;

                            let mut stack = vec![RcStackValue::new_dyn_value(
                                OwnedCellSlice::new_allow_exotic(CellBuilder::build_from(
                                    &args.address,
                                )?),
                            )];

                            for args_stack in args.stack {
                                for arg in args_stack {
                                    // TODO: check args
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
                                            OwnedCellSlice::new_allow_exotic(
                                                CellBuilder::build_from(&address)?,
                                            ),
                                        ));
                                        continue;
                                    }
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
                            let success = exit_code == 0;
                            let stack = vm_state.stack;

                            let result = parse_tvm_stack(stack).ok();

                            Ok(Json(TonCenterResponse {
                                ok: success,
                                result,
                                error: None,
                                code: None,
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
