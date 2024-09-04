use std::borrow::Cow;
use std::sync::OnceLock;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use everscale_types::boc::BocRepr;
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use everscale_types::prelude::*;

use self::protos::rpc::{self, request, response, Request};
use crate::endpoint::proto::extractor::{
    ProtoErrorResponse, ProtoOkResponse, Protobuf, ProtobufRef,
};
use crate::endpoint::{
    INTERNAL_ERROR_CODE, INVALID_ADDRESS, INVALID_BOC_CODE, METHOD_NOT_FOUND_CODE, NOT_READY_CODE,
    NOT_SUPPORTED_CODE, TOO_LARGE_LIMIT_CODE,
};
use crate::state::{LoadedAccountState, RpcState, RpcStateError};

mod extractor;
mod protos;

pub async fn route(State(state): State<RpcState>, Protobuf(req): Protobuf<Request>) -> Response {
    match req.call {
        Some(request::Call::GetCapabilities(())) => {
            let result = get_capabilities(&state);
            (StatusCode::OK, ProtobufRef(result)).into_response()
        }
        Some(request::Call::GetLatestKeyBlock(())) => match &*state.load_latest_key_block_proto() {
            Some(config) => {
                let result = response::Result::GetLatestKeyBlock(response::GetLatestKeyBlock {
                    block: config.as_ref().clone(),
                });
                ok_to_response(result)
            }
            None => error_to_response(RpcStateError::NotReady),
        },
        Some(request::Call::GetBlockchainConfig(())) => {
            match &*state.load_blockchain_config_proto() {
                Some(config) => {
                    let result =
                        response::Result::GetBlockchainConfig(response::GetBlockchainConfig {
                            global_id: config.global_id,
                            seqno: config.seqno,
                            config: config.config.clone(),
                        });
                    ok_to_response(result)
                }
                None => error_to_response(RpcStateError::NotReady),
            }
        }
        Some(request::Call::GetStatus(())) => {
            let result = response::Result::GetStatus(response::GetStatus {
                ready: state.is_ready(),
            });
            ok_to_response(result)
        }
        Some(request::Call::GetTimings(())) => {
            if state.is_ready() {
                let timings = state.load_timings();
                let result = response::Result::GetTimings(response::GetTimings {
                    last_mc_block_seqno: timings.last_mc_block_seqno,
                    last_mc_utime: timings.last_mc_utime,
                    mc_time_diff: timings.mc_time_diff,
                    smallest_known_lt: timings.smallest_known_lt,
                });
                ok_to_response(result)
            } else {
                error_to_response(RpcStateError::NotReady)
            }
        }
        Some(request::Call::SendMessage(p)) => {
            if let Err(e) = BocRepr::decode::<Box<OwnedMessage>, _>(&p.message) {
                return ProtoErrorResponse {
                    code: INVALID_BOC_CODE,
                    message: e.to_string().into(),
                }
                .into_response();
            };
            state.broadcast_external_message(&p.message).await;
            ok_to_response(response::Result::SendMessage(()))
        }
        Some(request::Call::GetContractState(p)) => {
            let address = match BocRepr::decode::<StdAddr, _>(&p.address) {
                Ok(addr) => addr,
                Err(e) => {
                    return ProtoErrorResponse {
                        code: INVALID_ADDRESS,
                        message: e.to_string().into(),
                    }
                    .into_response()
                }
            };

            let item = match state.get_account_state(&address) {
                Ok(item) => item,
                Err(e) => return error_to_response(e),
            };

            let response = match &item {
                &LoadedAccountState::NotFound { timings } => response::GetContractState {
                    state: Some(response::get_contract_state::State::NotExists(
                        response::get_contract_state::NotExist {
                            gen_timings: Some(
                                response::get_contract_state::not_exist::GenTimings::Known(
                                    response::get_contract_state::Timings {
                                        gen_lt: timings.gen_lt,
                                        gen_utime: timings.gen_utime,
                                    },
                                ),
                            ),
                        },
                    )),
                },
                LoadedAccountState::Found {
                    state, gen_utime, ..
                } if Some(state.last_trans_lt) <= p.last_transaction_lt => {
                    response::GetContractState {
                        state: Some(response::get_contract_state::State::Unchanged(
                            response::get_contract_state::Timings {
                                gen_lt: state.last_trans_lt,
                                gen_utime: *gen_utime,
                            },
                        )),
                    }
                }
                LoadedAccountState::Found {
                    state, gen_utime, ..
                } => {
                    let timings = response::get_contract_state::Timings {
                        gen_lt: state.last_trans_lt,
                        gen_utime: *gen_utime,
                    };

                    let state = match state.load_account() {
                        Ok(Some(loaded)) => {
                            let account = match serialize_account(&loaded) {
                                Ok(account) => account,
                                Err(e) => {
                                    return error_to_response(RpcStateError::Internal(e.into()))
                                }
                            };
                            let last_transaction_id =
                                response::get_contract_state::exists::LastTransactionId::Exact(
                                    response::get_contract_state::exists::Exact {
                                        lt: state.last_trans_lt,
                                        hash: Bytes::copy_from_slice(
                                            state.last_trans_hash.as_slice(),
                                        ),
                                    },
                                );
                            response::get_contract_state::State::Exists(
                                response::get_contract_state::Exists {
                                    account,
                                    gen_timings: Some(timings),
                                    last_transaction_id: Some(last_transaction_id),
                                },
                            )
                        }
                        Ok(None) => response::get_contract_state::State::NotExists(
                            response::get_contract_state::NotExist {
                                gen_timings: Some(
                                    response::get_contract_state::not_exist::GenTimings::Known(
                                        response::get_contract_state::Timings {
                                            gen_lt: timings.gen_lt,
                                            gen_utime: timings.gen_utime,
                                        },
                                    ),
                                ),
                            },
                        ),
                        Err(e) => return error_to_response(RpcStateError::Internal(e.into())),
                    };

                    response::GetContractState { state: Some(state) }
                }
            };

            ok_to_response(response::Result::GetContractState(response))
        }
        Some(request::Call::GetAccountsByCodeHash(p)) => {
            if p.limit == 0 {
                let result = response::Result::GetAccounts(response::GetAccountsByCodeHash {
                    account: vec![],
                });
                return ok_to_response(result);
            } else if p.limit > MAX_LIMIT {
                return too_large_limit_response();
            }

            let code_hash = HashBytes::from_slice(p.code_hash.as_ref());
            let continuation = match p
                .continuation
                .map(|x| BocRepr::decode::<StdAddr, _>(&x))
                .transpose()
            {
                Ok(continuation) => continuation,
                Err(e) => {
                    return ProtoErrorResponse {
                        code: INVALID_ADDRESS,
                        message: e.to_string().into(),
                    }
                    .into_response()
                }
            };

            match state.get_raw_accounts_by_code_hash(&code_hash, continuation.as_ref()) {
                Ok(list) => {
                    let result = response::Result::GetAccounts(response::GetAccountsByCodeHash {
                        account: list.take(p.limit as usize).collect(),
                    });
                    ok_to_response(result)
                }
                Err(e) => error_to_response(e),
            }
        }
        Some(request::Call::GetTransactionsList(p)) => {
            if p.limit == 0 {
                let result = response::Result::GetTransactionsList(response::GetTransactionsList {
                    transactions: vec![],
                });
                return ok_to_response(result);
            } else if p.limit > MAX_LIMIT {
                return too_large_limit_response();
            }

            let account = match BocRepr::decode::<StdAddr, _>(&p.account) {
                Ok(addr) => addr,
                Err(e) => {
                    return ProtoErrorResponse {
                        code: INVALID_ADDRESS,
                        message: e.to_string().into(),
                    }
                    .into_response()
                }
            };

            match state.get_transactions(&account, p.last_transaction_lt) {
                Ok(list) => {
                    let transactions = list
                        .map(Bytes::copy_from_slice)
                        .take(p.limit as usize)
                        .collect();
                    let result =
                        response::Result::GetTransactionsList(response::GetTransactionsList {
                            transactions,
                        });
                    ok_to_response(result)
                }
                Err(e) => error_to_response(e),
            }
        }
        Some(request::Call::GetTransaction(p)) => {
            let hash = HashBytes::from_slice(&p.id);
            match state.get_transaction(&hash) {
                Ok(tx) => {
                    let result = response::Result::GetRawTransaction(response::GetRawTransaction {
                        transaction: tx.map(|slice| Bytes::copy_from_slice(slice.as_ref())),
                    });
                    ok_to_response(result)
                }
                Err(e) => error_to_response(e),
            }
        }
        Some(request::Call::GetDstTransaction(p)) => {
            let hash = HashBytes::from_slice(&p.message_hash);
            match state.get_dst_transaction(&hash) {
                Ok(tx) => {
                    let result = response::Result::GetRawTransaction(response::GetRawTransaction {
                        transaction: tx.map(|slice| Bytes::copy_from_slice(slice.as_ref())),
                    });
                    ok_to_response(result)
                }
                Err(e) => error_to_response(e),
            }
        }
        None => ProtoErrorResponse {
            code: METHOD_NOT_FOUND_CODE,
            message: "unknown method".into(),
        }
        .into_response(),
    }
}

// NOTE: `RpcState` full/not-full state is determined only once at startup,
// so it is ok to cache the response.
fn get_capabilities(state: &RpcState) -> &'static rpc::Response {
    static RESULT: OnceLock<Box<rpc::Response>> = OnceLock::new();
    RESULT.get_or_init(|| {
        let mut capabilities = vec![
            "getCapabilities",
            "getLatestKeyBlock",
            "getBlockchainConfig",
            "getStatus",
            "getTimings",
            "getContractState",
            "sendMessage",
        ];

        if state.is_full() {
            capabilities.extend([
                "getTransactionsList",
                "getTransaction",
                "getDstTransaction",
                "getAccountsByCodeHash",
            ]);
        }

        Box::new(rpc::Response {
            result: Some(response::Result::GetCapabilities(
                response::GetCapabilities {
                    capabilities: capabilities.into_iter().map(|s| s.into()).collect(),
                },
            )),
        })
    })
}

fn ok_to_response(result: response::Result) -> Response {
    ProtoOkResponse::new(result).into_response()
}

fn error_to_response(e: RpcStateError) -> Response {
    let (code, message) = match e {
        RpcStateError::NotReady => (NOT_READY_CODE, Cow::Borrowed("not ready")),
        RpcStateError::NotSupported => (NOT_SUPPORTED_CODE, Cow::Borrowed("method not supported")),
        RpcStateError::Internal(e) => (INTERNAL_ERROR_CODE, e.to_string().into()),
    };

    ProtoErrorResponse { code, message }.into_response()
}

fn too_large_limit_response() -> Response {
    ProtoErrorResponse {
        code: TOO_LARGE_LIMIT_CODE,
        message: Cow::Borrowed("limit is too large"),
    }
    .into_response()
}

fn serialize_account(account: &Account) -> Result<Bytes, everscale_types::error::Error> {
    let cx = &mut Cell::empty_context();
    let mut builder = CellBuilder::new();
    account.address.store_into(&mut builder, cx)?;
    account.storage_stat.store_into(&mut builder, cx)?;
    account.last_trans_lt.store_into(&mut builder, cx)?;
    account.balance.store_into(&mut builder, cx)?;
    account.state.store_into(&mut builder, cx)?;
    if account.init_code_hash.is_some() {
        account.init_code_hash.store_into(&mut builder, cx)?;
    }
    let cell = builder.build_ext(cx)?;
    Ok(Boc::encode(cell).into())
}

const MAX_LIMIT: u32 = 100;
