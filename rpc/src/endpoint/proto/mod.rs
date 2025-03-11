use std::borrow::Cow;
use std::sync::OnceLock;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tycho_block_util::message::validate_external_message;
use tycho_util::metrics::HistogramGuard;

pub use self::cache::ProtoEndpointCache;
use self::protos::rpc::{self, request, response, Request};
use super::INVALID_PARAMS_CODE;
use crate::endpoint::proto::extractor::{
    ProtoErrorResponse, ProtoOkResponse, Protobuf, ProtobufRef,
};
use crate::endpoint::proto::protos::rpc::response::GetLibraryCell;
use crate::endpoint::{
    INTERNAL_ERROR_CODE, INVALID_BOC_CODE, METHOD_NOT_FOUND_CODE, NOT_READY_CODE,
    NOT_SUPPORTED_CODE, TOO_LARGE_LIMIT_CODE,
};
use crate::state::{LoadedAccountState, RpcState, RpcStateError};

mod cache;
mod extractor;
mod protos;

macro_rules! declare_proto_methods {
    ($($name:ident),*$(,)?) => {
        trait RequestExt {
            fn method_name(&self) -> &'static str;
        }

        impl RequestExt for request::Call {
            fn method_name(&self) -> &'static str {
                match self {
                    $(request::Call::$name { .. } => stringify!($name)),*
                }
            }
        }
    };
}

declare_proto_methods! {
    GetCapabilities,
    GetLatestKeyBlock,
    GetBlockchainConfig,
    GetStatus,
    GetTimings,
    SendMessage,
    GetContractState,
    GetLibraryCell,
    GetAccountsByCodeHash,
    GetTransactionsList,
    GetTransaction,
    GetDstTransaction,
    GetTransactionBlockId,
}

pub async fn route(State(state): State<RpcState>, Protobuf(req): Protobuf<Request>) -> Response {
    let Some(call) = req.call else {
        return ProtoErrorResponse {
            code: METHOD_NOT_FOUND_CODE,
            message: "unknown method".into(),
        }
        .into_response();
    };

    let label = [("method", call.method_name())];
    let _hist = HistogramGuard::begin_with_labels("tycho_jrpc_request_time", &label);
    match call {
        request::Call::GetCapabilities(()) => {
            let result = get_capabilities(&state);
            (StatusCode::OK, ProtobufRef(result)).into_response()
        }
        request::Call::GetLatestKeyBlock(()) => {
            match &*state.proto_cache().load_latest_key_block() {
                Some(config) => config.as_ref().clone().into_response(),
                None => error_to_response(RpcStateError::NotReady),
            }
        }
        request::Call::GetBlockchainConfig(()) => {
            match &*state.proto_cache().load_blockchain_config() {
                Some(config) => config.as_ref().clone().into_response(),
                None => error_to_response(RpcStateError::NotReady),
            }
        }
        request::Call::GetStatus(()) => {
            let result = response::Result::GetStatus(response::GetStatus {
                ready: state.is_ready(),
            });
            ok_to_response(result)
        }
        request::Call::GetTimings(()) => {
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
        request::Call::SendMessage(p) => {
            if let Err(e) = validate_external_message(&p.message).await {
                return ProtoErrorResponse {
                    code: INVALID_BOC_CODE,
                    message: e.to_string().into(),
                }
                .into_response();
            }

            state.broadcast_external_message(&p.message).await;
            ok_to_response(response::Result::SendMessage(()))
        }
        request::Call::GetLibraryCell(p) => {
            let Some(hash) = hash_from_bytes(p.hash) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid hash".into(),
                }
                .into_response();
            };

            let cell_opt = match state.proto_cache().get_library_cell_proto(&hash) {
                Some(value) => Some(value),
                None => match state.get_raw_library(&hash) {
                    Ok(Some(cell)) => {
                        let boc = state.proto_cache().insert_library_cell(hash, cell);
                        Some(boc)
                    }
                    Ok(None) => None,
                    Err(e) => return error_to_response(RpcStateError::Internal(e)),
                },
            };
            ok_to_response(response::Result::GetLibraryCell(GetLibraryCell {
                cell: cell_opt,
            }))
        }
        request::Call::GetContractState(p) => {
            let Some(address) = addr_from_bytes(p.address) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid address".into(),
                }
                .into_response();
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
        request::Call::GetAccountsByCodeHash(p) => {
            if p.limit == 0 {
                let result = response::Result::GetAccounts(response::GetAccountsByCodeHash {
                    account: Vec::new(),
                });
                return ok_to_response(result);
            } else if p.limit > MAX_LIMIT {
                return too_large_limit_response();
            }

            let Some(code_hash) = hash_from_bytes(p.code_hash) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid code hash".into(),
                }
                .into_response();
            };

            let continuation = match p.continuation.map(addr_from_bytes) {
                Some(Some(continuation)) => Some(continuation),
                Some(None) => {
                    return ProtoErrorResponse {
                        code: INVALID_PARAMS_CODE,
                        message: "invalid continuation".into(),
                    }
                    .into_response()
                }
                None => None,
            };

            match state.get_accounts_by_code_hash(&code_hash, continuation.as_ref()) {
                Ok(list) => {
                    let result = response::Result::GetAccounts(response::GetAccountsByCodeHash {
                        account: list
                            .into_raw()
                            .take(p.limit as usize)
                            .map(|addr| Bytes::copy_from_slice(&addr))
                            .collect(),
                    });
                    ok_to_response(result)
                }
                Err(e) => error_to_response(e),
            }
        }
        request::Call::GetTransactionsList(p) => {
            if p.limit == 0 {
                let result = response::Result::GetTransactionsList(response::GetTransactionsList {
                    transactions: Vec::new(),
                });
                return ok_to_response(result);
            } else if p.limit > MAX_LIMIT {
                return too_large_limit_response();
            }

            let Some(account) = addr_from_bytes(p.account) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid address".into(),
                }
                .into_response();
            };

            match state.get_transactions(&account, p.last_transaction_lt) {
                Ok(list) => {
                    let transactions = list
                        .map(Bytes::copy_from_slice)
                        .take(p.limit as usize)
                        .collect();
                    ok_to_response(response::Result::GetTransactionsList(
                        response::GetTransactionsList { transactions },
                    ))
                }
                Err(e) => error_to_response(e),
            }
        }
        request::Call::GetTransaction(p) => {
            let Some(hash) = hash_from_bytes(p.id) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid tx id".into(),
                }
                .into_response();
            };

            match state.get_transaction(&hash) {
                Ok(tx) => ok_to_response(response::Result::GetRawTransaction(
                    response::GetRawTransaction {
                        transaction: tx.map(|slice| Bytes::copy_from_slice(slice.as_ref())),
                    },
                )),
                Err(e) => error_to_response(e),
            }
        }
        request::Call::GetDstTransaction(p) => {
            let Some(hash) = hash_from_bytes(p.message_hash) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid msg id".into(),
                }
                .into_response();
            };

            match state.get_dst_transaction(&hash) {
                Ok(tx) => ok_to_response(response::Result::GetRawTransaction(
                    response::GetRawTransaction {
                        transaction: tx.map(|slice| Bytes::copy_from_slice(slice.as_ref())),
                    },
                )),
                Err(e) => error_to_response(e),
            }
        }
        request::Call::GetTransactionBlockId(p) => {
            let Some(hash) = hash_from_bytes(p.id) else {
                return ProtoErrorResponse {
                    code: INVALID_PARAMS_CODE,
                    message: "invalid tx id".into(),
                }
                .into_response();
            };

            match state.get_transaction_block_id(&hash) {
                Ok(block_id) => ok_to_response(response::Result::GetTransactionBlockId(
                    response::GetTransactionBlockId {
                        block_id: block_id.map(|id| response::BlockId {
                            workchain: id.shard.workchain(),
                            shard: id.shard.prefix(),
                            seqno: id.seqno,
                            root_hash: Bytes::copy_from_slice(id.root_hash.as_ref()),
                            file_hash: Bytes::copy_from_slice(id.file_hash.as_ref()),
                        }),
                    },
                )),
                Err(e) => error_to_response(e),
            }
        }
    }
}

// NOTE: `RpcState` full/not-full state is determined only once at startup,
// so it is ok to cache the response.
fn get_capabilities(state: &RpcState) -> &'static rpc::Response {
    static RESULT: OnceLock<rpc::Response> = OnceLock::new();
    RESULT.get_or_init(|| {
        // FIXME: Why strings when we have enums in the proto?

        let mut capabilities = vec![
            "getCapabilities",
            "getLatestKeyBlock",
            "getBlockchainConfig",
            "getStatus",
            "getTimings",
            "getContractState",
            "sendMessage",
            "getLibraryCell",
        ];

        if state.is_full() {
            capabilities.extend([
                "getTransactionsList",
                "getTransaction",
                "getDstTransaction",
                "getAccountsByCodeHash",
                "getTransactionBlockId",
            ]);
        }

        rpc::Response {
            result: Some(response::Result::GetCapabilities(
                response::GetCapabilities {
                    capabilities: capabilities.into_iter().map(|s| s.into()).collect(),
                },
            )),
        }
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

fn addr_from_bytes(bytes: Bytes) -> Option<StdAddr> {
    (bytes.len() == 33)
        .then(|| StdAddr::new(bytes[0] as i8, HashBytes(bytes[1..33].try_into().unwrap())))
}

fn hash_from_bytes(bytes: Bytes) -> Option<HashBytes> {
    (bytes.len() == 32).then(|| HashBytes::from_slice(&bytes))
}

fn serialize_account(account: &Account) -> Result<Bytes, everscale_types::error::Error> {
    let cell = crate::models::serialize_account(account)?;
    Ok(Boc::encode(cell).into())
}

const MAX_LIMIT: u32 = 100;
