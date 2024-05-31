use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::OnceLock;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::models::*;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tycho_storage::{CodeHashesIter, TransactionsIterBuilder};
use tycho_util::serde_helpers;

use self::extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};
use crate::models::{GenTimings, LastTransactionId};
use crate::state::{LoadedAccountState, RpcState, RpcStateError};

mod extractor;

declare_jrpc_method! {
    pub enum MethodParams: Method {
        GetCapabilities(EmptyParams),
        GetLatestKeyBlock(EmptyParams),
        GetBlockchainConfig(EmptyParams),
        GetStatus(EmptyParams),
        GetTimings(EmptyParams),
        SendMessage(SendMessageRequest),
        GetContractState(GetContractStateRequest),
        GetAccountsByCodeHash(GetAccountsByCodeHashRequest),
        GetTransactionsList(GetTransactionsListRequest),
        GetTransaction(GetTransactionRequest),
        GetDstTransaction(GetDstTransactionRequest),
    }
}

pub async fn route(State(state): State<RpcState>, req: Jrpc<Method>) -> Response {
    match req.params {
        MethodParams::GetCapabilities(_) => ok_to_response(req.id, get_capabilities(&state)),
        MethodParams::GetLatestKeyBlock(_) => match &*state.load_latest_key_block_json() {
            Some(config) => ok_to_response(req.id, config.as_ref()),
            None => error_to_response(req.id, RpcStateError::NotReady),
        },
        MethodParams::GetBlockchainConfig(_) => match &*state.load_blockchain_config_json() {
            Some(config) => ok_to_response(req.id, config.as_ref()),
            None => error_to_response(req.id, RpcStateError::NotReady),
        },
        MethodParams::GetStatus(_) => ok_to_response(req.id, GetStatusResponse {
            ready: state.is_ready(),
        }),
        MethodParams::GetTimings(_) => {
            if state.is_ready() {
                ok_to_response(req.id, state.load_timings().as_ref())
            } else {
                error_to_response(req.id, RpcStateError::NotReady)
            }
        }
        MethodParams::SendMessage(p) => {
            let Ok(data) = BocRepr::encode(p.message) else {
                return JrpcErrorResponse {
                    id: Some(req.id),
                    code: INVALID_BOC_CODE,
                    message: Cow::Borrowed("invalid message BOC"),
                }
                .into_response();
            };
            state.broadcast_external_message(&data).await;
            ok_to_response(req.id, ())
        }
        MethodParams::GetContractState(p) => {
            let item = match state.get_account_state(&p.address) {
                Ok(item) => item,
                Err(e) => return error_to_response(req.id, e),
            };

            let account;
            ok_to_response(req.id, match &item {
                &LoadedAccountState::NotFound { timings } => {
                    GetContractStateResponse::NotExists { timings }
                }
                LoadedAccountState::Found {
                    state, gen_utime, ..
                } if Some(state.last_trans_lt) <= p.last_transaction_lt => {
                    GetContractStateResponse::Unchanged {
                        timings: GenTimings {
                            gen_lt: state.last_trans_lt,
                            gen_utime: *gen_utime,
                        },
                    }
                }
                LoadedAccountState::Found {
                    state, gen_utime, ..
                } => {
                    let timings = GenTimings {
                        gen_lt: state.last_trans_lt,
                        gen_utime: *gen_utime,
                    };
                    match state.load_account() {
                        Ok(Some(loaded)) => {
                            account = loaded;
                            GetContractStateResponse::Exists {
                                account: &account,
                                timings,
                                last_transaction_id: LastTransactionId {
                                    hash: state.last_trans_hash,
                                    lt: state.last_trans_lt,
                                },
                            }
                        }
                        Ok(None) => GetContractStateResponse::NotExists { timings },
                        Err(e) => {
                            return error_to_response(req.id, RpcStateError::Internal(e.into()))
                        }
                    }
                }
            })
        }
        MethodParams::GetAccountsByCodeHash(p) => {
            if p.limit == 0 {
                return JrpcOkResponse::new(req.id, [(); 0]).into_response();
            }
            match state.get_accounts_by_code_hash(&p.code_hash, p.continuation.as_ref()) {
                Ok(list) => ok_to_response(req.id, GetAccountsByCodeHashResponse {
                    list: RefCell::new(Some(list)),
                    limit: p.limit,
                }),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransactionsList(p) => {
            match state.get_transactions(&p.account, p.last_transaction_lt) {
                Ok(list) => ok_to_response(req.id, GetTransactionsListResponse {
                    list: RefCell::new(Some(list)),
                }),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransaction(p) => match state.get_transaction(&p.id) {
            Ok(value) => ok_to_response(req.id, value.map(encode_base64)),
            Err(e) => error_to_response(req.id, e),
        },
        MethodParams::GetDstTransaction(p) => match state.get_dst_transaction(&p.message_hash) {
            Ok(value) => ok_to_response(req.id, value.map(encode_base64)),
            Err(e) => error_to_response(req.id, e),
        },
    }
}

// === Requests ===

#[derive(Debug)]
pub struct EmptyParams;

impl<'de> Deserialize<'de> for EmptyParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Empty {}

        // Accepts both `null` and empty object.
        <Option<Empty>>::deserialize(deserializer).map(|_| Self)
    }
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    #[serde(with = "BocRepr")]
    pub message: Box<OwnedMessage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetContractStateRequest {
    pub address: StdAddr,
    #[serde(default, with = "serde_helpers::option_string")]
    pub last_transaction_lt: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAccountsByCodeHashRequest {
    pub code_hash: HashBytes,
    #[serde(default)]
    pub continuation: Option<StdAddr>,
    pub limit: u8,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionsListRequest {
    pub account: StdAddr,
    #[serde(with = "serde_helpers::option_string")]
    pub last_transaction_lt: Option<u64>,
    pub limit: u8,
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionRequest {
    pub id: HashBytes,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDstTransactionRequest {
    pub message_hash: HashBytes,
}

// === Responses ===

// NOTE: `RpcState` full/not-full state is determined only once at startup,
// so it is ok to cache the response.
fn get_capabilities(state: &RpcState) -> &'static RawValue {
    static RESULT: OnceLock<Box<RawValue>> = OnceLock::new();
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

        serde_json::value::to_raw_value(&capabilities).unwrap()
    })
}

#[derive(Serialize)]
pub struct GetStatusResponse {
    ready: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
enum GetContractStateResponse<'a> {
    NotExists {
        timings: GenTimings,
    },
    #[serde(rename_all = "camelCase")]
    Exists {
        #[serde(serialize_with = "serialize_account")]
        account: &'a Account,
        timings: GenTimings,
        last_transaction_id: LastTransactionId,
    },
    Unchanged {
        timings: GenTimings,
    },
}

fn serialize_account<S>(account: &Account, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::Error;

    fn write_account(account: &Account) -> Result<Cell, everscale_types::error::Error> {
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
        builder.build_ext(cx)
    }

    let cell = write_account(account).map_err(Error::custom)?;
    Boc::encode_base64(cell).serialize(serializer)
}

struct GetAccountsByCodeHashResponse<'a> {
    list: RefCell<Option<CodeHashesIter<'a>>>,
    limit: u8,
}

impl Serialize for GetAccountsByCodeHashResponse<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let list = self.list.borrow_mut().take().unwrap();

        // NOTE: We cannot use `limit` as the sequence length because
        // the iterator may return less.
        let mut seq = serializer.serialize_seq(None)?;
        for code_hash in list.take(self.limit as usize) {
            seq.serialize_element(&code_hash)?;
        }
        seq.end()
    }
}

struct GetTransactionsListResponse<'a> {
    list: RefCell<Option<TransactionsIterBuilder<'a>>>,
}

impl Serialize for GetTransactionsListResponse<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let list = self.list.borrow_mut().take().unwrap();

        let mut seq = serializer.serialize_seq(None)?;

        let mut buffer = String::new();

        // NOTE: We use a `.map` from a separate impl thus we cannot use `.try_for_each`.
        #[allow(clippy::map_collect_result_unit)]
        list.map(|item| {
            BASE64_STANDARD.encode_string(item, &mut buffer);
            let res = seq.serialize_element(&buffer);
            buffer.clear();
            res
        })
        .collect::<Result<(), _>>()?;

        seq.end()
    }
}

fn encode_base64<T: AsRef<[u8]>>(value: T) -> String {
    BASE64_STANDARD.encode(value)
}

fn ok_to_response<T: Serialize>(id: i64, result: T) -> Response {
    JrpcOkResponse::new(id, result).into_response()
}

fn error_to_response(id: i64, e: RpcStateError) -> Response {
    let (code, message) = match e {
        RpcStateError::NotReady => (NOT_READY_CODE, Cow::Borrowed("not ready")),
        RpcStateError::NotSupported => (NOT_SUPPORTED_CODE, Cow::Borrowed("method not supported")),
        RpcStateError::Internal(e) => (INTERNAL_ERROR_CODE, e.to_string().into()),
    };

    JrpcErrorResponse {
        id: Some(id),
        code,
        message,
    }
    .into_response()
}

// === Error codes ===

const INTERNAL_ERROR_CODE: i32 = -32000;
const NOT_READY_CODE: i32 = -32001;
const NOT_SUPPORTED_CODE: i32 = -32002;
const INVALID_BOC_CODE: i32 = -32003;
