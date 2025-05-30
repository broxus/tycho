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
use tycho_block_util::message::{validate_external_message, ExtMsgRepr};
use tycho_storage::{CodeHashesIter, TransactionsIterBuilder};
use tycho_util::metrics::HistogramGuard;
use tycho_util::serde_helpers::{self, Base64BytesWithLimit};

pub use self::cache::JrpcEndpointCache;
use crate::models::{GenTimings, LastTransactionId};
use crate::state::{LoadedAccountState, RpcState, RpcStateError};
use crate::util::error_codes::*;
use crate::util::jrpc_extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};

mod cache;

declare_jrpc_method! {
    pub enum MethodParams: Method {
        GetCapabilities(EmptyParams),
        GetLatestKeyBlock(EmptyParams),
        GetBlockchainConfig(EmptyParams),
        GetStatus(EmptyParams),
        GetTimings(EmptyParams),
        SendMessage(SendMessageRequest),
        GetContractState(GetContractStateRequest),
        GetLibraryCell(GetLibraryCellRequest),
        GetAccountsByCodeHash(GetAccountsByCodeHashRequest),
        GetTransactionsList(GetTransactionsListRequest),
        GetTransaction(GetTransactionRequest),
        GetDstTransaction(GetDstTransactionRequest),
        GetTransactionBlockId(GetTransactionRequest),
        GetKeyBlockProof(GetKeyBlockProofRequest),
        GetBlockProof(GetBlockRequest),
        // NOTE: Temp endpoint. Must be enforced by limits and other stuff.
        GetBlockData(GetBlockRequest),
    }
}

pub async fn route(State(state): State<RpcState>, req: Jrpc<i64, Method>) -> Response {
    let label = [("method", req.method)];
    let _hist = HistogramGuard::begin_with_labels("tycho_jrpc_request_time", &label);
    match req.params {
        MethodParams::GetCapabilities(_) => ok_to_response(req.id, get_capabilities(&state)),
        MethodParams::GetLatestKeyBlock(_) => match &*state.jrpc_cache().load_latest_key_block() {
            Some(config) => ok_to_response(req.id, config.as_ref()),
            None => error_to_response(req.id, RpcStateError::NotReady),
        },
        MethodParams::GetBlockchainConfig(_) => match &*state.jrpc_cache().load_blockchain_config()
        {
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
            if let Err(e) = validate_external_message(&p.message).await {
                return JrpcErrorResponse {
                    id: Some(req.id),
                    code: INVALID_BOC_CODE,
                    message: e.to_string().into(),
                }
                .into_response();
            }

            state.broadcast_external_message(&p.message).await;
            ok_to_response(req.id, ())
        }
        MethodParams::GetLibraryCell(p) => {
            let res = match state.jrpc_cache().get_library_cell_response(&p.hash) {
                Some(value) => value,
                None => match state.get_raw_library(&p.hash) {
                    Ok(res) => state.jrpc_cache().insert_library_cell_response(p.hash, res),
                    Err(e) => return error_to_response(req.id, RpcStateError::Internal(e)),
                },
            };
            ok_to_response(req.id, res.as_ref())
        }
        MethodParams::GetContractState(p) => {
            let item = match state.get_account_state(&p.address) {
                Ok(item) => item,
                Err(e) => return error_to_response(req.id, e),
            };

            let account;
            let _mc_ref_handle;
            ok_to_response(req.id, match item {
                LoadedAccountState::NotFound { timings, .. } => {
                    GetContractStateResponse::NotExists { timings }
                }
                LoadedAccountState::Found { state, timings, .. }
                    if Some(state.last_trans_lt) <= p.last_transaction_lt =>
                {
                    GetContractStateResponse::Unchanged { timings }
                }
                LoadedAccountState::Found {
                    state,
                    timings,
                    mc_ref_handle,
                    ..
                } => {
                    _mc_ref_handle = mc_ref_handle;
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
            } else if p.limit > GetAccountsByCodeHashResponse::MAX_LIMIT {
                return too_large_limit_response(req.id);
            }
            match state.get_accounts_by_code_hash(&p.code_hash, p.continuation.as_ref(), None) {
                Ok(list) => ok_to_response(req.id, GetAccountsByCodeHashResponse {
                    list: RefCell::new(Some(list)),
                    limit: p.limit,
                }),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransactionsList(p) => {
            if p.limit == 0 {
                return JrpcOkResponse::new(req.id, [(); 0]).into_response();
            } else if p.limit > GetTransactionsListResponse::MAX_LIMIT {
                return too_large_limit_response(req.id);
            }
            match state.get_transactions(&p.account, p.last_transaction_lt, 0, None) {
                // TODO: Move serialization to a separate blocking task pool.
                Ok(list) => ok_to_response(req.id, GetTransactionsListResponse {
                    list: RefCell::new(Some(list)),
                    limit: p.limit,
                }),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransaction(p) => match state.get_transaction(&p.id, None) {
            Ok(value) => ok_to_response(req.id, value.as_ref().map(encode_base64)),
            Err(e) => error_to_response(req.id, e),
        },
        MethodParams::GetDstTransaction(p) => {
            match state.get_dst_transaction(&p.message_hash, None) {
                Ok(value) => ok_to_response(req.id, value.as_ref().map(encode_base64)),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransactionBlockId(p) => match state.get_transaction_info(&p.id, None) {
            Ok(value) => ok_to_response(
                req.id,
                value.map(|info| BlockIdResponse {
                    block_id: info.block_id,
                }),
            ),
            Err(e) => error_to_response(req.id, e),
        },
        MethodParams::GetKeyBlockProof(p) => {
            let res = match state.jrpc_cache().get_key_block_proof_response(p.seqno) {
                Some(value) => value,
                None => {
                    let res = state.get_key_block_proof(p.seqno).await;
                    state
                        .jrpc_cache()
                        .insert_key_block_proof_response(p.seqno, res)
                }
            };
            ok_to_response(req.id, res.as_ref())
        }
        MethodParams::GetBlockProof(p) => {
            if !state.config().allow_huge_requests {
                return error_to_response(req.id, RpcStateError::NotSupported);
            }

            let proof = state.get_block_proof(&p.block_id).await.map(encode_base64);
            ok_to_response(req.id, BlockProofResponse { proof })
        }
        MethodParams::GetBlockData(p) => {
            if !state.config().allow_huge_requests {
                return error_to_response(req.id, RpcStateError::NotSupported);
            }

            // TODO: Rework rate limiting for this request.
            let _permit = state.acquire_download_block_permit().await;

            let Some(data) = state.get_block_data(&p.block_id).await else {
                return ok_to_response(req.id, BlockDataResponse { data: None });
            };

            tycho_util::sync::rayon_run(move || {
                let data = encode_base64(data);
                ok_to_response(req.id, BlockDataResponse { data: Some(data) })
            })
            .await
        }
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
    #[serde(with = "Base64BytesWithLimit::<{ ExtMsgRepr::MAX_BOC_SIZE }>")]
    pub message: bytes::Bytes,
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
pub struct GetLibraryCellRequest {
    pub hash: HashBytes,
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
    #[serde(default, with = "serde_helpers::option_string")]
    pub last_transaction_lt: Option<u64>,
    pub limit: u8,
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionRequest {
    pub id: HashBytes,
}

#[derive(Debug, Deserialize)]
pub struct GetKeyBlockProofRequest {
    pub seqno: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBlockRequest {
    #[serde(with = "serde_helpers::string")]
    pub block_id: BlockId,
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
            "getLibraryCell",
            "getKeyBlockProof",
        ];

        if state.config().allow_huge_requests {
            capabilities.extend(["getBlockProof", "getBlockData"]);
        }

        if state.is_full() {
            capabilities.extend([
                "getTransactionsList",
                "getTransaction",
                "getDstTransaction",
                "getAccountsByCodeHash",
                "getTransactionBlockId",
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

    let cell = crate::models::serialize_account(account).map_err(Error::custom)?;
    Boc::encode_base64(cell).serialize(serializer)
}

struct GetAccountsByCodeHashResponse<'a> {
    list: RefCell<Option<CodeHashesIter<'a>>>,
    limit: u8,
}

impl GetAccountsByCodeHashResponse<'_> {
    const MAX_LIMIT: u8 = 100;
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

struct GetTransactionsListResponse {
    list: RefCell<Option<TransactionsIterBuilder>>,
    limit: u8,
}

impl GetTransactionsListResponse {
    const MAX_LIMIT: u8 = 100;
}

impl Serialize for GetTransactionsListResponse {
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
            Some(res)
        })
        .take(self.limit as _)
        .collect::<Result<(), _>>()?;

        seq.end()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockIdResponse {
    #[serde(with = "serde_helpers::string")]
    block_id: BlockId,
}

#[derive(Serialize)]
struct BlockProofResponse {
    proof: Option<String>,
}

#[derive(Serialize)]
struct BlockDataResponse {
    data: Option<String>,
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
        RpcStateError::BadRequest(e) => (INVALID_PARAMS_CODE, e.to_string().into()),
    };

    JrpcErrorResponse {
        id: Some(id),
        code,
        message,
    }
    .into_response()
}

fn too_large_limit_response(id: i64) -> Response {
    JrpcErrorResponse {
        id: Some(id),
        code: TOO_LARGE_LIMIT_CODE,
        message: Cow::Borrowed("limit is too large"),
    }
    .into_response()
}
