use std::borrow::Cow;
use std::cell::RefCell;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::models::*;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_storage::{CodeHashesIter, TransactionsIterBuilder};
use tycho_util::serde_helpers;

use self::extractor::{Jrpc, JrpcErrorResponse, JrpcOkResponse};
use crate::declare_jrpc_method;
use crate::state::{RpcState, RpcStateError};

mod extractor;

pub async fn route(State(state): State<RpcState>, req: Jrpc<Method>) -> Response {
    match req.params {
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
            JrpcOkResponse::new(req.id, ()).into_response()
        }
        MethodParams::GetContractState(_) => todo!(),
        MethodParams::GetAccountsByCodeHash(p) => {
            if p.limit == 0 {
                return JrpcOkResponse::new(req.id, [(); 0]).into_response();
            }
            match state.get_accounts_by_code_hash(&p.code_hash, p.continuation.as_ref()) {
                Ok(list) => JrpcOkResponse::new(req.id, GetAccountsByCodeHashResponse {
                    list: RefCell::new(Some(list)),
                    limit: p.limit,
                })
                .into_response(),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransactionsList(p) => {
            match state.get_transactions(&p.account, p.last_transaction_lt) {
                Ok(list) => JrpcOkResponse::new(req.id, GetTransactionsListResponse {
                    list: RefCell::new(Some(list)),
                })
                .into_response(),
                Err(e) => error_to_response(req.id, e),
            }
        }
        MethodParams::GetTransaction(p) => match state.get_transaction(&p.id) {
            Ok(value) => JrpcOkResponse::new(req.id, value.map(encode_base64)).into_response(),
            Err(e) => error_to_response(req.id, e),
        },
        MethodParams::GetDstTransaction(p) => match state.get_dst_transaction(&p.message_hash) {
            Ok(value) => JrpcOkResponse::new(req.id, value.map(encode_base64)).into_response(),
            Err(e) => error_to_response(req.id, e),
        },
    }
}

declare_jrpc_method! {
    pub enum MethodParams: Method {
        SendMessage(SendMessageRequest),
        GetContractState(GetContractStateRequest),
        GetAccountsByCodeHash(GetAccountsByCodeHashRequest),
        GetTransactionsList(GetTransactionsListRequest),
        GetTransaction(GetTransactionRequest),
        GetDstTransaction(GetDstTransactionRequest),
    }
}

// === Requests ===

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

fn error_to_response(id: i64, e: RpcStateError) -> Response {
    let (code, message) = match e {
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
const NOT_SUPPORTED_CODE: i32 = -32001;
const INVALID_BOC_CODE: i32 = -32002;
