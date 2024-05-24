use axum::response::Response;
use everscale_types::models::*;
use everscale_types::prelude::*;
use serde::Deserialize;

use self::extractor::Jrpc;
use crate::declare_jrpc_method;

mod extractor;

async fn jrpc_router(req: Jrpc<Method>) -> Response {
    match req.params {
        MethodParams::SendMessage(_) => todo!(),
        MethodParams::GetContractState(_) => todo!(),
        MethodParams::GetTransactionsList(_) => todo!(),
        MethodParams::GetTransaction(_) => todo!(),
        MethodParams::GetDstTransaction(_) => todo!(),
    }
}

declare_jrpc_method! {
    pub enum MethodParams: Method {
        SendMessage(SendMessageRequest),
        GetContractState(GetContractStateRequest),
        GetTransactionsList(GetTransactionsListRequest),
        GetTransaction(GetTransactionRequest),
        GetDstTransaction(GetDstTransactionRequest),
    }
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    #[serde(with = "BocRepr")]
    pub message: Box<OwnedMessage>,
}

#[derive(Debug, Deserialize)]
pub struct GetContractStateRequest {
    pub address: StdAddr,
    // TODO: serialize as optional string
    pub last_transaction_lt: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionsListRequest {
    pub account: StdAddr,
    // TODO: serialize as optional string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transaction_lt: Option<u64>,
    pub limit: u8,
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionRequest {
    pub id: HashBytes,
}

#[derive(Debug, Deserialize)]
pub struct GetDstTransactionRequest {
    pub message_hash: HashBytes,
}

const NOT_READY_CODE: i32 = -32000;
const NOT_SUPPORTED_CODE: i32 = -32001;
