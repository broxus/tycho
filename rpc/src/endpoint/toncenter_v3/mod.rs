use std::borrow::Cow;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use everscale_types::boc::BocRepr;
use everscale_types::models::Transaction;
use serde::Serialize;

use self::models::*;
use super::jrpc::GetAccountsByCodeHashRequest;
use crate::state::{RpcState, RpcStateError};

mod models;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new().route("/adjacentTransactions", get(get_adjacent_transactions))
}

// === GET /adjacentTransactions

async fn get_adjacent_transactions(
    State(state): State<RpcState>,
    query: Result<Query<GetAccountsByCodeHashRequest>, QueryRejection>,
) -> Result<Response, ErrorResponse> {
    const NOT_FOUND: &str = "adjacent transactions not found";

    let Query(query) = query?;

    let Some(tx) = state.get_transaction(&query.code_hash)? else {
        return Err(ErrorResponse::text_not_found(NOT_FOUND));
    };
    let tx: Transaction = BocRepr::decode(tx).map_err(RpcStateError::internal)?;

    let in_msg = tx.load_in_msg().map_err(RpcStateError::internal)?;

    Ok(().into_response())
}

// === Helpers ===

fn ok_to_response<T: Serialize>(result: T) -> Response {
    axum::Json(result).into_response()
}

#[derive(Debug)]
struct ErrorResponse {
    status_code: StatusCode,
    error: Cow<'static, str>,
}

impl ErrorResponse {
    fn text_not_found(msg: &'static str) -> Self {
        Self {
            status_code: StatusCode::NOT_FOUND,
            error: Cow::Borrowed(msg),
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Response<'a> {
            error: &'a str,
        }

        IntoResponse::into_response((
            self.status_code,
            axum::Json(Response { error: &self.error }),
        ))
    }
}

impl From<RpcStateError> for ErrorResponse {
    fn from(value: RpcStateError) -> Self {
        let (status_code, error) = match value {
            RpcStateError::NotReady => {
                (StatusCode::SERVICE_UNAVAILABLE, Cow::Borrowed("not ready"))
            }
            RpcStateError::NotSupported => (
                StatusCode::NOT_IMPLEMENTED,
                Cow::Borrowed("method not supported"),
            ),
            RpcStateError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string().into()),
            RpcStateError::BadRequest(e) => (StatusCode::BAD_REQUEST, e.to_string().into()),
        };

        Self { status_code, error }
    }
}

impl From<QueryRejection> for ErrorResponse {
    fn from(value: QueryRejection) -> Self {
        Self::from(RpcStateError::BadRequest(value.into()))
    }
}
