use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::state::RpcStateError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("an internal server error occurred: {0}")]
    Anyhow(#[from] anyhow::Error),

    #[error("serde error occurred")]
    Serde(#[from] serde_json::Error),

    #[error("ever types error occurred")]
    Ever(#[from] everscale_types::error::Error),

    #[error("rpc error occurred")]
    Rpc(#[from] RpcStateError),

    #[error("bad request: {0}")]
    BadRequest(&'static str),

    #[error("not found: {0}")]
    NotFound(&'static str),
}

impl Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Anyhow(_) | Self::Ever(_) | Self::Rpc(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) | Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
        }
    }
}
/// Axum allows you to return `Result` from handler functions, but the error type
/// also must be some sort of response type.
///
/// By default, the generated `Display` impl is used to return a plaintext error message
/// to the client.
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Self::Anyhow(ref e) => {
                tracing::error!("Generic error: {:?}", e);
            }
            Self::Serde(ref e) => {
                tracing::error!("Failed to deserialize string {:?}", e);
            }
            Self::Ever(ref e) => {
                tracing::error!("Internal error {:?}", e);
            }
            Self::Rpc(ref e) => {
                tracing::error!("Internal error {:?}", e);
            }
            Self::BadRequest(ref e) => {
                tracing::error!("Bad request: {:?}", e);
            }
            Self::NotFound(ref e) => {
                tracing::error!("Not found: {:?}", e);
            }
        }

        (
            self.status_code(),
            Json(serde_json::json!({"error": self.to_string(), "error_code": 0})),
        )
            .into_response()
    }
}
