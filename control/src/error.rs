use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("client failed: {0}")]
    ClientFailed(#[from] anyhow::Error),
    #[error("RPC failed: {0}")]
    RpcFailed(#[from] tarpc::client::RpcError),
    #[error("server responded with an error: {0}")]
    Internal(#[from] ServerError),
}

pub type ClientResult<T, E = ClientError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
#[error("server error: {0}")]
pub struct ServerError(String);

impl ServerError {
    pub fn new<S: Into<String>>(msg: S) -> Self {
        Self(msg.into())
    }
}

impl From<anyhow::Error> for ServerError {
    fn from(value: anyhow::Error) -> Self {
        Self(value.to_string())
    }
}

pub type ServerResult<T, E = ServerError> = std::result::Result<T, E>;
