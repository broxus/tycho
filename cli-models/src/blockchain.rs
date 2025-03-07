use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Output for blockchain config parameter query.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigParamOutput {
    /// Global blockchain ID.
    pub global_id: i32,

    /// Current config sequence number.
    pub seqno: u32,

    /// The requested parameter value.
    pub param: Option<Value>,
}

/// Output for blockchain config modification command.
#[derive(Debug, Serialize)]
pub struct ConfigMessageOutput {
    /// Expiration timestamp for the message.
    pub expire_at: u32,

    /// Message hash.
    pub message_hash: String,

    /// Message BOC in base64.
    pub message: String,
}
