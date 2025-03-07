use serde::Serialize;

/// Output for election-related commands.
#[derive(Debug, Serialize)]
pub struct ElectionOutput {
    /// Election ID.
    pub election_id: u32,

    /// Expiration timestamp.
    pub expire_at: u32,

    /// Message hash.
    pub message_hash: String,

    /// Message BOC in base64.
    pub message: String,
}
