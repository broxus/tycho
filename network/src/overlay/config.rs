use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OverlayConfig {
    /// Maximum time to live for public overlay peer entries.
    ///
    /// Default: 1 hour.
    #[serde(with = "serde_helpers::humantime")]
    pub max_public_entry_tll: Duration,

    /// Number of peers to send in response to a peer exchange request.
    ///
    /// Default: 20.
    pub exchanged_peer_response_len: usize,
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            max_public_entry_tll: Duration::from_secs(3600),
            exchanged_peer_response_len: 20,
        }
    }
}
