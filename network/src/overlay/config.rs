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

    /// A period of exchanging public overlay peers.
    ///
    /// Default: 3 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_exchange_period: Duration,

    /// A maximum value of a random jitter for the peer exchange period.
    ///
    /// Default: 30 seconds.
    pub public_overlay_peer_exchange_max_jitter: Duration,

    /// Number of peers to send during entries exchange request.
    ///
    /// Default: 20.
    pub exchange_public_entries_batch: usize,
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            max_public_entry_tll: Duration::from_secs(3600),
            public_overlay_peer_exchange_period: Duration::from_secs(3 * 60),
            public_overlay_peer_exchange_max_jitter: Duration::from_secs(30),
            exchange_public_entries_batch: 20,
        }
    }
}
