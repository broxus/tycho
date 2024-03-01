use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OverlayConfig {
    /// A period of exchanging public overlay peers.
    ///
    /// Default: 3 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_exchange_period: Duration,

    /// A maximum value of a random jitter for the peer exchange period.
    ///
    /// Default: 30 seconds.
    pub public_overlay_peer_exchange_max_jitter: Duration,

    /// A period of resolving peer info of public overlay entries.
    ///
    /// Default: 1 minute.
    pub public_overlay_peer_resolve_period: Duration,

    /// A maximum value of a random jitter for the public peer resolve period.
    ///
    /// Default: 20 seconds.
    pub public_overlay_peer_resolve_max_jitter: Duration,

    /// A period of resolving peer info of private overlay entries.
    ///
    /// Default: 1 minute.
    pub private_overlay_peer_resolve_period: Duration,

    /// A maximum value of a random jitter for the private peer resolve period.
    ///
    /// Default: 20 seconds.
    pub private_overlay_peer_resolve_max_jitter: Duration,

    /// Number of peers to send during entries exchange request.
    ///
    /// Default: 20.
    pub exchange_public_entries_batch: usize,

    /// Maximum number of parallel resolver requests (for each overlay).
    ///
    /// Default: 10.
    pub max_parallel_resolver_requests: usize,
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            public_overlay_peer_exchange_period: Duration::from_secs(3 * 60),
            public_overlay_peer_exchange_max_jitter: Duration::from_secs(30),
            public_overlay_peer_resolve_period: Duration::from_secs(60),
            public_overlay_peer_resolve_max_jitter: Duration::from_secs(20),
            private_overlay_peer_resolve_period: Duration::from_secs(60),
            private_overlay_peer_resolve_max_jitter: Duration::from_secs(20),
            exchange_public_entries_batch: 20,
            max_parallel_resolver_requests: 10,
        }
    }
}
