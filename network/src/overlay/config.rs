use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OverlayConfig {
    /// A period of storing public overlay entries in local DHT.
    ///
    /// Default: 3 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_store_period: Duration,

    /// A maximum value of a random jitter for the entries store period.
    ///
    /// Default: 30 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_store_max_jitter: Duration,

    /// A maximum number of public overlay entries to store.
    ///
    /// Default: 20.
    pub public_overlay_peer_store_max_entries: usize,

    /// A period of exchanging public overlay peers.
    ///
    /// Default: 3 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_exchange_period: Duration,

    /// A maximum value of a random jitter for the peer exchange period.
    ///
    /// Default: 30 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_exchange_max_jitter: Duration,

    /// A period of discovering public overlay peers.
    ///
    /// Default: 3 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_discovery_period: Duration,

    /// A maximum value of a random jitter for the peer discovery period.
    ///
    /// Default: 30 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub public_overlay_peer_discovery_max_jitter: Duration,

    /// Number of peers to send during entries exchange request.
    ///
    /// Default: 20.
    pub exchange_public_entries_batch: usize,
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            public_overlay_peer_store_period: Duration::from_secs(3 * 60),
            public_overlay_peer_store_max_jitter: Duration::from_secs(30),
            public_overlay_peer_store_max_entries: 20,
            public_overlay_peer_exchange_period: Duration::from_secs(3 * 60),
            public_overlay_peer_exchange_max_jitter: Duration::from_secs(30),
            public_overlay_peer_discovery_period: Duration::from_secs(3 * 60),
            public_overlay_peer_discovery_max_jitter: Duration::from_secs(30),
            exchange_public_entries_batch: 20,
        }
    }
}
