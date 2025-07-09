use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct PublicOverlayClientConfig {
    /// Ordinary peers used to download blocks and other stuff.
    pub neighbors: NeighborsConfig,
    /// Validators as broadcast targets.
    pub validators: ValidatorsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NeighborsConfig {
    /// The interval at which neighbours list is updated.
    ///
    /// Default: 2 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub update_interval: Duration,

    /// The interval at which current neighbours are pinged.
    ///
    /// Default: 30 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub ping_interval: Duration,

    /// The interval at which neighbours score is applied to selection index.
    ///
    /// Default: 10 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub apply_score_interval: Duration,

    /// The maximum number of neighbours to keep.
    ///
    /// Default: 5.
    pub keep: usize,

    /// The maximum number of ping tasks to run concurrently.
    ///
    /// Default: 5.
    pub max_ping_tasks: usize,

    /// The default roundtrip time to use when a neighbour is added.
    ///
    /// Default: 300 ms.
    #[serde(with = "serde_helpers::humantime")]
    pub default_roundtrip: Duration,

    /// Send timeout (unidirectional).
    ///
    /// Default: 500ms.
    #[serde(with = "serde_helpers::humantime")]
    pub send_timeout: Duration,

    /// Query timeout (bidirectional).
    ///
    /// Default: 1s.
    #[serde(with = "serde_helpers::humantime")]
    pub query_timeout: Duration,
}

impl Default for NeighborsConfig {
    fn default() -> Self {
        Self {
            update_interval: Duration::from_secs(2 * 60),
            ping_interval: Duration::from_secs(30),
            apply_score_interval: Duration::from_secs(10),
            keep: 5,
            max_ping_tasks: 5,
            default_roundtrip: Duration::from_millis(300),
            send_timeout: Duration::from_millis(500),
            query_timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ValidatorsConfig {
    /// The interval at which target validators are pinged.
    ///
    /// Default: 60 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub ping_interval: Duration,

    /// The timeout for a ping.
    ///
    /// Default: 1 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub ping_timeout: Duration,

    /// The maximum number of validators to keep.
    ///
    /// Default: 5.
    pub keep: usize,

    /// The maximum number of ping tasks to run concurrently.
    ///
    /// Default: 5.
    pub max_ping_tasks: usize,

    /// Send timeout (unidirectional).
    ///
    /// Default: 500ms.
    #[serde(with = "serde_helpers::humantime")]
    pub send_timeout: Duration,
}

impl Default for ValidatorsConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(60),
            ping_timeout: Duration::from_secs(1),
            keep: 5,
            max_ping_tasks: 5,
            send_timeout: Duration::from_millis(500),
        }
    }
}
