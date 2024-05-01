use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct PublicOverlayClientConfig {
    /// The interval at which neighbours list is updated.
    ///
    /// Default: 2 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub neighbours_update_interval: Duration,

    /// The interval at which current neighbours are pinged.
    ///
    /// Default: 30 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub neighbours_ping_interval: Duration,

    /// The maximum number of neighbours to keep.
    ///
    /// Default: 5.
    pub max_neighbours: usize,

    /// The maximum number of ping tasks to run concurrently.
    ///
    /// Default: 5.
    pub max_ping_tasks: usize,

    /// The default roundtrip time to use when a neighbour is added.
    ///
    /// Default: 300 ms.
    #[serde(with = "serde_helpers::humantime")]
    pub default_roundtrip: Duration,
}

impl Default for PublicOverlayClientConfig {
    fn default() -> Self {
        Self {
            neighbours_update_interval: Duration::from_secs(2 * 60),
            neighbours_ping_interval: Duration::from_secs(30),
            max_neighbours: 5,
            max_ping_tasks: 5,
            default_roundtrip: Duration::from_millis(300),
        }
    }
}
