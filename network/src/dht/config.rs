use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

// TODO: add max storage item size
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DhtConfig {
    /// DHT K parameter.
    ///
    /// Default: 6.
    pub max_k: usize,
    /// Maximum time to live for node info.
    ///
    /// Default: 1 hour.
    #[serde(with = "serde_helpers::humantime")]
    pub max_node_info_ttl: Duration,
    /// Maximum time to live for stored values.
    ///
    /// Default: 1 hour.
    #[serde(with = "serde_helpers::humantime")]
    pub max_stored_value_ttl: Duration,
    /// Maximum storage capacity (number of entries).
    ///
    /// Default: 10000.
    pub max_storage_capacity: u64,
    /// Time until a stored item is considered idle and can be removed.
    ///
    /// Default: unlimited.
    #[serde(with = "serde_helpers::humantime")]
    pub storage_item_time_to_idle: Option<Duration>,
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            max_k: 6,
            max_node_info_ttl: Duration::from_secs(3600),
            max_stored_value_ttl: Duration::from_secs(3600),
            max_storage_capacity: 10000,
            storage_item_time_to_idle: None,
        }
    }
}
