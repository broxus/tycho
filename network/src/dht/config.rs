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

    /// A period of refreshing the local node info.
    ///
    /// Default: 1 minute.
    #[serde(with = "serde_helpers::humantime")]
    pub local_info_refresh_period: Duration,

    /// A period of storing the local node info into the DHT.
    ///
    /// Default: 10 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub local_info_announce_period: Duration,

    /// A maximum value of a random jitter for the peer announce period.
    ///
    /// Default: 1 minute.
    #[serde(with = "serde_helpers::humantime")]
    pub max_local_info_announce_period_jitter: Duration,

    /// A period of updating and populating the routing table.
    ///
    /// Default: 10 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub populate_period: Duration,

    /// A maximum value of a random jitter for the populate period.
    ///
    /// Default: 1 minutes.
    #[serde(with = "serde_helpers::humantime")]
    pub max_populate_period_jitter: Duration,
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            max_k: 6,
            max_node_info_ttl: Duration::from_secs(3600),
            max_stored_value_ttl: Duration::from_secs(3600),
            max_storage_capacity: 10000,
            storage_item_time_to_idle: None,
            local_info_refresh_period: Duration::from_secs(60),
            local_info_announce_period: Duration::from_secs(600),
            max_local_info_announce_period_jitter: Duration::from_secs(60),
            populate_period: Duration::from_secs(600),
            max_populate_period_jitter: Duration::from_secs(60),
        }
    }
}
