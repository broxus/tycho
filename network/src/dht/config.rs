use std::time::Duration;

// TODO: add max storage item size
#[derive(Debug, Clone)]
pub struct DhtConfig {
    /// DHT K parameter.
    pub max_k: usize,
    /// Maximum time to live for node info.
    pub max_node_info_ttl: Duration,
    /// Maximum time to live for stored values.
    pub max_stored_value_ttl: Duration,
    /// Maximum length of stored key names.
    pub max_stored_key_name_len: usize,
    /// Maximum index of stored keys.
    pub max_stored_key_index: u32,
    /// Maximum storage capacity (number of entries).
    pub max_storage_capacity: u64,
    /// Time until a stored item is considered idle and can be removed.
    pub storage_item_time_to_idle: Option<Duration>,
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            max_k: 6,
            max_node_info_ttl: Duration::from_secs(3600),
            max_stored_value_ttl: Duration::from_secs(3600),
            max_stored_key_name_len: 128,
            max_stored_key_index: 4,
            max_storage_capacity: 10000,
            storage_item_time_to_idle: None,
        }
    }
}
