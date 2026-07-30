use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum_client_ip::ClientIpSource;
use serde::{Deserialize, Serialize};
use tycho_types::models::StdAddr;
use tycho_util::config::PartialConfig;
use tycho_util::serde_helpers;

use crate::endpoint::RpcRateLimitsConfig;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
pub struct RpcConfig {
    /// TCP socket address to listen for incoming RPC connections.
    ///
    /// Default: `0.0.0.0:8000`
    #[important]
    pub listen_addr: SocketAddr,

    /// Whether to generate a stub keyblock from zerostate.
    ///
    /// Default: `false`.
    pub generate_stub_keyblock: bool,

    /// Number of virtual shards.
    ///
    /// Default: `4` (= 16 virtual shards).
    pub shard_split_depth: u8,

    // NOTE: TEMP
    /// Whether `getKeyBlockProof`, `getBlockProof` and `getBlockData` queries are enabled.
    ///
    /// Default: `false`.
    pub allow_huge_requests: bool,

    /// Max number of parallel block downloads.
    ///
    /// Default: `10`.
    pub max_parallel_block_downloads: usize,

    /// Configuration of getter requests.
    pub run_get_method: RunGetMethodConfig,

    /// Subscriptions limits and buffering.
    pub subscriptions: SubscriptionsConfig,

    /// Source for resolving the real client IP
    pub real_ip_source: ClientIpSource,

    /// Rate limits for inbound RPC requests.
    ///
    /// Default: disabled.
    pub rate_limits: Option<RpcRateLimitsConfig>,

    #[important]
    pub storage: RpcStorageConfig,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RunGetMethodConfig {
    /// The maximum number of methods running in parallel.
    /// Zero means disabled.
    ///
    /// Default: `20`.
    pub max_vms: usize,

    /// Max time to wait for a VM slot.
    ///
    /// Default: `50ms`.
    #[serde(with = "serde_helpers::humantime")]
    pub max_wait_for_vm: Duration,

    /// Max stack items in response.
    ///
    /// Default: 32.
    pub max_response_stack_items: usize,

    /// Default VM gas.
    ///
    /// Default: `1000000`.
    pub vm_getter_gas: u64,
}

impl Default for RunGetMethodConfig {
    fn default() -> Self {
        Self {
            max_vms: 20,
            max_wait_for_vm: Duration::from_millis(50),
            max_response_stack_items: 32,
            vm_getter_gas: 1000000,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RpcStorageConfig {
    Full {
        /// Transactions garbage collector configuration.
        ///
        /// Default: clear all transactions older than `1 week` every `1 hour`.
        ///
        /// `None` to disable garbage collection.
        gc: Option<TransactionsGcConfig>,

        /// Transaction partition rotation configuration.
        #[serde(default)]
        transaction_partitions: RpcTransactionPartitionsConfig,

        /// Reset all accounts.
        ///
        /// Default: `false`.
        force_reindex: bool,

        /// Path to account blacklist file. RPC skips storing transactions for this list.
        ///
        /// Default: `None`.
        blacklist_path: Option<PathBuf>,
    },
    /// Only store the state, no transactions and code hashes.
    StateOnly,
}

impl RpcStorageConfig {
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full { .. })
    }

    pub fn gc_is_enabled(&self) -> bool {
        match self {
            Self::Full { gc, .. } => gc.is_some(),
            Self::StateOnly => false,
        }
    }

    pub fn transaction_partitions(&self) -> Option<&RpcTransactionPartitionsConfig> {
        match self {
            Self::Full {
                transaction_partitions,
                ..
            } => Some(transaction_partitions),
            Self::StateOnly => None,
        }
    }

    pub fn is_force_reindex(&self) -> bool {
        match self {
            Self::Full { force_reindex, .. } => *force_reindex,
            Self::StateOnly => false,
        }
    }

    pub fn blacklist_path(&self) -> Option<PathBuf> {
        match self {
            Self::Full { blacklist_path, .. } => blacklist_path.clone(),
            Self::StateOnly => None,
        }
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            listen_addr: (Ipv4Addr::UNSPECIFIED, 8000).into(),
            generate_stub_keyblock: false,
            shard_split_depth: 4,
            allow_huge_requests: false,
            max_parallel_block_downloads: 10,
            run_get_method: RunGetMethodConfig::default(),
            subscriptions: SubscriptionsConfig::default(),
            real_ip_source: ClientIpSource::ConnectInfo,
            rate_limits: None,
            storage: RpcStorageConfig::Full {
                gc: None,
                transaction_partitions: Default::default(),
                force_reindex: false,
                blacklist_path: None,
            },
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcTransactionPartitionsConfig {
    pub target_lsm_bytes: u64,
    pub target_blob_bytes: u64,
    pub target_index_records: u64,
    pub max_open_sealed_partitions: usize,
    #[serde(default)]
    pub filters: RpcTransactionFiltersConfig,
}

impl RpcTransactionPartitionsConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.target_lsm_bytes == 0 {
            return Err("rpc transaction partition target_lsm_bytes must be positive");
        }
        if self.target_blob_bytes == 0 {
            return Err("rpc transaction partition target_blob_bytes must be positive");
        }
        if self.target_index_records == 0 {
            return Err("rpc transaction partition target_index_records must be positive");
        }
        if self.max_open_sealed_partitions == 0 {
            return Err("rpc transaction partition max_open_sealed_partitions must be positive");
        }
        self.filters.validate()?;
        Ok(())
    }
}

impl Default for RpcTransactionPartitionsConfig {
    fn default() -> Self {
        Self {
            target_lsm_bytes: 4 * 1024 * 1024 * 1024,
            target_blob_bytes: 16 * 1024 * 1024 * 1024,
            target_index_records: 50_000_000,
            max_open_sealed_partitions: 8,
            filters: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcTransactionFiltersConfig {
    pub transaction_false_positive_rate_ppm: u32,
    pub inbound_message_false_positive_rate_ppm: u32,
    pub block_false_positive_rate_ppm: u32,
    pub max_false_positive_rate_ppm: u32,
    pub max_concurrent_sealed_exact_lookups: usize,
    pub max_filter_bundle_bytes: u64,
}

impl RpcTransactionFiltersConfig {
    pub const MIN_FALSE_POSITIVE_RATE_PPM: u32 = 1;
    pub const MAX_FALSE_POSITIVE_RATE_PPM: u32 = 500_000;

    pub fn validate(&self) -> Result<(), &'static str> {
        for rate in [
            self.transaction_false_positive_rate_ppm,
            self.inbound_message_false_positive_rate_ppm,
            self.block_false_positive_rate_ppm,
            self.max_false_positive_rate_ppm,
        ] {
            if !(Self::MIN_FALSE_POSITIVE_RATE_PPM..=Self::MAX_FALSE_POSITIVE_RATE_PPM)
                .contains(&rate)
            {
                return Err("rpc transaction filter false-positive rate must be in 1..=500000 ppm");
            }
        }
        if [
            self.transaction_false_positive_rate_ppm,
            self.inbound_message_false_positive_rate_ppm,
            self.block_false_positive_rate_ppm,
        ]
        .into_iter()
        .any(|rate| rate > self.max_false_positive_rate_ppm)
        {
            return Err("rpc transaction filter false-positive rate must not exceed max_false_positive_rate_ppm");
        }
        if self.max_concurrent_sealed_exact_lookups == 0 {
            return Err("rpc transaction filter max_concurrent_sealed_exact_lookups must be positive");
        }
        if self.max_filter_bundle_bytes == 0 {
            return Err("rpc transaction filter max_filter_bundle_bytes must be positive");
        }
        Ok(())
    }
}

impl Default for RpcTransactionFiltersConfig {
    fn default() -> Self {
        Self {
            transaction_false_positive_rate_ppm: 1_000,
            inbound_message_false_positive_rate_ppm: 1_000,
            block_false_positive_rate_ppm: 1_000,
            max_false_positive_rate_ppm: 100_000,
            max_concurrent_sealed_exact_lookups: 8,
            max_filter_bundle_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
pub struct SubscriptionsConfig {
    pub max_clients: u32,
    pub max_addrs: u32,
    pub max_streams_per_addr: Option<NonZeroU32>,
    /// Pending updates buffered per client; clamped to at least 1.
    pub queue_depth: usize,
}

impl Default for SubscriptionsConfig {
    fn default() -> Self {
        Self {
            max_clients: 1_000_000,
            max_addrs: 1_000_000,
            max_streams_per_addr: None,
            queue_depth: 5,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionsGcConfig {
    /// Minimum transaction TTL.
    ///
    /// Default: `1 week`.
    #[serde(with = "serde_helpers::humantime")]
    pub tx_ttl: Duration,

    /// Keep at least this amount of transactions per account.
    ///
    /// Default: `10`.
    #[serde(default)]
    pub keep_tx_per_account: usize,
}

impl Default for TransactionsGcConfig {
    fn default() -> Self {
        Self {
            tx_ttl: Duration::from_secs(60 * 60 * 24 * 7),
            keep_tx_per_account: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlackListConfig {
    pub accounts: Vec<StdAddr>,
}

impl BlackListConfig {
    pub fn load_from<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        serde_helpers::load_json_from_file(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_partitions_default_is_valid() {
        assert!(RpcTransactionPartitionsConfig::default().validate().is_ok());
    }

    #[test]
    fn transaction_partitions_deserialize_with_defaults() {
        let config: RpcStorageConfig = serde_json::from_str(
            r#"{"type":"Full","gc":null,"force_reindex":false,"blacklist_path":null}"#,
        )
        .unwrap();
        assert_eq!(
            config.transaction_partitions(),
            Some(&RpcTransactionPartitionsConfig::default())
        );
    }

    #[test]
    fn transaction_partitions_reject_zero_values() {
        let mut config = RpcTransactionPartitionsConfig::default();
        config.target_lsm_bytes = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.target_blob_bytes = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.target_index_records = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.max_open_sealed_partitions = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn transaction_filters_deserialize_with_defaults() {
        let config: RpcTransactionPartitionsConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.filters, RpcTransactionFiltersConfig::default());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn transaction_filters_accept_valid_boundaries() {
        let mut config = RpcTransactionFiltersConfig::default();
        config.transaction_false_positive_rate_ppm = 1;
        config.inbound_message_false_positive_rate_ppm = 500_000;
        config.block_false_positive_rate_ppm = 500_000;
        config.max_false_positive_rate_ppm = 500_000;
        assert!(config.validate().is_ok());
        let round_trip: RpcTransactionFiltersConfig =
            serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
        assert_eq!(round_trip, config);
    }

    #[test]
    fn transaction_filters_reject_invalid_values() {
        let mut config = RpcTransactionFiltersConfig::default();
        config.transaction_false_positive_rate_ppm = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.inbound_message_false_positive_rate_ppm = 500_001;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.block_false_positive_rate_ppm = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.max_false_positive_rate_ppm = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.max_false_positive_rate_ppm = 999;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.max_concurrent_sealed_exact_lookups = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.max_filter_bundle_bytes = 0;
        assert!(config.validate().is_err());
    }
}
