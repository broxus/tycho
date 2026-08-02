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
        /// Default: clear all transactions older than `1 week`, starting GC no more often than
        /// every `10 minutes`.
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
#[serde(default, deny_unknown_fields)]
pub struct RpcTransactionPartitionsConfig {
    pub target_transaction_lsm_bytes: u64,
    pub target_transaction_blob_bytes: u64,
    pub target_transaction_index_records: u64,
    pub target_block_metadata_bytes: u64,
    pub max_open_sealed_partitions: usize,
    #[serde(default)]
    pub filters: RpcTransactionFiltersConfig,
    #[serde(default)]
    pub maintenance: RpcTransactionMaintenanceConfig,
}

impl RpcTransactionPartitionsConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.target_transaction_lsm_bytes == 0 {
            return Err("rpc transaction partition target_transaction_lsm_bytes must be positive");
        }
        if self.target_transaction_blob_bytes == 0 {
            return Err("rpc transaction partition target_transaction_blob_bytes must be positive");
        }
        if self.target_transaction_index_records == 0 {
            return Err("rpc transaction partition target_transaction_index_records must be positive");
        }
        if self.target_block_metadata_bytes == 0 {
            return Err("rpc transaction partition target_block_metadata_bytes must be positive");
        }
        if self.max_open_sealed_partitions == 0 {
            return Err("rpc transaction partition max_open_sealed_partitions must be positive");
        }
        self.filters.validate()?;
        self.maintenance.validate()?;
        Ok(())
    }
}

impl Default for RpcTransactionPartitionsConfig {
    fn default() -> Self {
        Self {
            target_transaction_lsm_bytes: 4 * 1024 * 1024 * 1024,
            target_transaction_blob_bytes: 16 * 1024 * 1024 * 1024,
            target_transaction_index_records: 50_000_000,
            target_block_metadata_bytes: 1024 * 1024 * 1024,
            max_open_sealed_partitions: 8,
            filters: Default::default(),
            maintenance: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcTransactionFiltersConfig {
    pub transaction_false_positive_rate_ppm: u32,
    pub inbound_message_false_positive_rate_ppm: u32,
    pub block_false_positive_rate_ppm: u32,
    pub account_false_positive_rate_ppm: u32,
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
            self.account_false_positive_rate_ppm,
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
            self.account_false_positive_rate_ppm,
        ]
        .into_iter()
        .any(|rate| rate > self.max_false_positive_rate_ppm)
        {
            return Err("rpc transaction filter false-positive rate must not exceed max_false_positive_rate_ppm");
        }
        if self.max_concurrent_sealed_exact_lookups == 0 {
            return Err("rpc transaction filter max_concurrent_sealed_exact_lookups must be positive");
        }
        if self.max_concurrent_sealed_exact_lookups > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("rpc transaction filter max_concurrent_sealed_exact_lookups exceeds Tokio semaphore limit");
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
            account_false_positive_rate_ppm: 1_000,
            max_false_positive_rate_ppm: 100_000,
            max_concurrent_sealed_exact_lookups: 8,
            max_filter_bundle_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RpcTransactionMaintenanceConfig {
    pub max_concurrent_tasks: usize,
    pub gc_accounts_per_chunk: usize,
    pub gc_max_staged_bytes_per_batch: u64,
    pub tail_sweep_records_per_batch: usize,
}

impl RpcTransactionMaintenanceConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_concurrent_tasks == 0 {
            return Err("rpc transaction maintenance max_concurrent_tasks must be positive");
        }
        if self.gc_accounts_per_chunk == 0 {
            return Err("rpc transaction maintenance gc_accounts_per_chunk must be positive");
        }
        if self.gc_accounts_per_chunk > 128 {
            return Err("rpc transaction maintenance gc_accounts_per_chunk must not exceed 128");
        }
        if self.gc_max_staged_bytes_per_batch == 0 {
            return Err("rpc transaction maintenance gc_max_staged_bytes_per_batch must be positive");
        }
        if self.tail_sweep_records_per_batch == 0 {
            return Err("rpc transaction maintenance tail_sweep_records_per_batch must be positive");
        }
        Ok(())
    }
}

impl Default for RpcTransactionMaintenanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 1,
            gc_accounts_per_chunk: 128,
            gc_max_staged_bytes_per_batch: 16 * 1024 * 1024,
            tail_sweep_records_per_batch: 1024,
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
    #[serde(default = "default_transactions_gc_keep_tx_per_account")]
    pub keep_tx_per_account: usize,

    /// Minimum interval between transaction GC pass starts.
    ///
    /// Default: `10 minutes`.
    #[serde(default = "default_transactions_gc_min_interval", with = "serde_helpers::humantime")]
    pub min_interval: Duration,
}

fn default_transactions_gc_min_interval() -> Duration {
    Duration::from_secs(60 * 10)
}

fn default_transactions_gc_keep_tx_per_account() -> usize {
    10
}

impl Default for TransactionsGcConfig {
    fn default() -> Self {
        Self {
            tx_ttl: Duration::from_secs(60 * 60 * 24 * 7),
            keep_tx_per_account: default_transactions_gc_keep_tx_per_account(),
            min_interval: default_transactions_gc_min_interval(),
        }
    }
}

impl TransactionsGcConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.tx_ttl.is_zero() {
            return Err("rpc transactions GC tx_ttl must be positive");
        }
        if self.tx_ttl.subsec_nanos() != 0 {
            return Err("rpc transactions GC tx_ttl must use whole seconds");
        }
        if self.min_interval.is_zero() {
            return Err("rpc transactions GC min_interval must be positive");
        }
        u64::try_from(self.keep_tx_per_account)
            .map_err(|_| "rpc transactions GC keep_tx_per_account must fit u64")?;
        Ok(())
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
        let config = RpcTransactionPartitionsConfig::default();
        assert_eq!(config.target_transaction_lsm_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(config.target_transaction_blob_bytes, 16 * 1024 * 1024 * 1024);
        assert_eq!(config.target_transaction_index_records, 50_000_000);
        assert_eq!(config.target_block_metadata_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.filters.account_false_positive_rate_ppm, 1_000);
        assert_eq!(config.maintenance.max_concurrent_tasks, 1);
        assert_eq!(config.maintenance.gc_accounts_per_chunk, 128);
        assert_eq!(config.maintenance.gc_max_staged_bytes_per_batch, 16 * 1024 * 1024);
        assert_eq!(config.maintenance.tail_sweep_records_per_batch, 1024);
        assert!(config.validate().is_ok());
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
    fn transaction_partitions_deserialize_new_targets_and_accept_max_values() {
        let config: RpcTransactionPartitionsConfig = serde_json::from_str(
            r#"{
                "target_transaction_lsm_bytes":11,
                "target_transaction_blob_bytes":12,
                "target_transaction_index_records":13,
                "target_block_metadata_bytes":14
            }"#,
        )
        .unwrap();
        assert_eq!(config.target_transaction_lsm_bytes, 11);
        assert_eq!(config.target_transaction_blob_bytes, 12);
        assert_eq!(config.target_transaction_index_records, 13);
        assert_eq!(config.target_block_metadata_bytes, 14);

        let mut max = RpcTransactionPartitionsConfig::default();
        max.target_transaction_lsm_bytes = u64::MAX;
        max.target_transaction_blob_bytes = u64::MAX;
        max.target_transaction_index_records = u64::MAX;
        max.target_block_metadata_bytes = u64::MAX;
        max.max_open_sealed_partitions = usize::MAX;
        max.filters.transaction_false_positive_rate_ppm = RpcTransactionFiltersConfig::MAX_FALSE_POSITIVE_RATE_PPM;
        max.filters.inbound_message_false_positive_rate_ppm = RpcTransactionFiltersConfig::MAX_FALSE_POSITIVE_RATE_PPM;
        max.filters.block_false_positive_rate_ppm = RpcTransactionFiltersConfig::MAX_FALSE_POSITIVE_RATE_PPM;
        max.filters.account_false_positive_rate_ppm = RpcTransactionFiltersConfig::MAX_FALSE_POSITIVE_RATE_PPM;
        max.filters.max_false_positive_rate_ppm = RpcTransactionFiltersConfig::MAX_FALSE_POSITIVE_RATE_PPM;
        max.filters.max_concurrent_sealed_exact_lookups = tokio::sync::Semaphore::MAX_PERMITS;
        max.filters.max_filter_bundle_bytes = u64::MAX;
        max.maintenance.max_concurrent_tasks = usize::MAX;
        max.maintenance.gc_accounts_per_chunk = 128;
        max.maintenance.gc_max_staged_bytes_per_batch = u64::MAX;
        max.maintenance.tail_sweep_records_per_batch = usize::MAX;
        assert!(max.validate().is_ok());
        assert!(TransactionsGcConfig {
            tx_ttl: Duration::from_secs(u64::MAX),
            keep_tx_per_account: usize::MAX,
            min_interval: Duration::from_nanos(1),
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn transaction_partitions_reject_zero_values() {
        let mut config = RpcTransactionPartitionsConfig::default();
        config.target_transaction_lsm_bytes = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.target_transaction_blob_bytes = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.target_transaction_index_records = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.target_block_metadata_bytes = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionPartitionsConfig::default();
        config.max_open_sealed_partitions = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn transaction_partitions_reject_old_target_names() {
        for old_name in ["target_lsm_bytes", "target_blob_bytes", "target_index_records"] {
            let json = format!(r#"{{"{old_name}":1}}"#);
            assert!(serde_json::from_str::<RpcTransactionPartitionsConfig>(&json).is_err());
        }
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
        config.account_false_positive_rate_ppm = 500_000;
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
        config.account_false_positive_rate_ppm = 500_001;
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
        config.max_concurrent_sealed_exact_lookups = tokio::sync::Semaphore::MAX_PERMITS + 1;
        assert!(config.validate().is_err());
        config = RpcTransactionFiltersConfig::default();
        config.max_filter_bundle_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn transaction_maintenance_deserialize_with_defaults() {
        let config: RpcTransactionPartitionsConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.maintenance, RpcTransactionMaintenanceConfig::default());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn transaction_maintenance_reject_zero_values() {
        let mut config = RpcTransactionMaintenanceConfig::default();
        config.max_concurrent_tasks = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionMaintenanceConfig::default();
        config.gc_accounts_per_chunk = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionMaintenanceConfig::default();
        config.gc_accounts_per_chunk = 129;
        assert!(config.validate().is_err());
        config = RpcTransactionMaintenanceConfig::default();
        config.gc_max_staged_bytes_per_batch = 0;
        assert!(config.validate().is_err());
        config = RpcTransactionMaintenanceConfig::default();
        config.tail_sweep_records_per_batch = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn transactions_gc_validates_fixed_codec_boundaries() {
        let mut config = TransactionsGcConfig::default();
        assert_eq!(config.tx_ttl, Duration::from_secs(60 * 60 * 24 * 7));
        assert_eq!(config.keep_tx_per_account, 10);
        assert_eq!(config.min_interval, Duration::from_secs(60 * 10));
        assert!(config.validate().is_ok());
        config.keep_tx_per_account = usize::MAX;
        assert!(config.validate().is_ok());
        config.tx_ttl = Duration::ZERO;
        assert!(config.validate().is_err());
        config.tx_ttl = Duration::from_millis(1500);
        assert!(config.validate().is_err());
        config.tx_ttl = Duration::from_secs(1);
        config.min_interval = Duration::ZERO;
        assert!(config.validate().is_err());
        config.min_interval = Duration::from_nanos(1);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn transactions_gc_min_interval_serde_is_backward_compatible() {
        let config: TransactionsGcConfig = serde_json::from_str(r#"{"tx_ttl":"1h"}"#).unwrap();
        assert_eq!(config.tx_ttl, Duration::from_secs(60 * 60));
        assert_eq!(config.keep_tx_per_account, 10);
        assert_eq!(config.min_interval, Duration::from_secs(60 * 10));

        let config: TransactionsGcConfig = serde_json::from_str(
            r#"{"tx_ttl":"1h","keep_tx_per_account":7,"min_interval":"250ms"}"#,
        )
        .unwrap();
        assert_eq!(config.min_interval, Duration::from_millis(250));
        assert_eq!(
            serde_json::from_str::<TransactionsGcConfig>(&serde_json::to_string(&config).unwrap())
                .unwrap(),
            config,
        );
    }

    #[test]
    fn transaction_config_serde_rejects_numeric_overflow() {
        assert!(serde_json::from_str::<RpcTransactionPartitionsConfig>(
            r#"{"target_transaction_lsm_bytes":18446744073709551616}"#,
        )
        .is_err());
        assert!(serde_json::from_str::<RpcTransactionFiltersConfig>(
            r#"{"transaction_false_positive_rate_ppm":4294967296}"#,
        )
        .is_err());

        let usize_overflow = usize::MAX as u128 + 1;
        let maintenance = format!(r#"{{"max_concurrent_tasks":{usize_overflow}}}"#);
        assert!(serde_json::from_str::<RpcTransactionMaintenanceConfig>(&maintenance).is_err());
        let gc = format!(
            r#"{{"tx_ttl":"1s","keep_tx_per_account":{usize_overflow}}}"#,
        );
        assert!(serde_json::from_str::<TransactionsGcConfig>(&gc).is_err());
        assert!(serde_json::from_str::<TransactionsGcConfig>(
            r#"{"tx_ttl":"18446744073709551616s"}"#,
        )
        .is_err());
        assert!(serde_json::from_str::<TransactionsGcConfig>(
            r#"{"tx_ttl":"1s","min_interval":"invalid"}"#,
        )
        .is_err());
        assert!(serde_json::from_str::<TransactionsGcConfig>(
            r#"{"tx_ttl":"1s","min_interval":"18446744073709551616s"}"#,
        )
        .is_err());
    }
}
