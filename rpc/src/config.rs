use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_types::models::StdAddr;
use tycho_util::config::PartialConfig;
use tycho_util::serde_helpers;

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
            storage: RpcStorageConfig::Full {
                gc: Some(Default::default()),
                force_reindex: false,
                blacklist_path: None,
            },
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
