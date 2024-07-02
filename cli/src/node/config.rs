use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_types::cell::HashBytes;
use serde::{Deserialize, Serialize};
use tycho_collator::types::CollationConfig;
use tycho_core::block_strider::{ArchiveBlockProviderConfig, BlockchainBlockProviderConfig};
use tycho_core::blockchain_rpc::BlockchainRpcServiceConfig;
use tycho_core::overlay_client::PublicOverlayClientConfig;
use tycho_network::{DhtConfig, NetworkConfig, OverlayConfig, PeerResolverConfig};
use tycho_rpc::RpcConfig;
use tycho_storage::StorageConfig;

#[derive(Debug, Deserialize)]
pub struct NodeKeys {
    pub secret: HashBytes,
}

impl NodeKeys {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub fn as_secret(&self) -> ed25519::SecretKey {
        ed25519::SecretKey::from_bytes(self.secret.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    /// Public IP address of the node.
    ///
    /// Default: resolved automatically.
    pub public_ip: Option<IpAddr>,

    /// Ip address to listen on.
    ///
    /// Default: 0.0.0.0
    pub local_ip: IpAddr,

    /// Default: 30000.
    pub port: u16,

    pub network: NetworkConfig,

    pub dht: DhtConfig,

    pub peer_resolver: PeerResolverConfig,

    pub overlay: OverlayConfig,

    pub public_overlay_client: PublicOverlayClientConfig,

    pub storage: StorageConfig,

    pub blockchain_rpc_service: BlockchainRpcServiceConfig,

    pub archive_block_provider: ArchiveBlockProviderConfig,

    pub blockchain_block_provider: BlockchainBlockProviderConfig,

    pub collator: CollationConfig,

    pub rpc: Option<RpcConfig>,

    pub metrics: Option<MetricsConfig>,

    pub threads: ThreadPoolConfig,

    pub profiling: MemoryProfilingConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            public_ip: None,
            local_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 30000,
            network: NetworkConfig::default(),
            dht: DhtConfig::default(),
            peer_resolver: PeerResolverConfig::default(),
            overlay: OverlayConfig::default(),
            public_overlay_client: PublicOverlayClientConfig::default(),
            storage: StorageConfig::default(),
            blockchain_rpc_service: BlockchainRpcServiceConfig::default(),
            archive_block_provider: ArchiveBlockProviderConfig::default(),
            blockchain_block_provider: BlockchainBlockProviderConfig::default(),
            collator: CollationConfig::default(),
            rpc: Some(RpcConfig::default()),
            metrics: Some(MetricsConfig::default()),
            threads: ThreadPoolConfig::default(),
            profiling: Default::default(),
        }
    }
}

impl NodeConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Listen address of metrics. Used by the client to gather prometheus metrics.
    /// Default: `127.0.0.1:10000`
    #[serde(with = "tycho_util::serde_helpers::string")]
    pub listen_addr: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ThreadPoolConfig {
    pub rayon_threads: usize,
    pub tokio_workers: usize,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        let total_threads = std::thread::available_parallelism()
            .expect("failed to get total threads")
            .get();
        Self {
            rayon_threads: total_threads,
            tokio_workers: total_threads,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryProfilingConfig {
    pub profiling_dir: PathBuf,
}
