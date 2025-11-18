use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{
    ArchiveBlockProviderConfig, BlockchainBlockProviderConfig, StarterConfig,
};
use tycho_core::blockchain_rpc::{BlockchainRpcClientConfig, BlockchainRpcServiceConfig};
pub use tycho_core::node::NodeKeys;
use tycho_core::overlay_client::PublicOverlayClientConfig;
#[cfg(feature = "s3")]
use tycho_core::s3::S3ClientConfig;
use tycho_core::storage::CoreStorageConfig;
use tycho_network::{DhtConfig, NetworkConfig, OverlayConfig, PeerResolverConfig};
use tycho_rpc::RpcConfig;
use tycho_storage::StorageConfig;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::LoggerConfig;
use tycho_util::cli::metrics::MetricsConfig;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct NodeConfig<T> {
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

    pub core_storage: CoreStorageConfig,

    pub blockchain_rpc_client: BlockchainRpcClientConfig,

    pub blockchain_rpc_service: BlockchainRpcServiceConfig,

    pub blockchain_block_provider: BlockchainBlockProviderConfig,

    pub archive_block_provider: ArchiveBlockProviderConfig,

    pub rpc: Option<RpcConfig>,

    pub metrics: Option<MetricsConfig>,

    pub threads: ThreadPoolConfig,

    pub profiling: MemoryProfilingConfig,

    pub logger_config: LoggerConfig,

    pub starter: StarterConfig,

    #[cfg(feature = "s3")]
    pub s3_client: Option<S3ClientConfig>,

    #[serde(flatten)]
    pub user_config: T,
}

impl<T> Default for NodeConfig<T>
where
    T: Default,
{
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
            core_storage: CoreStorageConfig::default(),
            blockchain_rpc_client: BlockchainRpcClientConfig::default(),
            blockchain_rpc_service: BlockchainRpcServiceConfig::default(),
            blockchain_block_provider: BlockchainBlockProviderConfig::default(),
            archive_block_provider: Default::default(),
            rpc: Some(RpcConfig::default()),
            metrics: Some(MetricsConfig::default()),
            threads: ThreadPoolConfig::default(),
            profiling: Default::default(),
            logger_config: Default::default(),
            starter: Default::default(),

            #[cfg(feature = "s3")]
            s3_client: None,

            user_config: Default::default(),
        }
    }
}

impl<T> NodeConfig<T>
where
    T: Serialize + DeserializeOwned + Default,
{
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<NodeConfig<T>> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryProfilingConfig {
    pub profiling_dir: PathBuf,
}
