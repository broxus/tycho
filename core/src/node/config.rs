use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tycho_network::{DhtConfig, NetworkConfig, OverlayConfig, PeerResolverConfig};
use tycho_storage::StorageConfig;
use tycho_util::config::PartialConfig;

use crate::block_strider::{
    ArchiveBlockProviderConfig, BlockchainBlockProviderConfig, StarterConfig,
};
use crate::blockchain_rpc::{BlockchainRpcClientConfig, BlockchainRpcServiceConfig};
use crate::overlay_client::PublicOverlayClientConfig;
#[cfg(feature = "s3")]
use crate::s3::S3ClientConfig;
use crate::storage::CoreStorageConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
pub struct NodeBaseConfig {
    /// Public IP address of the node.
    ///
    /// Default: resolved automatically.
    #[important]
    pub public_ip: Option<IpAddr>,

    /// Ip address to listen on.
    ///
    /// Default: 0.0.0.0
    #[important]
    pub local_ip: IpAddr,

    /// Default: 30000.
    #[important]
    pub port: u16,

    pub network: NetworkConfig,

    pub dht: DhtConfig,

    pub peer_resolver: PeerResolverConfig,

    pub overlay: OverlayConfig,

    pub public_overlay_client: PublicOverlayClientConfig,

    #[partial]
    pub storage: StorageConfig,

    #[partial]
    pub core_storage: CoreStorageConfig,

    #[partial]
    pub starter: StarterConfig,

    pub blockchain_rpc_client: BlockchainRpcClientConfig,

    pub blockchain_rpc_service: BlockchainRpcServiceConfig,

    pub archive_block_provider: ArchiveBlockProviderConfig,

    pub blockchain_block_provider: BlockchainBlockProviderConfig,

    #[cfg(feature = "s3")]
    pub s3_client: Option<S3ClientConfig>,
}

impl Default for NodeBaseConfig {
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
            starter: StarterConfig::default(),
            blockchain_rpc_client: BlockchainRpcClientConfig::default(),
            blockchain_rpc_service: BlockchainRpcServiceConfig::default(),
            blockchain_block_provider: BlockchainBlockProviderConfig::default(),
            archive_block_provider: ArchiveBlockProviderConfig::default(),

            #[cfg(feature = "s3")]
            s3_client: None,
        }
    }
}

impl NodeBaseConfig {
    pub fn with_relative_paths<P: AsRef<Path>>(mut self, base_dir: P) -> Self {
        let base_dir = base_dir.as_ref();
        self.storage.root_dir = base_dir.join(self.storage.root_dir);
        self
    }
}
