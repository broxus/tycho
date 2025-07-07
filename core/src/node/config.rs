use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tycho_network::{DhtConfig, NetworkConfig, OverlayConfig, PeerResolverConfig};
use tycho_storage::StorageConfig;

use crate::block_strider::{
    ArchiveBlockProviderConfig, BlockchainBlockProviderConfig, StarterConfig,
};
use crate::blockchain_rpc::{BlockchainRpcClientConfig, BlockchainRpcServiceConfig};
use crate::overlay_client::PublicOverlayClientConfig;
use crate::storage::CoreStorageConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeBaseConfig {
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

    pub starter: StarterConfig,

    pub blockchain_rpc_client: BlockchainRpcClientConfig,

    pub blockchain_rpc_service: BlockchainRpcServiceConfig,

    pub archive_block_provider: ArchiveBlockProviderConfig,

    pub blockchain_block_provider: BlockchainBlockProviderConfig,
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
