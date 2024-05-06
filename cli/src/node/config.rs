use std::net::Ipv4Addr;
use std::path::Path;

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_types::cell::HashBytes;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::BlockchainBlockProviderConfig;
use tycho_core::blockchain_rpc::BlockchainRpcServiceConfig;
use tycho_core::overlay_client::PublicOverlayClientConfig;
use tycho_network::{DhtConfig, NetworkConfig, OverlayConfig, PeerResolverConfig};
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
    pub public_ip: Option<Ipv4Addr>,

    /// Ip address to listen on.
    ///
    /// Default: 0.0.0.0
    pub local_ip: Ipv4Addr,

    /// Default: 30000.
    pub port: u16,

    pub network: NetworkConfig,

    pub dht: DhtConfig,

    pub peer_resolver: PeerResolverConfig,

    pub overlay: OverlayConfig,

    pub public_overlay_client: PublicOverlayClientConfig,

    pub storage: StorageConfig,

    pub blockchain_rpc_service: BlockchainRpcServiceConfig,

    pub blockchain_block_provider: BlockchainBlockProviderConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            public_ip: None,
            local_ip: Ipv4Addr::UNSPECIFIED,
            port: 30000,
            network: NetworkConfig::default(),
            dht: DhtConfig::default(),
            peer_resolver: PeerResolverConfig::default(),
            overlay: OverlayConfig::default(),
            public_overlay_client: PublicOverlayClientConfig::default(),
            storage: StorageConfig::default(),
            blockchain_rpc_service: BlockchainRpcServiceConfig::default(),
            blockchain_block_provider: BlockchainBlockProviderConfig::default(),
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