use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_types::cell::HashBytes;
use everscale_types::models::StdAddr;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tycho_collator::internal_queue::queue::QueueConfig;
use tycho_collator::types::CollatorConfig;
use tycho_collator::validator::ValidatorStdImplConfig;
use tycho_consensus::prelude::MempoolNodeConfig;
use tycho_control::ControlServerConfig;
use tycho_core::block_strider::{
    ArchiveBlockProviderConfig, BlockchainBlockProviderConfig, StarterConfig,
};
use tycho_core::blockchain_rpc::BlockchainRpcServiceConfig;
use tycho_core::overlay_client::PublicOverlayClientConfig;
use tycho_network::{DhtConfig, NetworkConfig, OverlayConfig, PeerResolverConfig};
use tycho_rpc::RpcConfig;
use tycho_storage::StorageConfig;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::{LoggerConfig, LoggerOutput};

use crate::util::FpTokens;

#[derive(Debug)]
pub struct NodeKeys {
    pub secret: HashBytes,
}

impl NodeKeys {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    pub fn as_secret(&self) -> ed25519::SecretKey {
        ed25519::SecretKey::from_bytes(self.secret.0)
    }

    pub fn public_key(&self) -> ed25519::PublicKey {
        ed25519::PublicKey::from(&self.as_secret())
    }
}

impl<'de> Deserialize<'de> for NodeKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        struct PartialKeys {
            secret: HashBytes,
            #[serde(default)]
            public: Option<HashBytes>,
        }

        let partial = PartialKeys::deserialize(deserializer)?;

        let secret = ed25519::SecretKey::from_bytes(partial.secret.0);
        let public = ed25519::PublicKey::from(&secret);

        if let Some(stored_public) = partial.public {
            if stored_public.as_array() != public.as_bytes() {
                return Err(Error::custom(format!(
                    "public key mismatch (stored: {stored_public}, expected: {public})",
                )));
            }
        }

        Ok(NodeKeys {
            secret: partial.secret,
        })
    }
}

impl Serialize for NodeKeys {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct FullKeys<'a> {
            secret: &'a HashBytes,
            public: &'a HashBytes,
        }

        let secret = ed25519::SecretKey::from_bytes(self.secret.0);
        let public = ed25519::PublicKey::from(&secret);

        FullKeys {
            secret: &self.secret,
            public: HashBytes::wrap(public.as_bytes()),
        }
        .serialize(serializer)
    }
}

impl rand::distributions::Distribution<NodeKeys> for rand::distributions::Standard {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> NodeKeys {
        NodeKeys {
            secret: rand::distributions::Standard.sample(rng),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ty")]
pub enum ElectionsConfig {
    Simple(SimpleElectionsConfig),
}

impl ElectionsConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }
}

#[derive(Debug, Clone)]
pub struct SimpleElectionsConfig {
    pub wallet_secret: HashBytes,
    pub wallet_address: StdAddr,
    pub stake: Option<FpTokens>,
    pub stake_factor: Option<u32>,
}

impl SimpleElectionsConfig {
    pub fn compute_wallet_address(public: &ed25519::PublicKey) -> StdAddr {
        crate::util::wallet::compute_address(-1, public)
    }

    pub fn from_key(
        secret: &ed25519::SecretKey,
        stake: Option<FpTokens>,
        stake_factor: Option<u32>,
    ) -> Self {
        let public = ed25519::PublicKey::from(secret);

        Self {
            wallet_secret: HashBytes(secret.to_bytes()),
            wallet_address: Self::compute_wallet_address(&public),
            stake,
            stake_factor,
        }
    }

    pub fn as_secret(&self) -> ed25519::SecretKey {
        ed25519::SecretKey::from_bytes(self.wallet_secret.0)
    }

    pub fn public_key(&self) -> ed25519::PublicKey {
        ed25519::PublicKey::from(&self.as_secret())
    }
}

impl<'de> serde::Deserialize<'de> for SimpleElectionsConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        struct PartialConfig {
            wallet_secret: HashBytes,
            #[serde(default)]
            wallet_public: Option<HashBytes>,
            #[serde(default)]
            wallet_address: Option<StdAddr>,

            #[serde(default)]
            stake: Option<FpTokens>,
            #[serde(default)]
            stake_factor: Option<u32>,
        }

        let partial = PartialConfig::deserialize(deserializer)?;

        let secret = ed25519::SecretKey::from_bytes(partial.wallet_secret.0);
        let public = ed25519::PublicKey::from(&secret);

        if let Some(stored_public) = partial.wallet_public {
            if stored_public.as_array() != public.as_bytes() {
                return Err(Error::custom(format!(
                    "public key mismatch (stored: {stored_public}, expected: {public})",
                )));
            }
        }

        let wallet_address = Self::compute_wallet_address(&public);
        if let Some(stored_wallet) = partial.wallet_address {
            if stored_wallet != wallet_address {
                return Err(Error::custom(format!(
                    "wallet address mismatch (stored: {stored_wallet}, expected: {wallet_address})",
                )));
            }
        }

        Ok(Self {
            wallet_secret: partial.wallet_secret,
            wallet_address,
            stake: partial.stake,
            stake_factor: partial.stake_factor,
        })
    }
}

impl serde::Serialize for SimpleElectionsConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct FullConfig<'a> {
            wallet_secret: &'a HashBytes,
            wallet_public: &'a HashBytes,
            wallet_address: &'a StdAddr,
            #[serde(skip_serializing_if = "Option::is_none")]
            stake: Option<FpTokens>,
            #[serde(skip_serializing_if = "Option::is_none")]
            stake_factor: Option<u32>,
        }

        let public = self.public_key();

        FullConfig {
            wallet_secret: &self.wallet_secret,
            wallet_public: HashBytes::wrap(public.as_bytes()),
            wallet_address: &self.wallet_address,
            stake: self.stake,
            stake_factor: self.stake_factor,
        }
        .serialize(serializer)
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

    pub starter: StarterConfig,

    pub blockchain_rpc_service: BlockchainRpcServiceConfig,

    pub blockchain_block_provider: BlockchainBlockProviderConfig,

    pub archive_block_provider: ArchiveBlockProviderConfig,

    pub collator: CollatorConfig,

    pub mempool: MempoolNodeConfig,

    pub internal_queue: QueueConfig,

    pub validator: ValidatorStdImplConfig,

    pub rpc: Option<RpcConfig>,

    pub control: ControlServerConfig,

    pub metrics: Option<MetricsConfig>,

    pub threads: ThreadPoolConfig,

    pub profiling: MemoryProfilingConfig,

    pub logger: LoggerConfig,
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
            starter: StarterConfig::default(),
            blockchain_rpc_service: BlockchainRpcServiceConfig::default(),
            blockchain_block_provider: BlockchainBlockProviderConfig::default(),
            archive_block_provider: ArchiveBlockProviderConfig::default(),
            collator: CollatorConfig::default(),
            mempool: MempoolNodeConfig::default(),
            validator: ValidatorStdImplConfig::default(),
            rpc: Some(RpcConfig::default()),
            control: Default::default(),
            metrics: Some(MetricsConfig::default()),
            threads: ThreadPoolConfig::default(),
            profiling: Default::default(),
            logger: Default::default(),
            internal_queue: QueueConfig::default(),
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

    pub fn with_relative_paths<P: AsRef<Path>>(mut self, base_dir: P) -> Self {
        let base_dir = base_dir.as_ref();

        self.storage.root_dir = base_dir.join(self.storage.root_dir);
        self.profiling.profiling_dir = base_dir.join(self.profiling.profiling_dir);
        for output in &mut self.logger.outputs {
            if let LoggerOutput::File(output) = output {
                output.dir = base_dir.join(&output.dir);
            }
        }

        self
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryProfilingConfig {
    pub profiling_dir: PathBuf,
}
