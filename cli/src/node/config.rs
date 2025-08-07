use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tycho_collator::internal_queue::queue::QueueConfig;
use tycho_collator::types::CollatorConfig;
use tycho_collator::validator::ValidatorStdImplConfig;
use tycho_consensus::prelude::MempoolNodeConfig;
use tycho_control::ControlServerConfig;
use tycho_core::node::NodeBaseConfig;
use tycho_crypto::ed25519;
use tycho_rpc::RpcConfig;
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::{LoggerConfig, LoggerOutput};
use tycho_util::cli::metrics::MetricsConfig;
use tycho_util::config::PartialConfig;

use crate::util::FpTokens;

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

        if let Some(stored_public) = partial.wallet_public
            && stored_public.as_array() != public.as_bytes()
        {
            return Err(Error::custom(format!(
                "public key mismatch (stored: {stored_public}, expected: {public})",
            )));
        }

        let wallet_address = Self::compute_wallet_address(&public);
        if let Some(stored_wallet) = partial.wallet_address
            && stored_wallet != wallet_address
        {
            return Err(Error::custom(format!(
                "wallet address mismatch (stored: {stored_wallet}, expected: {wallet_address})",
            )));
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

#[derive(Debug, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
pub struct NodeConfig {
    #[partial]
    #[serde(flatten)]
    pub base: NodeBaseConfig,

    #[partial]
    pub collator: CollatorConfig,

    pub mempool: MempoolNodeConfig,

    pub internal_queue: QueueConfig,

    pub validator: ValidatorStdImplConfig,

    #[partial]
    pub rpc: Option<RpcConfig>,

    pub control: ControlServerConfig,

    #[important]
    pub metrics: Option<MetricsConfig>,

    #[important]
    pub threads: ThreadPoolConfig,

    pub profiling: MemoryProfilingConfig,

    #[important]
    pub logger: LoggerConfig,
}

impl Default for NodeConfig {
    #[inline]
    fn default() -> Self {
        Self {
            base: Default::default(),
            collator: Default::default(),
            mempool: Default::default(),
            internal_queue: Default::default(),
            validator: Default::default(),
            rpc: Some(Default::default()),
            control: Default::default(),
            metrics: Some(Default::default()),
            threads: Default::default(),
            profiling: Default::default(),
            logger: Default::default(),
        }
    }
}

impl std::ops::Deref for NodeConfig {
    type Target = NodeBaseConfig;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl std::ops::DerefMut for NodeConfig {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
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

        self.base = self.base.with_relative_paths(base_dir);
        self.profiling.profiling_dir = base_dir.join(self.profiling.profiling_dir);
        for output in &mut self.logger.outputs {
            if let LoggerOutput::File(output) = output {
                output.dir = base_dir.join(&output.dir);
            }
        }

        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryProfilingConfig {
    pub profiling_dir: PathBuf,
}
