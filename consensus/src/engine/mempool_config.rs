use std::num::{NonZeroU8, NonZeroU16};
use std::sync::OnceLock;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tycho_crypto::ed25519::{KeyPair, SecretKey};
use tycho_network::{OverlayId, PeerId};
use tycho_types::models::{ConsensusConfig, GenesisInfo};

use crate::dag::AnchorStage;
use crate::models::{Link, Point, PointData, Round, UnixTime};

// replace with `ArcSwapOption` + copy on get() if need to change in runtime
static NODE_CONFIG: OnceLock<MempoolNodeConfig> = OnceLock::new();
pub struct NodeConfig;
impl NodeConfig {
    pub fn get() -> &'static MempoolNodeConfig {
        (NODE_CONFIG.get()).expect("mempool node config not initialized")
    }
}

/// values that can be changed in runtime via key block, private to crate
#[derive(Clone, Debug, PartialEq)]
pub struct MempoolConfig {
    pub consensus: ConsensusConfig,
    pub genesis_round: Round,
    /// Estimated hard limit on serialized point size
    pub point_max_bytes: usize,
}

/// visible outside crate and used as DTO
#[derive(Clone, Debug)]
pub struct MempoolMergedConfig {
    pub genesis_info: GenesisInfo,
    pub conf: MempoolConfig,
    pub(crate) overlay_id: OverlayId,
}

impl MempoolMergedConfig {
    pub(crate) fn genesis_author(&self) -> PeerId {
        let key_pair = KeyPair::from(&SecretKey::from_bytes(self.overlay_id.0));
        key_pair.public_key.into()
    }

    pub(crate) fn genesis(&self) -> Point {
        let key_pair = KeyPair::from(&SecretKey::from_bytes(self.overlay_id.0));
        let millis = UnixTime::from_millis(self.genesis_info.genesis_millis);
        Point::new(
            &key_pair,
            PeerId::from(key_pair.public_key),
            self.conf.genesis_round,
            Default::default(),
            PointData {
                time: millis,
                includes: Default::default(),
                witness: Default::default(),
                evidence: Default::default(),
                anchor_trigger: Link::ToSelf,
                anchor_proof: Link::ToSelf,
                anchor_time: millis,
            },
            &self.conf,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MempoolConfigBuilder {
    genesis_info: Option<GenesisInfo>,
    consensus_config: Option<ConsensusConfig>,
}

impl MempoolConfigBuilder {
    pub fn new(node_config: &MempoolNodeConfig) -> Self {
        if NODE_CONFIG.get_or_init(|| node_config.clone()) != node_config {
            tracing::error!(
                "mempool node config was not changed; using prev {:?} ignored new {:?}",
                NodeConfig::get(),
                node_config,
            );
        };
        Self {
            genesis_info: None,
            consensus_config: None,
        }
    }

    pub fn set_consensus_config(&mut self, consensus_config: &ConsensusConfig) -> Result<()> {
        ensure!(
            consensus_config.max_consensus_lag_rounds >= consensus_config.commit_history_rounds,
            "max consensus lag must be greater than commit depth"
        );

        ensure!(
            consensus_config.payload_buffer_bytes >= consensus_config.payload_batch_bytes,
            "no need to evict cached externals if can send them in one message"
        );

        self.consensus_config = Some(consensus_config.clone());
        Ok(())
    }

    pub fn set_genesis(&mut self, info: GenesisInfo) {
        self.genesis_info = Some(info);
    }

    pub fn get_consensus_config(&self) -> Option<&ConsensusConfig> {
        self.consensus_config.as_ref()
    }

    pub fn get_genesis(&self) -> Option<GenesisInfo> {
        self.genesis_info
    }

    pub fn build(&self) -> Result<MempoolMergedConfig> {
        let genesis_info = *self
            .genesis_info
            .as_ref()
            .context("mempool genesis info for config is not known")?;

        let consensus = self
            .consensus_config
            .as_ref()
            .context("mempool consensus config is not known")?;

        let genesis_round = AnchorStage::align_genesis(genesis_info.start_round);

        let mempool_config = MempoolConfig {
            consensus: consensus.clone(),
            genesis_round,
            point_max_bytes: Point::max_byte_size(consensus),
        };

        // reset types to u128 as it does not match fields in `ConsensusConfig`
        // and may be changed just to keep them handy, that must not affect hash
        let mut hasher = blake3::Hasher::new();
        // unaligned `genesis_info.start_round` is not used
        hasher.update(&(genesis_round.0 as u128).to_be_bytes());
        hasher.update(&(genesis_info.genesis_millis as u128).to_be_bytes());
        hasher.update(&(consensus.clock_skew_millis as u128).to_be_bytes());
        hasher.update(&(consensus.payload_batch_bytes as u128).to_be_bytes());
        hasher.update(&(consensus.commit_history_rounds as u128).to_be_bytes());
        hasher.update(&(consensus.deduplicate_rounds as u128).to_be_bytes());
        hasher.update(&(consensus.max_consensus_lag_rounds as u128).to_be_bytes());
        // TODO add comment in tycho-types
        hasher.update(&(consensus.sync_support_rounds as u128).to_be_bytes());

        let overlay_id = OverlayId(hasher.finalize().into());

        Ok(MempoolMergedConfig {
            genesis_info,
            conf: mempool_config,
            overlay_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MempoolNodeConfig {
    /// `true` to truncate hashes, signatures and use non-standard format for large structs
    /// that may be more readable
    pub log_truncate_long_values: bool,

    /// How often (in rounds) delete obsolete data and trigger rocksDB compaction.
    pub clean_db_period_rounds: NonZeroU16,

    /// amount of future [Round]s (beyond current [`Dag`](crate::dag::DagHead))
    /// that [`BroadcastFilter`](crate::intercom::BroadcastFilter) caches
    /// to extend [`Dag`](crate::engine::ConsensusConfigExt) without downloading points
    pub cache_future_broadcasts_rounds: u16,

    /// [`Tokio default upper limit`](tokio::runtime::Builder::max_blocking_threads) is 512.
    ///
    /// [`Tycho tread pool config`](tycho_util::cli::config::ThreadPoolConfig)
    /// does not have a corresponding parameter and does not change tokio's default value.
    pub max_blocking_tasks: NonZeroU16,

    /// Max simultaneous point search tasks fulfilling download request
    pub max_upload_tasks: NonZeroU8,
}

impl Default for MempoolNodeConfig {
    fn default() -> Self {
        Self {
            log_truncate_long_values: true,
            clean_db_period_rounds: NonZeroU16::new(105).unwrap(),
            cache_future_broadcasts_rounds: 105,
            max_blocking_tasks: NonZeroU16::new(250).unwrap(),
            max_upload_tasks: NonZeroU8::new(50).unwrap(),
        }
    }
}
