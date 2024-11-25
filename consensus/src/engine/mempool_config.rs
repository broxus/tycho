use std::num::NonZeroU16;
use std::sync::OnceLock;

use anyhow::{anyhow, ensure, Result};
use everscale_crypto::ed25519::{KeyPair, SecretKey};
use everscale_types::models::ConsensusConfig;
use serde::{Deserialize, Serialize};
use tycho_network::OverlayId;

use crate::dag::align_genesis;
use crate::models::{Link, Point, PointData, PointId, UnixTime};

static CONFIG: OnceLock<MempoolConfig> = OnceLock::new();

static GENESIS: OnceLock<PointId> = OnceLock::new();

pub struct Genesis();

impl Genesis {
    pub fn id() -> &'static PointId {
        GENESIS.get().expect("genesis not initialized")
    }
}

pub struct CachedConfig;

impl CachedConfig {
    pub fn get() -> &'static MempoolConfig {
        CONFIG.get().expect("config not initialized")
    }

    pub fn init(config: &MempoolConfig) -> (Point, OverlayId) {
        let genesis_round = align_genesis(config.genesis.start_round);

        // reset types to u128 as it does not match fields in `ConsensusConfig`
        // and may be changed just to keep them handy, that must not affect hash
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(genesis_round.0 as u128).to_be_bytes());
        hasher.update(&(config.genesis.time_millis as u128).to_be_bytes());
        hasher.update(&(config.consensus.clock_skew_millis as u128).to_be_bytes());
        hasher.update(&(config.consensus.payload_batch_bytes as u128).to_be_bytes());
        hasher.update(&(config.consensus.commit_history_rounds as u128).to_be_bytes());
        hasher.update(&(config.consensus.deduplicate_rounds as u128).to_be_bytes());
        hasher.update(&(config.consensus.max_consensus_lag_rounds as u128).to_be_bytes());

        let overlay_id = OverlayId(hasher.finalize().into());

        let genesis_keys = KeyPair::from(&SecretKey::from_bytes(overlay_id.0));

        CONFIG.set(config.clone()).ok(); // may try to set the same value

        let genesis = Point::new(
            &genesis_keys,
            genesis_round,
            Default::default(),
            Default::default(),
            PointData {
                author: genesis_keys.public_key.into(),
                time: UnixTime::from_millis(config.genesis.time_millis),
                includes: Default::default(),
                witness: Default::default(),
                anchor_trigger: Link::ToSelf,
                anchor_proof: Link::ToSelf,
                anchor_time: UnixTime::from_millis(config.genesis.time_millis),
            },
        );

        GENESIS.set(genesis.id()).ok(); // may try to set the same value

        assert_eq!(
            *Genesis::id(),
            genesis.id(),
            "genesis is not properly initialized"
        );

        (genesis, overlay_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MempoolConfig {
    pub genesis: GenesisData,
    pub consensus: ConsensusConfig,
    pub node: MempoolNodeConfig,
    /// Estimated hard limit on serialized point size
    pub point_max_bytes: usize,
}

#[derive(Default, Debug)]
pub struct MempoolConfigBuilder {
    genesis_data: Option<GenesisData>,
    consensus_config: Option<ConsensusConfig>,
    node_config: Option<MempoolNodeConfig>,
}

impl MempoolConfigBuilder {
    pub fn set_node_config(&mut self, node_config: &MempoolNodeConfig) {
        self.node_config = Some(node_config.clone());
    }

    pub fn set_consensus_config(&mut self, consensus_config: &ConsensusConfig) {
        self.consensus_config = Some(consensus_config.clone());
    }

    pub fn set_genesis(&mut self, start_round: u32, time_millis: u64) {
        self.genesis_data = Some(GenesisData {
            start_round,
            time_millis,
        });
    }

    pub fn get_consensus_config(&self) -> Option<&ConsensusConfig> {
        self.consensus_config.as_ref()
    }

    pub fn get_genesis(&self) -> Option<GenesisData> {
        self.genesis_data
    }

    pub fn build(&self) -> Result<MempoolConfig> {
        let genesis_data = *self
            .genesis_data
            .as_ref()
            .ok_or(anyhow!("mempool genesis data for config is not known"))?;
        let consensus_config = self
            .consensus_config
            .as_ref()
            .ok_or(anyhow!("mempool consensus config is not known"))?
            .clone();
        let node_config = self
            .node_config
            .as_ref()
            .ok_or(anyhow!("mempool node config is not known"))?
            .clone();

        let point_max_bytes = Point::max_byte_size(consensus_config.payload_batch_bytes as usize);

        ensure!(
            consensus_config.max_consensus_lag_rounds >= consensus_config.commit_history_rounds,
            "max consensus lag must be greater than commit depth"
        );

        ensure!(
            consensus_config.payload_buffer_bytes >= consensus_config.payload_batch_bytes,
            "no need to evict cached externals if can send them in one message"
        );

        Ok(MempoolConfig {
            genesis: genesis_data,
            consensus: consensus_config,
            node: node_config,
            point_max_bytes,
        })
    }
}

// Note: never derive Default for Genesis data
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct GenesisData {
    /// will be aligned to become genesis round (will not be lesser, may become a bit greater)
    pub start_round: u32,
    /// value will be copied to genesis point without changes
    pub time_millis: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MempoolNodeConfig {
    /// `true` to truncate hashes, signatures and use non-standard format for large structs
    /// that may be more readable
    pub log_truncate_long_values: bool,

    /// How often (in rounds) delete obsolete data and trigger rocksDB compaction.
    pub clean_db_period_rounds: NonZeroU16,

    /// amount of future [Round]s that [`BroadcastFilter`](crate::intercom::BroadcastFilter) caches
    /// to extend [`Dag`](crate::dag::DagFront) without downloading points for locally skipped rounds
    pub cache_future_broadcasts_rounds: NonZeroU16,
}

impl Default for MempoolNodeConfig {
    fn default() -> Self {
        Self {
            log_truncate_long_values: true,
            clean_db_period_rounds: NonZeroU16::new(105).unwrap(),
            cache_future_broadcasts_rounds: NonZeroU16::new(105).unwrap(),
        }
    }
}
