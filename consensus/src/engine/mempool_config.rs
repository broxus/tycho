use std::num::NonZeroU8;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use everscale_crypto::ed25519::{KeyPair, SecretKey};
use everscale_types::models::ConsensusConfig;
use serde::{Deserialize, Serialize};
use tycho_network::OverlayId;

use crate::dag::WAVE_ROUNDS;
use crate::models::{Link, Point, PointData, PointId, Round, UnixTime};

static CONFIG: OnceLock<MempoolConfig> = OnceLock::new();

static GENESIS: OnceLock<PointId> = OnceLock::new();

pub struct Genesis();

impl Genesis {
    pub fn id() -> &'static PointId {
        GENESIS.get().expect("genesis not initialized")
    }

    pub fn round() -> Round {
        CachedConfig::get().genesis_round
    }

    pub fn time() -> UnixTime {
        CachedConfig::get().genesis_time
    }
}

pub struct CachedConfig;

impl CachedConfig {
    /// how far a signed point (by the time in its body)
    /// may be in the future compared with local (wall) time.
    /// Lower bound is defined by genesis, and then advanced by leaders among reliable peers.
    /// Time is non-decreasing due to its inheritance from anchor candidate in every point.
    pub fn clock_skew() -> UnixTime {
        Self::get().clock_skew
    }

    /// hard limit (in rounds) on anchor history length, must be divisible by 4 (i.e. commit wave length)
    pub fn commit_history_rounds() -> u32 {
        Self::get().commit_history_rounds
    }

    /// hard limit on point payload (excessive will be postponed)
    pub fn payload_batch_bytes() -> usize {
        Self::get().payload_batch_bytes
    }
    /// Estimated hard limit on serialized point size
    pub fn point_max_bytes() -> usize {
        Self::get().point_max_bytes
    }

    /// External messages are deduplicated within a fixed amount of rounds for each anchor.
    pub fn deduplicate_rounds() -> u32 {
        Self::get().deduplicate_rounds
    }

    /// The max expected distance (in rounds) between two sequential top known anchors (TKA),
    /// i.e. anchors from two sequential top known blocks (TKB, signed master chain blocks,
    /// available to local node, and which state is not necessarily applied by local node). For
    /// example, the last TKA=`1` and the config value is `210`, so the range `(2..=211).len() == 210`
    /// is expected to contain a new committed anchor, i.e. some leader successfully completed
    /// its 3 rounds in a row (collected 2F+1 signatures for the anchor trigger) and
    /// there were 2 additional consensus rounds for the trigger to be delivered to all nodes.
    /// So every collator is expected to create a block containing the new anchor and a greater
    /// chain time. Until an updated TKA in range `(2..=211)` is received by the local mempool,
    /// it will not produce a point at round `212` (wait in a "silent mode", probably consuming
    /// other's broadcasts in case it's a local-only lag and consensus is still advancing).
    /// For this example, corresponding config method returns `211`, the upper bound of `(2..=211)`.
    /// Effectively defines negative feedback from block validation consensus to mempool consensus.
    /// Also see [`TopKnownAnchor`](crate::engine::round_watch::TopKnownAnchor).
    pub fn max_consensus_lag_rounds() -> u32 {
        Self::get().max_consensus_lag_rounds
    }
    pub fn silent_after(top_known_anchor: Round) -> Round {
        // collation will continue across epoch change, so no limit on current genesis round
        Round((top_known_anchor.0).saturating_add(Self::get().max_consensus_lag_rounds))
    }

    // == Configs above must be globally same for consensus to run
    // ========
    // == Configs below affects performance and may differ across nodes,
    // == though misconfiguration may make the node unusable or banned

    /// amount of future [Round]s that [`BroadcastFilter`](crate::intercom::BroadcastFilter) caches
    /// to extend [`Dag`](crate::dag::DagFront) without downloading points for locally skipped rounds
    pub fn cache_future_broadcasts_rounds() -> u32 {
        Self::get().cache_future_broadcasts_rounds
    }

    /// see [`LogFlavor`]
    pub fn log_flavor() -> LogFlavor {
        Self::get().log_flavor
    }

    /// we try to gather as many points and signatures as we can within some time frame;
    /// this is a tradeoff between breaking on exactly 2F+1 elements
    /// (dependencies and/or signatures), and waiting for slow nodes
    pub fn broadcast_retry() -> Duration {
        Self::get().broadcast_retry
    }

    /// hard limit on cached external messages ring buffer, see [`Self::payload_batch_bytes`]
    pub fn payload_buffer_bytes() -> usize {
        Self::get().payload_buffer_bytes
    }

    /// amount of random peers to request at each attempt; does not include mandatory peers;
    /// value increases with each attempt, until 2F successfully responded `None`
    /// or a verifiable point is found (incorrectly signed points do not count)
    pub fn download_peers() -> usize {
        Self::get().download_peers
    }

    /// [`Downloader`](crate::intercom::Downloader) makes responses in groups after previous
    /// group completed or this interval elapsed (in order to not wait for some slow responding peer)
    ///
    /// 2F "point not found" responses lead to invalidation of all referencing points;
    /// failed network queries are retried after all peers were queried the same amount of times,
    /// and only successful responses that point is not found are taken into account.
    ///
    /// Notice that reliable peers respond immediately with points they already have
    /// validated successfully, or return `None`.
    pub fn download_retry() -> Duration {
        Self::get().download_retry
    }

    /// max count of ongoing downloads (except the first one) at any point of time
    pub fn download_tasks() -> u16 {
        Self::get().download_tasks
    }

    /// Max distance (in rounds) behind consensus at which local mempool
    /// in silent mode (see [`Self::max_consensus_lag_rounds`]) is supposed to keep history;
    /// provide means to collate optimistically when local node lags to get blocks signed;
    /// provides data for syncing neighbours
    pub fn sync_support_rounds() -> u32 {
        Self::get().sync_support_rounds
    }

    /// How often (in rounds) try to flush and delete obsolete data. Cannot be zero.
    /// Also affects WAL file size (more often flushes create more small files).
    pub fn clean_rocks_period() -> u32 {
        Self::get().clean_db_period_rounds
    }
}

impl CachedConfig {
    fn get() -> &'static MempoolConfig {
        CONFIG.get().expect("config not initialized")
    }

    pub fn init(config: &MempoolConfig) -> (Point, OverlayId) {
        // reset types to u128 as it does not match fields in `ConsensusConfig`
        // and may be changed just to keep them handy, that must not affect hash
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(config.genesis_round.0 as u128).to_be_bytes());
        hasher.update(&(config.genesis_time.as_u64() as u128).to_be_bytes());
        hasher.update(&(config.clock_skew.as_u64() as u128).to_be_bytes());
        hasher.update(&(config.payload_batch_bytes as u128).to_be_bytes());
        hasher.update(&(config.commit_history_rounds as u128).to_be_bytes());
        hasher.update(&(config.deduplicate_rounds as u128).to_be_bytes());
        hasher.update(&(config.max_consensus_lag_rounds as u128).to_be_bytes());

        let overlay_id = OverlayId(hasher.finalize().into());

        let genesis_keys = KeyPair::from(&SecretKey::from_bytes(overlay_id.0));

        CONFIG.set(config.clone()).ok(); // may try to set the same value

        let genesis = Point::new(
            &genesis_keys,
            config.genesis_round,
            Default::default(),
            Default::default(),
            PointData {
                author: genesis_keys.public_key.into(),
                time: config.genesis_time,
                includes: Default::default(),
                witness: Default::default(),
                anchor_trigger: Link::ToSelf,
                anchor_proof: Link::ToSelf,
                anchor_time: config.genesis_time,
            },
        );

        GENESIS.set(genesis.id()).ok(); // may try to set the same value

        assert_eq!(
            *Genesis::id(),
            genesis.id(),
            "genesis is not properly initialized"
        );

        assert_eq!(
            Genesis::id().round,
            CachedConfig::get().genesis_round,
            "cached config mismathces cached genesis"
        );

        (genesis, overlay_id)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
/// Prod config should use [`Full`], while [`Truncated`] is more friendly to human eyes
pub enum LogFlavor {
    Full,
    Truncated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MempoolConfig {
    // next settings affect overlay id and are set by collator
    genesis_round: Round,
    genesis_time: UnixTime,
    // next settings affect affects overlay id
    clock_skew: UnixTime,
    payload_batch_bytes: usize,
    commit_history_rounds: u32,
    deduplicate_rounds: u32,
    max_consensus_lag_rounds: u32,
    // next settings do not affect overlay id
    payload_buffer_bytes: usize,
    broadcast_retry: Duration,
    download_retry: Duration,
    download_peers: usize,
    download_tasks: u16,
    sync_support_rounds: u32,
    // per node settings
    log_flavor: LogFlavor,
    clean_db_period_rounds: u32,
    cache_future_broadcasts_rounds: u32,
    // derived values
    point_max_bytes: usize,
}
impl MempoolConfig {
    /// returns actual round number after alignment of mempool start round
    pub fn genesis_round(&self) -> u32 {
        self.genesis_round.0
    }
}

#[derive(Default, Debug)]
pub struct MempoolConfigBuilder {
    genesis_data: Option<GenesisData>,
    consensus_config: Option<ConsensusConfig>,
    node_config: Option<MempoolNodeConfig>,
}

impl MempoolConfigBuilder {
    pub fn set_node_config(&mut self, node_config: &MempoolNodeConfig) -> &mut Self {
        self.node_config = Some(node_config.clone());
        self
    }

    pub fn set_consensus_config(&mut self, consensus_config: &ConsensusConfig) -> &mut Self {
        self.consensus_config = Some(consensus_config.clone());
        self
    }

    pub fn set_genesis(&mut self, start_round: u32, time_millis: u64) -> bool {
        // Must be (divisible by 4)+1, ie 1,5,9 etc., see `crate::dag::AnchorStage::of()`
        let aligned_start = ((start_round + 2) / WAVE_ROUNDS) * WAVE_ROUNDS + 1;
        assert!(
            aligned_start > Round::BOTTOM.0,
            "aligned genesis round is too low and will make code panic"
        );

        let mut updated = false;

        if let Some(genesis) = &mut self.genesis_data {
            if genesis.time_millis < time_millis {
                genesis.round = aligned_start;
                genesis.time_millis = time_millis;

                updated = true;
            }
        } else {
            self.genesis_data = Some(GenesisData {
                round: aligned_start,
                time_millis,
            });

            updated = true;
        }

        updated
    }

    pub fn get_consensus_config(&self) -> Option<&ConsensusConfig> {
        self.consensus_config.as_ref()
    }

    pub fn get_genesis_round(&self) -> Option<u32> {
        self.genesis_data.as_ref().map(|genesis| genesis.round)
    }

    pub fn build(&self) -> Result<MempoolConfig> {
        let genesis_data = self
            .genesis_data
            .as_ref()
            .ok_or(anyhow!("mempool genesis data for config is not known"))?;
        let consensus_config = self
            .consensus_config
            .as_ref()
            .ok_or(anyhow!("mempool consensus config is not known"))?;
        let node_config = self
            .node_config
            .as_ref()
            .ok_or(anyhow!("mempool node config is not known"))?;

        let inner = MempoolConfig {
            genesis_round: Round(genesis_data.round),
            genesis_time: UnixTime::from_millis(genesis_data.time_millis),
            clock_skew: UnixTime::from_millis(consensus_config.clock_skew_millis as u64),
            payload_batch_bytes: consensus_config.payload_batch_bytes as usize,
            commit_history_rounds: consensus_config.commit_history_rounds as u32,
            deduplicate_rounds: consensus_config.deduplicate_rounds as u32,
            max_consensus_lag_rounds: consensus_config.max_consensus_lag_rounds as u32,
            payload_buffer_bytes: consensus_config.payload_buffer_bytes as usize,
            broadcast_retry: Duration::from_millis(consensus_config.broadcast_retry_millis as u64),
            download_retry: Duration::from_millis(consensus_config.download_retry_millis as u64),
            download_peers: consensus_config.download_peers as usize,
            download_tasks: consensus_config.download_tasks,
            sync_support_rounds: consensus_config.sync_support_rounds as u32,
            log_flavor: if node_config.log_truncate_long_values {
                LogFlavor::Truncated
            } else {
                LogFlavor::Full
            },
            clean_db_period_rounds: node_config.clean_db_period_rounds.get() as u32,
            cache_future_broadcasts_rounds: node_config.cache_future_broadcasts_rounds.get() as u32,
            point_max_bytes: Point::max_byte_size(consensus_config.payload_batch_bytes as usize),
        };

        ensure!(
            inner.max_consensus_lag_rounds >= inner.commit_history_rounds,
            "max consensus lag must be greater than commit depth"
        );

        ensure!(
            inner.payload_buffer_bytes >= inner.payload_batch_bytes,
            "no need to evict cached externals if can send them in one message"
        );

        ensure!(
            inner.clean_db_period_rounds > 0,
            "rocks db clean period cannot be zero rounds"
        );

        Ok(inner)
    }
}

// Note: never derive Default for Genesis data
#[derive(Clone, Debug, Eq, PartialEq)]
struct GenesisData {
    round: u32,
    time_millis: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MempoolNodeConfig {
    pub log_truncate_long_values: bool,
    pub clean_db_period_rounds: NonZeroU8,
    pub cache_future_broadcasts_rounds: NonZeroU8,
}

impl Default for MempoolNodeConfig {
    fn default() -> Self {
        Self {
            log_truncate_long_values: true,
            clean_db_period_rounds: NonZeroU8::new(105).unwrap(),
            cache_future_broadcasts_rounds: NonZeroU8::new(105).unwrap(),
        }
    }
}
