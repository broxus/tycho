use std::sync::OnceLock;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tycho_network::OverlayId;

use crate::dag::WAVE_ROUNDS;
use crate::models::{Round, UnixTime};

#[derive(Copy, Clone, PartialEq, Debug)]
/// Prod config should use [`Full`], while [`Truncated`] is more friendly to human eyes
pub enum LogFlavor {
    Full,
    Truncated,
}

/// Runtime constants that need to be initialized
struct RuntimeConstants {
    /// how far a signed point (by the time in its body)
    /// may be in the future compared with local (wall) time.
    /// Lower bound is defined by genesis, and then advanced by leaders among reliable peers.
    /// Time is non-decreasing due to its inheritance from anchor candidate in every point.
    clock_skew: OnceLock<UnixTime>,

    /// hard limit (in rounds) on anchor history length, must be divisible by 4 (i.e. commit wave length)
    commit_depth: OnceLock<u8>,

    /// round to start from
    genesis_round: OnceLock<Round>,

    /// hard limit on point payload (excessive will be postponed)
    payload_batch_bytes: OnceLock<usize>,

    /// External messages are deduplicated within a fixed depth for each anchor.
    /// Zero turns off deduplication.
    deduplicate_rounds: OnceLock<u16>,

    /// The max expected distance (in rounds) between two anchor triggers. Defines both:
    /// * max acceptable distance between consensus and the top known block,
    ///   before local mempool enters silent mode (stops to produce points).
    /// * max amount of rounds (behind peer's last commit, defined by an anchor trigger in points)
    ///   a peer must respond with valid points if it directly referenced them
    max_anchor_distance: OnceLock<u16>,

    /// Max distance (in rounds) behind consensus at which local mempool
    /// in silent mode (see [`Self::MAX_ANCHOR_DISTANCE`])
    /// is supposed to keep collation-ready history
    /// (see [`Self::DEDUPLICATE_ROUNDS`] and [`Self::COMMIT_DEPTH`])
    acceptable_collator_lag: OnceLock<u16>,
}

impl RuntimeConstants {
    const fn new() -> Self {
        Self {
            clock_skew: OnceLock::new(),
            commit_depth: OnceLock::new(),
            genesis_round: OnceLock::new(),
            deduplicate_rounds: OnceLock::new(),
            max_anchor_distance: OnceLock::new(),
            payload_batch_bytes: OnceLock::new(),
            acceptable_collator_lag: OnceLock::new(),
        }
    }
}

static RUNTIME_CONSTANTS: RuntimeConstants = RuntimeConstants::new();

pub struct MempoolConfig;

impl MempoolConfig {
    pub fn init(global_config: &MempoolGlobalConfig) {
        RUNTIME_CONSTANTS
            .deduplicate_rounds
            .get_or_init(|| global_config.deduplicate_rounds);
        RUNTIME_CONSTANTS
            .clock_skew
            .get_or_init(|| UnixTime::from_millis(global_config.clock_skew));
        RUNTIME_CONSTANTS
            .commit_depth
            .get_or_init(|| global_config.commit_depth);

        RUNTIME_CONSTANTS
            .acceptable_collator_lag
            .get_or_init(|| 1_050 - global_config.max_anchor_distance);

        // Must be (divisible by 4)+1, ie 1,5,9 etc., see `crate::dag::AnchorStage::of()`
        let aligned = ((global_config.genesis_round + 2) / WAVE_ROUNDS) * WAVE_ROUNDS + 1;
        assert!(
            aligned > Round::BOTTOM.0,
            "invalid config: genesis round is too low and will make code panic"
        );
        RUNTIME_CONSTANTS
            .genesis_round
            .get_or_init(|| Round(aligned));

        RUNTIME_CONSTANTS
            .max_anchor_distance
            .get_or_init(|| global_config.max_anchor_distance);
        assert!(
            MempoolConfig::max_anchor_distance() >= MempoolConfig::commit_depth() as u16,
            "invalid config: max acceptable anchor distance cannot be less than commit depth"
        );

        RUNTIME_CONSTANTS
            .payload_batch_bytes
            .get_or_init(|| global_config.payload_batch_size as usize);
        assert!(
            MempoolConfig::PAYLOAD_BUFFER_BYTES >= MempoolConfig::payload_batch_size(),
            "invalid config: no need to evict cached externals if can send them in one message"
        );
    }

    pub fn clock_skew() -> UnixTime {
        *RUNTIME_CONSTANTS
            .clock_skew
            .get()
            .expect("CLOCK_SKEW not initialized")
    }

    pub fn commit_depth() -> u8 {
        *RUNTIME_CONSTANTS
            .commit_depth
            .get()
            .expect("COMMIT_DEPTH not initialized")
    }

    pub fn genesis_round() -> Round {
        *RUNTIME_CONSTANTS
            .genesis_round
            .get()
            .expect("GENESIS_ROUND not initialized")
    }

    pub fn payload_batch_size() -> usize {
        *RUNTIME_CONSTANTS
            .payload_batch_bytes
            .get()
            .expect("PAYLOAD_BATCH_BYTES not initialized")
    }

    pub fn deduplicate_rounds() -> u16 {
        *RUNTIME_CONSTANTS
            .deduplicate_rounds
            .get()
            .expect("DEDUPLICATE_ROUNDS not initialized")
    }

    pub fn max_anchor_distance() -> u16 {
        *RUNTIME_CONSTANTS
            .max_anchor_distance
            .get()
            .expect("MAX_ANCHOR_DISTANCE not initialized")
    }

    pub fn acceptable_collator_lag() -> u16 {
        *RUNTIME_CONSTANTS
            .acceptable_collator_lag
            .get()
            .expect("ACCEPTABLE_COLLATOR_LAG not initialized")
    }

    // == Configs above must be globally same for consensus to run
    // ========
    // == Configs below affects performance and may differ across nodes,
    // == though misconfiguration may make the node unusable or banned

    /// amount of future [Round]s that [`BroadcastFilter`](crate::intercom::BroadcastFilter) caches
    /// to extend [`Dag`](crate::dag::DagFront) without downloading points for locally skipped rounds
    pub const CACHE_AHEAD_ENGINE_ROUNDS: u8 = 105;

    /// see [`LogFlavor`]
    pub const LOG_FLAVOR: LogFlavor = LogFlavor::Truncated;

    /// we try to gather as many points and signatures as we can within some time frame;
    /// this is a tradeoff between breaking on exactly 2F+1 elements
    /// (dependencies and/or signatures), and waiting for slow nodes
    pub const RETRY_INTERVAL: Duration = Duration::from_millis(150);

    /// hard limit on cached external messages ring buffer, see [`Self::PAYLOAD_BATCH_BYTES`]
    pub const PAYLOAD_BUFFER_BYTES: usize = 50 * 1024 * 1024;

    /// amount of random peers to request at each attempt; does not include mandatory peers;
    /// value increases with each attempt, until 2F successfully responded `None`
    /// or a verifiable point is found (incorrectly signed points do not count)
    pub const DOWNLOAD_PEERS: u8 = 2;

    /// [`Downloader`](crate::intercom::Downloader) makes responses in groups after previous
    /// group completed or this interval elapsed (in order to not wait for some slow responding peer)
    ///
    /// 2F "point not found" responses lead to invalidation of all referencing points;
    /// failed network queries are retried after all peers were queried the same amount of times,
    /// and only successful responses that point is not found are taken into account.
    ///
    /// Notice that reliable peers respond immediately with points they already have
    /// validated successfully, or return `None`.
    pub const DOWNLOAD_INTERVAL: Duration = Duration::from_millis(25);

    /// max count of ongoing downloads (except the first one) at any point of time
    pub const CONCURRENT_DOWNLOADS: u16 = if cfg!(feature = "test") { 1 } else { 260 };

    /// How often (in rounds) try to flush and delete obsolete data. Cannot be zero.
    /// Also affects WAL file size (more often flushes create more small files).
    pub const CLEAN_ROCKS_PERIOD: u16 = if cfg!(feature = "test") { 10 } else { 105 };
}

const _: () = assert!(
    MempoolConfig::CLEAN_ROCKS_PERIOD > 0,
    "invalid config: rocks db clean period cannot be zero rounds"
);

pub struct MempoolGlobalConfig {
    pub clock_skew: u64,
    pub commit_depth: u8,
    pub genesis_round: u32,
    pub payload_batch_size: u64,
    pub deduplicate_rounds: u16,
    pub max_anchor_distance: u16,
}

impl MempoolGlobalConfig {
    const SIZE: usize = 8 + 1 + 4 + 8 + 2 + 2;

    pub fn overlay_id(&self) -> OverlayId {
        let mut result = Vec::with_capacity(Self::SIZE);

        result.extend_from_slice(&self.clock_skew.to_be_bytes());
        result.push(self.commit_depth);
        result.extend_from_slice(&self.genesis_round.to_be_bytes());
        result.extend_from_slice(&self.payload_batch_size.to_be_bytes());
        result.extend_from_slice(&self.deduplicate_rounds.to_be_bytes());
        result.extend_from_slice(&self.max_anchor_distance.to_be_bytes());

        let hash = Sha256::digest(&result);
        OverlayId(hash.into())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn mempool_config_init() {
        let global_config = MempoolGlobalConfig {
            clock_skew: 5000,
            commit_depth: 20,
            genesis_round: 0,
            payload_batch_size: 786432,
            deduplicate_rounds: 140,
            max_anchor_distance: 210,
        };

        MempoolConfig::init(&global_config);

        assert_eq!(
            MempoolConfig::clock_skew().as_u64(),
            global_config.clock_skew
        );
        assert_eq!(
            MempoolConfig::payload_batch_size() as u64,
            global_config.payload_batch_size
        );
        assert_eq!(
            MempoolConfig::max_anchor_distance(),
            global_config.max_anchor_distance
        );
        assert_eq!(MempoolConfig::commit_depth(), global_config.commit_depth);

        let round = ((global_config.genesis_round + 2) / WAVE_ROUNDS) * WAVE_ROUNDS + 1;
        assert_eq!(MempoolConfig::genesis_round().0, round);
    }

    #[test]
    fn mempool_global_config_size() {
        assert_eq!(
            MempoolGlobalConfig::SIZE,
            size_of::<u64>()       // clock_skew
                + size_of::<u8>()  // commit_depth
                + size_of::<u32>() // genesis_round
                + size_of::<u64>() // payload_batch_size
                + size_of::<u16>() // deduplicate_rounds
                + size_of::<u16>() // max_anchor_distance
        );
    }

    #[test]
    fn mempool_global_config_id() {
        let global_config = MempoolGlobalConfig {
            clock_skew: 5000,
            commit_depth: 20,
            genesis_round: 0,
            payload_batch_size: 786432,
            deduplicate_rounds: 140,
            max_anchor_distance: 210,
        };

        let overlay_id = global_config.overlay_id();
        assert_eq!(
            hex::encode(overlay_id.as_bytes()),
            "5c393c876b4c91f1d8679bbcccc198ae19b61a6f55bad0dd49095f0d734bea70"
        );
    }
}
