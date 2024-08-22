use std::time::Duration;

use crate::models::{Round, UnixTime};

#[derive(Copy, Clone, PartialEq, Debug)]
/// Prod config should use [`Full`], while [`Truncated`] is more friendly to human eyes
pub enum LogFlavor {
    Full,
    Truncated,
}

pub struct MempoolConfig;

impl MempoolConfig {
    /// how far a signed point (by the time in its body)
    /// may be in the future compared with local (wall) time.
    /// Lower bound is defined by genesis, and then advanced by leaders among reliable peers.
    /// Time is non-decreasing due to its inheritance from anchor candidate in every point.
    pub const CLOCK_SKEW: UnixTime = UnixTime::from_millis(5 * 1000);

    /// hard limit (in rounds) on anchor history length
    /// includes anchor candidate round, though it is committed at the next attempt
    pub const COMMIT_DEPTH: u8 = 21;

    pub const GENESIS_ROUND: Round = Round(1);

    /// hard limit on point payload (excessive will be postponed)
    pub const PAYLOAD_BATCH_BYTES: usize = 768 * 1024;

    /// External messages are deduplicated within a fixed depth for each anchor.
    /// Zero turns off deduplication.
    pub const DEDUPLICATE_ROUNDS: u16 = 140;

    /// The max expected distance (in rounds) between two anchor triggers. Defines both:
    /// * max acceptable distance between consensus and the top known block,
    ///   before local mempool enters silent mode (stops to produce points).
    /// * max amount of rounds (behind peer's last commit, defined by an anchor trigger in points)
    ///   a peer must respond with valid points if it directly referenced them
    pub const MAX_ANCHOR_DISTANCE: u16 = if cfg!(feature = "test") { 21 } else { 210 };

    // == Configs above must be globally same for consensus to run
    // ========
    // == Configs below affects performance and may differ across nodes,
    // == though misconfiguration may make the node unusable or banned

    /// amount of future [Round]s that [`BroadcastFilter`](crate::intercom::BroadcastFilter) caches
    /// to extend [`Dag`](crate::dag::Dag) without downloading points for locally skipped rounds
    pub const CACHE_AHEAD_ENGINE_ROUNDS: u8 = 21;

    /// see [`LogFlavor`]
    pub const LOG_FLAVOR: LogFlavor = LogFlavor::Truncated;

    /// we try to gather as many points and signatures as we can within some time frame;
    /// this is a tradeoff between breaking on exactly 2F+1 elements
    /// (dependencies and/or signatures), and waiting for slow nodes
    pub const RETRY_INTERVAL: Duration = Duration::from_millis(150);

    /// hard limit on cached external messages ring buffer, see [`Self::PAYLOAD_BATCH_BYTES`]
    pub const PAYLOAD_BUFFER_BYTES: usize = 50 * 1024 * 1024;

    /// amount of random peers to request at each attempt; does not include mandatory peers;
    /// value increases exponentially with each attempt, until 2F successfully responded `None`
    /// or a verifiable point is found (ill-formed or incorrectly signed points do not count)
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
    pub const CONCURRENT_DOWNLOADS: u16 = if cfg!(feature = "test") { 0 } else { 260 };

    /// Max distance (in rounds) behind consensus at which local mempool
    /// in silent mode (see [`Self::MAX_ANCHOR_DISTANCE`])
    /// is supposed to keep collation-ready history
    /// (see [`Self::DEDUPLICATE_ROUNDS`] and [`Self::COMMIT_DEPTH`])
    pub const ACCEPTABLE_COLLATOR_LAG: u16 = 1_050 - Self::MAX_ANCHOR_DISTANCE;

    /// How often (in rounds) try to flush and delete obsolete data. Cannot be zero.
    /// Also affects WAL file size (more often flushes create more small files).
    pub const CLEAN_ROCKS_PERIOD: u16 = if cfg!(feature = "test") { 10 } else { 105 };
}

const _: () = assert!(
    MempoolConfig::GENESIS_ROUND.0 > Round::BOTTOM.0,
    "invalid config: genesis round is too low and will make code panic"
);

const _: () = assert!(
    MempoolConfig::MAX_ANCHOR_DISTANCE >= MempoolConfig::COMMIT_DEPTH as u16,
    "invalid config: max acceptable anchor distance cannot be less than commit depth"
);

const _: () = assert!(
    MempoolConfig::PAYLOAD_BUFFER_BYTES >= MempoolConfig::PAYLOAD_BATCH_BYTES,
    "invalid config: no need to evict cached externals if can send them in one message"
);

const _: () = assert!(
    MempoolConfig::CLEAN_ROCKS_PERIOD > 0,
    "invalid config: rocks db clean period cannot be zero rounds"
);
