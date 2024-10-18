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

    /// hard limit (in rounds) on anchor history length, must be divisible by 4 (i.e. commit wave length)
    pub const COMMIT_DEPTH: u8 = 20;

    /// hard limit on point payload (excessive will be postponed)
    pub const PAYLOAD_BATCH_BYTES: usize = 768 * 1024;

    /// External messages are deduplicated within a fixed depth for each anchor.
    /// Zero turns off deduplication.
    pub const DEDUPLICATE_ROUNDS: u16 = if cfg!(feature = "test") { 20 } else { 140 };

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
    pub const MAX_CONSENSUS_LAG_ROUNDS: u16 = if cfg!(feature = "test") { 20 } else { 210 };

    pub fn silent_after(top_known_anchor: Round) -> Round {
        // collation will continue across epoch change, so no limit on current genesis round
        Round((top_known_anchor.0).saturating_add(Self::MAX_CONSENSUS_LAG_ROUNDS as u32))
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

    /// Max distance (in rounds) behind consensus at which local mempool
    /// in silent mode (see [`Self::MAX_CONSENSUS_LAG_ROUNDS`]) is supposed to keep history;
    /// provide means to collate optimistically when local node lags to get blocks signed;
    /// provides data for syncing neighbours
    pub const SYNC_SUPPORT_ROUNDS: u16 = if cfg!(feature = "test") { 15 } else { 840 };

    /// How often (in rounds) try to flush and delete obsolete data. Cannot be zero.
    /// Also affects WAL file size (more often flushes create more small files).
    pub const CLEAN_ROCKS_PERIOD: u16 = if cfg!(feature = "test") { 10 } else { 105 };
}

const _: () = assert!(
    MempoolConfig::MAX_CONSENSUS_LAG_ROUNDS >= MempoolConfig::COMMIT_DEPTH as u16,
    "invalid config: expected max consensus lag >= commit depth"
);

const _: () = assert!(
    MempoolConfig::PAYLOAD_BUFFER_BYTES >= MempoolConfig::PAYLOAD_BATCH_BYTES,
    "invalid config: no need to evict cached externals if can send them in one message"
);

const _: () = assert!(
    MempoolConfig::CLEAN_ROCKS_PERIOD > 0,
    "invalid config: rocks db clean period cannot be zero rounds"
);
