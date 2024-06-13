use std::ops::RangeInclusive;
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
    /// may be in the future compared with local (wall) time
    const CLOCK_SKEW: UnixTime = UnixTime::from_millis(5 * 1000);
    /// how long a point from past remains eligible for signature and inclusion;
    /// time in point body is compared with wall time;
    /// if consensus makes no progress for such long, it will need a manual restart from a new genesis
    const MAX_OUTDATED: UnixTime = UnixTime::from_millis(365 * 24 * 60 * 60 * 1000);

    /// see [`CLOCK_SKEW`](Self::CLOCK_SKEW) and [`MAX_OUTDATED`](Self::MAX_OUTDATED)
    pub fn sign_time_range() -> RangeInclusive<UnixTime> {
        let now = UnixTime::now();
        now - Self::MAX_OUTDATED..=now + Self::CLOCK_SKEW
    }

    /// the least amount of [Round]s that are kept in DAG until they are discarded
    pub const COMMIT_DEPTH: u8 = 20;

    pub const GENESIS_ROUND: Round = Round(1);

    /// hard limit on point payload (excessive will be postponed)
    pub const PAYLOAD_BATCH_BYTES: usize = 768 * 1024;

    // == Configs above must be globally same for consensus to run
    // ========
    // == Configs below affects performance and may differ across nodes,
    // == though misconfiguration may make the node unusable or banned

    /// see [`LogFlavor`]
    pub const LOG_FLAVOR: LogFlavor = LogFlavor::Truncated;

    /// we try to gather as many points and signatures as we can within some time frame;
    /// this is a tradeoff between breaking on exactly 2F+1 elements
    /// (dependencies and/or signatures), and waiting for slow nodes
    pub const RETRY_INTERVAL: Duration = Duration::from_millis(150);

    /// hard limit on cached external messages ring buffer, see [`Self::PAYLOAD_BATCH_BYTES`]
    pub const PAYLOAD_BUFFER_BYTES: usize = 50 * 1024 * 1024;

    /// should not be less than 3 (as in average 1 of 3 is unreliable and another one did not sign);
    /// value is multiplied by the current attempt number, until 2F+1 successfully responded
    /// or a verifiable point is found (ill-formed or incorrectly signed points are not eligible)
    pub const DOWNLOAD_PEERS: u8 = 5;

    /// [`Downloader`](crate::intercom::Downloader) makes responses in groups after previous
    /// group completed or this interval elapsed (in order to not wait for some slow responding peer)
    ///
    /// 2F+1 "point not found" responses lead to invalidation of all referencing points;
    /// failed network queries are retried after all peers were queried the same amount of times,
    /// and only successful responses that point is not found are taken into account.
    ///
    /// Notice that reliable peers respond immediately with points they already have
    /// validated successfully, or return `None`.
    pub const DOWNLOAD_INTERVAL: Duration = Duration::from_millis(30);
}
