use std::ops::RangeInclusive;
use std::time::Duration;

use crate::models::{Round, UnixTime};

pub struct MempoolConfig;

impl MempoolConfig {
    /// how far a signed point (by the time in its body)
    /// may be in the future compared with local (wall) time
    const CLOCK_SKEW: UnixTime = UnixTime::from_millis(5 * 1000);
    /// how long a point from past remains eligible for signature and inclusion;
    /// time in point body is compared with wall time;
    /// if consensus makes no progress for such long, it will need a manual restart from a new genesis
    const MAX_OUTDATED: UnixTime = UnixTime::from_millis(365 * 24 * 60 * 60 * 1000);

    /// see [CLOCK_SKEW](Self::CLOCK_SKEW) and [MAX_OUTDATED](Self::MAX_OUTDATED)
    pub fn sign_time_range() -> RangeInclusive<UnixTime> {
        let now = UnixTime::now();
        now - Self::MAX_OUTDATED..=now + Self::CLOCK_SKEW
    }

    /// we try to gather as many points and signatures as we can within some time frame;
    /// this is a tradeoff between breaking on exactly 2F+1 elements
    /// (dependencies and/or signatures), and waiting for slow nodes
    pub const RETRY_INTERVAL: Duration = Duration::from_millis(150);

    /// the least amount of [Round]s that are kept in DAG until they are discarded
    pub const COMMIT_DEPTH: u8 = 20;

    pub const GENESIS_ROUND: Round = Round(1);

    /// should not be less than 3 (as in average 1 of 3 is unreliable and another one did not sign);
    /// includes at least the author of dependant point and the author of dependency point;
    /// increases exponentially on every attempt, until every node out of 2F+1 is queried once
    /// or a verifiable point is found (ill-formed or incorrectly signed points are not eligible)
    pub const DOWNLOAD_PEERS: u8 = 3;

    /// hard limit on cached external messages ring buffer
    pub const PAYLOAD_BUFFER_BYTES: usize = 50 * 1024 * 1024;

    /// hard limit on point payload (excessive will be postponed)
    pub const PAYLOAD_BATCH_BYTES: usize = 768 * 1024;

    /// every failed response is accounted as point is not found;
    /// 1/3+1 failed responses leads to invalidation of the point and all its dependants
    pub const DOWNLOAD_SPAWN_INTERVAL: Duration = Duration::from_millis(50);
}
