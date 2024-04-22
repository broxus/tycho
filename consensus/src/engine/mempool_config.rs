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
    pub const RETRY_INTERVAL: Duration = Duration::from_millis(1000);

    pub const COMMIT_DEPTH: u32 = 20;

    pub const GENESIS_ROUND: Round = Round(1);
}
