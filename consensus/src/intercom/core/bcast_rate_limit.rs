use std::time::{Duration, Instant};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;
use crate::intercom::core::query::permit::{QueryPermit, QueryPermits};

/// During [`broadcast_retry_millis`](tycho_types::models::ConsensusConfig)
/// there may occur at most 1 broadcast and 2 signature queries for each point.
/// Consensus rounds advances at least [`min_sign_attempts`](tycho_types::models::ConsensusConfig)
/// times the [`broadcast_retry_millis`].
/// We use the double of the broadcast duration as a time window because
/// two consecutive broadcasts may overlap due to [`crate::intercom::Broadcaster::run_continue`].
/// [`QueryPermit`] is used to soft limit concurrent queries causing network error for cooldown.
pub struct BcastReceiverLimit {
    map: FastDashMap<PeerId, ReceiverLimits>,
    bcast_config: LimitConfig,
    sig_config: LimitConfig,
}

/// Doubles the rate limit until hard rejection
struct ReceiverLimits {
    broadcasts: SoftBucket,
    sig_queries: SoftBucket,
    permits: QueryPermits,
}

impl BcastReceiverLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            bcast_config: LimitConfig::new_bcast(conf),
            sig_config: LimitConfig::new_sig(conf),
        }
    }

    /// `Ok(None)` is a soft rejection that does not require ban
    pub fn broadcast(&self, peer_id: &PeerId) -> Result<Option<QueryPermit>, ()> {
        let now = Instant::now();
        let mut entry = (self.map.entry(*peer_id)).or_insert_with(|| self.new_entry(now));
        if entry.broadcasts.allows(now, &self.bcast_config)? {
            Ok(entry.permits.try_acquire())
        } else {
            Ok(None)
        }
    }

    /// `Ok(None)` is a soft rejection that does not require ban
    pub fn sig_query(&self, peer_id: &PeerId) -> Result<Option<QueryPermit>, ()> {
        let now = Instant::now();
        let mut entry = (self.map.entry(*peer_id)).or_insert_with(|| self.new_entry(now));
        if entry.sig_queries.allows(now, &self.sig_config)? {
            Ok(entry.permits.try_acquire())
        } else {
            Ok(None)
        }
    }

    fn new_entry(&self, now: Instant) -> ReceiverLimits {
        ReceiverLimits {
            broadcasts: SoftBucket::new(now, &self.bcast_config),
            sig_queries: SoftBucket::new(now, &self.sig_config),
            // for each round simultaneously can be queried either sig or bcast, 2 rounds at a time;
            // queries may have extra overlaps in case initiator drops its handle and moves forward
            // but the responder keeps processing the zombie request, thus we need a soft limit;
            // broadcaster must retry the failed query only after delay, so this reduces rate;
            // cannot track request round reliably because point must be checked for valid signature
            // after rate limit is applied, and the round extracted earlier cannot be trusted
            permits: QueryPermits::new(2.try_into().unwrap()),
        }
    }
}

pub struct BcastSenderLimit {
    map: FastDashMap<PeerId, SenderLimits>,
    bcast_config: LimitConfig,
    sig_config: LimitConfig,
}

struct SenderLimits {
    broadcasts: Bucket,
    sig_queries: Bucket,
    permits: QueryPermits,
}

impl BcastSenderLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            bcast_config: LimitConfig::new_bcast(conf),
            sig_config: LimitConfig::new_sig(conf),
        }
    }

    pub fn broadcast(&self, peer_id: &PeerId) -> Result<QueryPermit, ()> {
        let now = Instant::now();
        let mut entry = (self.map.entry(*peer_id)).or_insert_with(|| self.new_entry(now));
        if entry.broadcasts.passes(now, &self.bcast_config) {
            entry.permits.try_acquire().ok_or(())
        } else {
            Err(())
        }
    }

    pub fn sig_query(&self, peer_id: &PeerId) -> Result<QueryPermit, ()> {
        let now = Instant::now();
        let mut entry = (self.map.entry(*peer_id)).or_insert_with(|| self.new_entry(now));
        if entry.sig_queries.passes(now, &self.sig_config) {
            entry.permits.try_acquire().ok_or(())
        } else {
            Err(())
        }
    }

    fn new_entry(&self, now: Instant) -> SenderLimits {
        SenderLimits {
            broadcasts: Bucket::new(now, &self.bcast_config),
            sig_queries: Bucket::new(now, &self.sig_config),
            permits: QueryPermits::new(2.try_into().unwrap()), // as for receiver
        }
    }
}

struct LimitConfig {
    window: Duration,
    tokens: u16,
    soft_rejects: u16,
}

impl LimitConfig {
    fn new_bcast(conf: &MempoolConfig) -> Self {
        let min_sign_attempts = conf.consensus.min_sign_attempts.get();
        // two consecutive `Broadcaster`s may overlap, so x2
        Self {
            window: Duration::from_millis(
                min_sign_attempts as u64 * conf.consensus.broadcast_retry_millis.get() as u64,
            ),
            tokens: min_sign_attempts as u16 * 2,
            soft_rejects: min_sign_attempts as u16 * 2,
        }
    }
    fn new_sig(conf: &MempoolConfig) -> Self {
        let bcast = Self::new_bcast(conf);
        // each broadcast cycle pattern is at most `sig?->bcast->sig?`, so bcast x2
        Self {
            window: bcast.window,
            tokens: bcast.tokens * 2,
            soft_rejects: bcast.soft_rejects * 2,
        }
    }
}

struct SoftBucket {
    bucket: Bucket,
    soft_rejected: u16,
}

impl SoftBucket {
    fn new(now: Instant, config: &LimitConfig) -> Self {
        Self {
            bucket: Bucket::new(now, config),
            soft_rejected: 0,
        }
    }

    fn allows(&mut self, now: Instant, config: &LimitConfig) -> Result<bool, ()> {
        if self.bucket.passes(now, config) {
            self.soft_rejected = 0;
            Ok(true)
        } else if self.soft_rejected < config.soft_rejects {
            self.soft_rejected += 1;
            Ok(false)
        } else {
            Err(())
        }
    }
}

struct Bucket {
    next_refill: Instant,
    left_tokens: u16,
}

impl Bucket {
    fn new(now: Instant, config: &LimitConfig) -> Self {
        Self {
            next_refill: now + config.window,
            left_tokens: config.tokens,
        }
    }

    fn passes(&mut self, now: Instant, config: &LimitConfig) -> bool {
        if self.next_refill <= now {
            self.next_refill = now + config.window;
            self.left_tokens = config.tokens;
        }
        if self.left_tokens > 0 {
            self.left_tokens -= 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    #[tokio::test]
    async fn test_bucket() {
        let config = LimitConfig {
            window: Duration::from_secs(2),
            tokens: 5,
            soft_rejects: 0,
        };

        let start = now();

        let mut bucket = Bucket::new(start, &config);

        for n in (0..config.tokens).rev() {
            assert!(bucket.passes(start, &config), "left {n} tokens on start");
        }
        assert!(!bucket.passes(start, &config), "shall not pass on start");

        tokio::time::sleep(config.window / 2).await;

        assert!(!bucket.passes(now(), &config), "no refill");

        tokio::time::sleep(config.window / 2).await;

        assert!(bucket.passes(now(), &config), "refilled");

        assert!(bucket.passes(start, &config), "out-of-order");

        for n in (0..config.tokens - 2).rev() {
            assert!(bucket.passes(now(), &config), "left {n} tokens");
        }
        assert!(!bucket.passes(now(), &config), "shall not pass");
    }

    #[tokio::test]
    async fn test_soft_bucket() {
        let config = LimitConfig {
            window: Duration::from_secs(2),
            tokens: 5,
            soft_rejects: 6,
        };

        let start = now();

        let mut sb = SoftBucket::new(start, &config);

        for n in (0..config.tokens).rev() {
            assert_eq!(sb.allows(start, &config), Ok(true), "left {n} tokens");
        }

        for n in (0..config.soft_rejects).rev() {
            assert_eq!(sb.allows(start, &config), Ok(false), "left {n} soft");
        }

        assert_eq!(sb.allows(start, &config), Err(()), "hard after soft");
        assert_eq!(sb.allows(start, &config), Err(()), "hard again");

        tokio::time::sleep(config.window / 2).await;

        assert_eq!(sb.allows(now(), &config), Err(()), "no refill");

        tokio::time::sleep(config.window / 2).await;

        assert_eq!(sb.allows(now(), &config), Ok(true), "refilled");

        assert_eq!(sb.allows(start, &config), Ok(true), "out-of-order token");

        for n in (0..config.tokens - 2).rev() {
            assert_eq!(sb.allows(now(), &config), Ok(true), "left {n} tokens");
        }

        assert_eq!(sb.allows(now(), &config), Ok(false), "soft refilled");

        assert_eq!(sb.allows(start, &config), Ok(false), "out-of-order soft");

        for n in (0..config.soft_rejects - 2).rev() {
            assert_eq!(sb.allows(now(), &config), Ok(false), "left {n} soft");
        }

        assert_eq!(sb.allows(now(), &config), Err(()), "hard after refill");
    }
}
