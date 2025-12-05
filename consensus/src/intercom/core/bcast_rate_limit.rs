use std::num::NonZeroU8;
use std::time::{Duration, Instant};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;
use crate::intercom::core::query::permit::{QueryPermit, QueryPermits};
use crate::intercom::core::query::token_bucket::{SoftTokenBucket, TokenBucket, TokenBucketConfig};

/// During [`broadcast_retry_millis`](tycho_types::models::ConsensusConfig)
/// there may occur at most 1 broadcast and 2 signature queries for each point.
/// Consensus rounds advances at least [`min_sign_attempts`](tycho_types::models::ConsensusConfig)
/// times the [`broadcast_retry_millis`].
/// We use the double of the broadcast duration as a time window because
/// two consecutive broadcasts may overlap due to [`crate::intercom::Broadcaster::run_continue`].
/// [`QueryPermit`] is used to soft limit concurrent queries causing network error for cooldown.
pub struct BcastReceiverLimits {
    map: FastDashMap<PeerId, ReceiverLimit>,
    bcast_config: TokenBucketConfig,
    sig_config: TokenBucketConfig,
}

/// Doubles the rate limit until hard rejection
struct ReceiverLimit {
    broadcasts: SoftTokenBucket,
    sig_queries: SoftTokenBucket,
    permits: QueryPermits,
}

impl BcastReceiverLimits {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            bcast_config: bcast_config(conf),
            sig_config: sig_config(conf),
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

    fn new_entry(&self, now: Instant) -> ReceiverLimit {
        ReceiverLimit {
            broadcasts: SoftTokenBucket::new(now, &self.bcast_config),
            sig_queries: SoftTokenBucket::new(now, &self.sig_config),
            permits: QueryPermits::new(QUERY_PERMITS),
        }
    }

    pub fn remove(&self, to_remove: &[PeerId]) {
        for peer_id in to_remove {
            self.map.remove(peer_id);
        }
    }
}

pub struct BcastSenderLimits {
    map: FastDashMap<PeerId, SenderLimit>,
    bcast_config: TokenBucketConfig,
    sig_config: TokenBucketConfig,
}

struct SenderLimit {
    broadcasts: TokenBucket,
    sig_queries: TokenBucket,
    permits: QueryPermits,
}

impl BcastSenderLimits {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            bcast_config: bcast_config(conf),
            sig_config: sig_config(conf),
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

    fn new_entry(&self, now: Instant) -> SenderLimit {
        SenderLimit {
            broadcasts: TokenBucket::new(now, &self.bcast_config),
            sig_queries: TokenBucket::new(now, &self.sig_config),
            permits: QueryPermits::new(QUERY_PERMITS),
        }
    }

    pub fn remove(&self, to_remove: &[PeerId]) {
        for peer_id in to_remove {
            self.map.remove(peer_id);
        }
    }
}

/// For each round simultaneously can be queried either sig or bcast,
/// but there are two independent broadcasters in action with overlapping queries.
/// Cannot enforce any pair of queries to belong to distinct rounds,
/// because point must be checked for valid signature after rate limit is applied,
/// and the round extracted before the check cannot be trusted.
const QUERY_PERMITS: NonZeroU8 = NonZeroU8::new(2).unwrap();

fn bcast_config(conf: &MempoolConfig) -> TokenBucketConfig {
    let min_sign_attempts = conf.consensus.min_sign_attempts.get();
    // two consecutive `Broadcaster`s may overlap, so x2
    TokenBucketConfig {
        window: Duration::from_millis(
            min_sign_attempts as u64 * conf.consensus.broadcast_retry_millis.get() as u64,
        ),
        tokens: min_sign_attempts as u16 * 2,
        soft_rejects: min_sign_attempts as u16 * 2,
    }
}

fn sig_config(conf: &MempoolConfig) -> TokenBucketConfig {
    let bcast = bcast_config(conf);
    // each broadcast cycle pattern is at most `sig?->bcast->sig?`, so bcast x2
    TokenBucketConfig {
        window: bcast.window,
        tokens: bcast.tokens * 2,
        soft_rejects: bcast.soft_rejects * 2,
    }
}
