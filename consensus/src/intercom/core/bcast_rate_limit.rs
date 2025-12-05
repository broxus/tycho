use std::cmp;
use std::collections::BinaryHeap;
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
    config: LimitConfig,
}

struct ReceiverLimits {
    broadcasts: ReceiverHistory,
    sig_queries: ReceiverHistory,
    permits: QueryPermits,
}

struct LimitConfig {
    window: Duration,
    bcast_limit: usize,
    sig_limit: usize,
}
impl LimitConfig {
    fn new(conf: &MempoolConfig) -> Self {
        let min_sign_attempts = conf.consensus.min_sign_attempts;
        Self {
            window: Duration::from_millis(
                min_sign_attempts.get() as u64 * conf.consensus.broadcast_retry_millis.get() as u64,
            ),
            // each broadcast cycle pattern is at most `sig?->bcast->sig?`
            // two consecutive `Broadcaster`s may overlap, so extra x2
            bcast_limit: min_sign_attempts.get() as usize * 2,
            sig_limit: min_sign_attempts.get() as usize * 2 * 2,
        }
    }
}

impl BcastReceiverLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            config: LimitConfig::new(conf),
        }
    }

    /// `Ok(None)` is a soft rejection that does not require ban
    pub fn broadcast(&self, peer_id: &PeerId) -> Result<Option<QueryPermit>, ()> {
        self.history_of(peer_id, |entry| &mut entry.broadcasts)
    }

    /// `Ok(None)` is a soft rejection that does not require ban
    pub fn sig_query(&self, peer_id: &PeerId) -> Result<Option<QueryPermit>, ()> {
        self.history_of(peer_id, |entry| &mut entry.sig_queries)
    }

    fn history_of<F>(&self, peer_id: &PeerId, f: F) -> Result<Option<QueryPermit>, ()>
    where
        F: FnOnce(&mut ReceiverLimits) -> &mut ReceiverHistory,
    {
        let now = Instant::now();
        let mut entry = self.map.entry(*peer_id).or_insert_with(|| ReceiverLimits {
            broadcasts: ReceiverHistory::new(self.config.bcast_limit, self.config.window, now),
            sig_queries: ReceiverHistory::new(self.config.sig_limit, self.config.window, now),
            // for each round simultaneously can be queried either sig or bcast, 2 rounds at a time;
            // queries may have extra overlaps in case initiator drops its handle and moves forward
            // but the responder keeps processing the zombie request, thus we need a soft limit;
            // broadcaster must retry the failed query only after delay, so this reduces rate;
            // cannot track request round reliably because point must be checked for valid signature
            // after rate limit is applied, and the round extracted earlier cannot be trusted
            permits: QueryPermits::new(2.try_into().unwrap()),
        });
        if f(&mut entry).allows(self.config.window, now)? {
            Ok(entry.permits.try_acquire())
        } else {
            Ok(None)
        }
    }
}

/// Doubles the rate limit until hard rejection
struct ReceiverHistory {
    history: CallHistory,
    soft_rejected: usize,
}

impl ReceiverHistory {
    fn new(len: usize, window: Duration, now: Instant) -> Self {
        Self {
            history: CallHistory::new(len, window, now),
            soft_rejected: 0,
        }
    }
    fn allows(&mut self, window: Duration, now: Instant) -> Result<bool, ()> {
        if self.history.allows(window, now) {
            self.soft_rejected = 0;
            Ok(true)
        } else if self.soft_rejected < self.history.limit() {
            self.soft_rejected += 1;
            Ok(false)
        } else {
            Err(())
        }
    }
}

pub struct BcastSenderLimit {
    map: FastDashMap<PeerId, SenderLimits>,
    config: LimitConfig,
}

struct SenderLimits {
    broadcasts: CallHistory,
    sig_queries: CallHistory,
    permits: QueryPermits,
}

impl BcastSenderLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            config: LimitConfig::new(conf),
        }
    }

    pub fn broadcast(&self, peer_id: &PeerId) -> Result<QueryPermit, ()> {
        self.history_of(peer_id, |entry| &mut entry.broadcasts)
    }

    pub fn sig_query(&self, peer_id: &PeerId) -> Result<QueryPermit, ()> {
        self.history_of(peer_id, |entry| &mut entry.sig_queries)
    }

    fn history_of<F>(&self, peer_id: &PeerId, f: F) -> Result<QueryPermit, ()>
    where
        F: FnOnce(&mut SenderLimits) -> &mut CallHistory,
    {
        let now = Instant::now();
        let mut entry = self.map.entry(*peer_id).or_insert_with(|| SenderLimits {
            broadcasts: CallHistory::new(self.config.bcast_limit, self.config.window, now),
            sig_queries: CallHistory::new(self.config.sig_limit, self.config.window, now),
            permits: QueryPermits::new(2.try_into().unwrap()), // same as for receiver
        });
        if f(&mut entry).allows(self.config.window, now) {
            entry.permits.try_acquire().ok_or(())
        } else {
            Err(())
        }
    }
}

struct CallHistory {
    min_heap: BinaryHeap<cmp::Reverse<Instant>>,
}

impl CallHistory {
    fn new(limit: usize, window: Duration, now: Instant) -> Self {
        let mut min_heap = BinaryHeap::with_capacity(limit); // won't grow
        for _ in 0..limit {
            min_heap.push(cmp::Reverse(now - window)); // length doesn't change; let it allow on start
        }
        Self { min_heap }
    }

    fn limit(&self) -> usize {
        self.min_heap.len()
    }

    fn allows(&mut self, window: Duration, now: Instant) -> bool {
        match self.min_heap.peek_mut() {
            None => true, // length is always a limit; empty is unlimited
            Some(mut oldest) => {
                let is_allowed = now.duration_since(oldest.0) >= window;
                *oldest = cmp::Reverse(now); // will be placed at right order despite the name
                is_allowed
            }
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::{Result, ensure};

    use super::*;
    use crate::test_utils::default_test_config;

    #[tokio::test]
    async fn test_heap() -> Result<()> {
        let start = Instant::now();
        let window = Duration::from_secs(2);
        let mut history = CallHistory::new(3, window, start);

        ensure!(history.allows(window, start), "left 2 tries");
        ensure!(history.allows(window, Instant::now()), "left 1 try");
        ensure!(history.allows(window, Instant::now()), "left 0 tries");

        ensure!(!history.allows(window, Instant::now()), "shall not pass");
        ensure!(!history.allows(window, Instant::now()), "shall not pass");

        tokio::time::sleep(Duration::from_secs(1)).await;

        ensure!(!history.allows(window, Instant::now()), "shall not pass");
        ensure!(!history.allows(window, Instant::now()), "shall not pass");

        tokio::time::sleep(Duration::from_secs(1)).await;

        ensure!(
            history.allows(window, Instant::now()),
            "replenished 3-2=1 tries, 1 used, 0 left"
        );

        ensure!(!history.allows(window, Instant::now()), "shall not pass");

        Ok(())
    }

    #[tokio::test]
    async fn test_soft_reject() -> Result<()> {
        let limit = BcastReceiverLimit::new(&default_test_config().conf);

        let peer_id = PeerId([0; _]);

        for _ in 0..limit.config.bcast_limit {
            assert!(
                matches!(limit.broadcast(&peer_id), Ok(Some(_))),
                "all ok at start"
            );
        }

        for _ in 0..limit.config.bcast_limit {
            assert!(
                matches!(limit.broadcast(&peer_id), Ok(None)),
                "all soft after all ok"
            );
        }

        assert!(
            matches!(limit.broadcast(&peer_id), Err(())),
            "hard after all soft"
        );
        assert!(matches!(limit.broadcast(&peer_id), Err(())), "hard again");

        tokio::time::sleep(limit.config.window).await;

        for _ in 0..limit.config.bcast_limit {
            assert!(
                matches!(limit.broadcast(&peer_id), Ok(Some(_))),
                "full time resets all ok"
            );
        }

        for _ in 0..limit.config.bcast_limit {
            assert!(
                matches!(limit.broadcast(&peer_id), Ok(None)),
                "full time resets all soft"
            );
        }

        assert!(matches!(limit.broadcast(&peer_id), Err(())), "hard");
        assert!(matches!(limit.broadcast(&peer_id), Err(())), "hard again");

        tokio::time::sleep(limit.config.window / 2).await;

        const N: usize = 3;

        for _ in 0..N {
            assert!(
                matches!(limit.broadcast(&peer_id), Err(())),
                "half time doesn't reset"
            );
        }

        tokio::time::sleep(limit.config.window / 2).await;

        for _ in 0..limit.config.bcast_limit - N {
            assert!(
                matches!(limit.broadcast(&peer_id), Ok(Some(_))),
                "full time resets all except {N} fails"
            );
        }

        for _ in 0..limit.config.bcast_limit {
            assert!(
                matches!(limit.broadcast(&peer_id), Ok(None)),
                "any ok after {N} fails resets all soft"
            );
        }

        assert!(
            matches!(limit.broadcast(&peer_id), Err(())),
            "hard after all soft"
        );

        Ok(())
    }
}
