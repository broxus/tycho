use std::mem;
use std::num::NonZeroU8;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, atomic};
use std::time::{Duration, Instant};

use smallvec::SmallVec;
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;
use crate::intercom::core::QueryLimitError;
use crate::models::Round;

/// During [`broadcast_retry_millis`](tycho_types::models::ConsensusConfig)
/// there may occur at most 1 signature query and 1 broadcast for each point.
/// Consensus rounds advances at least [`min_sign_attempts`](tycho_types::models::ConsensusConfig)
/// times the [`broadcast_retry_millis`].
/// We use the double of the broadcast duration as a time window because of:
/// * two consecutive broadcasts may overlap due to [`crate::intercom::Broadcaster::run_continue`]
/// * one of two broadcasts may be aborted due to the advance of consensus round
pub struct BcastRateLimit {
    map: FastDashMap<PeerId, BcastLimits>,
    window: Duration,
    min_sign_attempts: NonZeroU8,
}

// min_sign_attempts = 6 (== 5 retry intervals per round) looks like the greatest sane value
struct BcastLimits {
    broadcasts: CallHistory<12>,  // (6) x2
    sig_queries: CallHistory<22>, // (6 * 2 - 1) x2
    // bcast and sig request must not overlap for each of the two rounds
    rounds: (RoundSemaphore, RoundSemaphore),
}

impl BcastRateLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            window: Duration::from_millis(
                (conf.consensus.min_sign_attempts.get() as u64 - 1) // intervals between attempts 
                * conf.consensus.broadcast_retry_millis.get() as u64,
            ),
            min_sign_attempts: conf.consensus.min_sign_attempts,
        }
    }

    pub fn broadcast(
        &self,
        peer_id: &PeerId,
        round: Round,
    ) -> Result<RoundPermit, QueryLimitError> {
        self.history_of(peer_id, round, |entry| &mut entry.broadcasts)
    }

    pub fn sig_query(
        &self,
        peer_id: &PeerId,
        round: Round,
    ) -> Result<RoundPermit, QueryLimitError> {
        self.history_of(peer_id, round, |entry| &mut entry.sig_queries)
    }

    fn history_of<F, const N: usize>(
        &self,
        peer_id: &PeerId,
        round: Round,
        f: F,
    ) -> Result<RoundPermit, QueryLimitError>
    where
        F: FnOnce(&mut BcastLimits) -> &mut CallHistory<N>,
    {
        let now = Instant::now();
        let mut entry = match self.map.get_mut(peer_id) {
            Some(entry) => entry,
            None => (self.map.entry(*peer_id)).or_insert_with(|| {
                // patterns: fist attempt is `bcast->sig?`, others are at most `sig?->bcast->sig?`
                // outer x2: two consecutive broadcasts may overlap
                let bcast_limit = match self.min_sign_attempts {
                    NonZeroU8::MIN => 0, // unlimited
                    more => (more.get() as usize) * 2,
                };
                let sig_limit = match self.min_sign_attempts {
                    NonZeroU8::MIN => 0,
                    more => (more.get() as usize * 2 - 1) * 2,
                };
                BcastLimits {
                    broadcasts: CallHistory::new(bcast_limit, now, self.window),
                    sig_queries: CallHistory::new(sig_limit, now, self.window),
                    rounds: Default::default(),
                }
            }),
        };
        f(&mut entry)
            .allows(now, self.window)
            .then_some(())
            .ok_or(QueryLimitError::RateLimit)?;
        let (a, b) = &mut entry.rounds;
        let permit = (a.try_acquire(round))
            .inspect(|_| mem::swap(a, b)) // most likely the other will be free for the next call
            .or_else(|| b.try_acquire(round))
            .ok_or(QueryLimitError::ConcurrentQueries)?;
        Ok(permit)
    }
}

struct CallHistory<const N: usize>(SmallVec<[Instant; N]>);

impl<const N: usize> CallHistory<N> {
    fn new(limit: usize, now: Instant, window: Duration) -> Self {
        let mut inner = SmallVec::with_capacity(limit);
        for _ in 0..limit {
            inner.push(now - window);
        }
        Self(inner)
    }

    fn allows(&mut self, now: Instant, window: Duration) -> bool {
        if self.0.is_empty() {
            // length is always a limit; empty is unlimited
            return true;
        }
        let (i, _) = (self.0.iter())
            .enumerate()
            .min_by_key(|(_, request_time)| *request_time)
            .expect("cannot be empty");
        let oldest = mem::replace(&mut self.0[i], now);
        now.duration_since(oldest) >= window
    }
}

#[derive(Default)]
struct RoundSemaphore(Arc<AtomicU32>);

impl RoundSemaphore {
    fn try_acquire(&self, round: Round) -> Option<RoundPermit> {
        self.0
            .compare_exchange(
                0, // zero round cannot contain points
                round.0,
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
            )
            .is_ok()
            .then(|| RoundPermit(self.0.clone()))
    }
}

pub struct RoundPermit(Arc<AtomicU32>);

impl Drop for RoundPermit {
    fn drop(&mut self) {
        self.0.store(0, atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use anyhow::{Result, ensure};

    use super::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let start = Instant::now();
        let window = Duration::from_secs(2);
        let mut history = CallHistory::<5>::new(3, start, window);

        ensure!(history.allows(start, window), "left 2 tries");
        ensure!(history.allows(Instant::now(), window), "left 1 try");
        ensure!(history.allows(Instant::now(), window), "left 0 tries");

        ensure!(!history.allows(Instant::now(), window), "shall not pass");
        ensure!(!history.allows(Instant::now(), window), "shall not pass");

        tokio::time::sleep(Duration::from_secs(1)).await;

        ensure!(!history.allows(Instant::now(), window), "shall not pass");
        ensure!(!history.allows(Instant::now(), window), "shall not pass");

        tokio::time::sleep(Duration::from_secs(1)).await;

        ensure!(
            history.allows(Instant::now(), window),
            "replenished 3-2=1 tries, 1 used, 0 left"
        );

        ensure!(!history.allows(Instant::now(), window), "shall not pass");

        Ok(())
    }
}
