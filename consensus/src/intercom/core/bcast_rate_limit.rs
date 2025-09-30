use std::cmp;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;

/// During [`broadcast_retry_millis`](tycho_types::models::ConsensusConfig)
/// there may occur at most 1 broadcast and 2 signature queries for each point.
/// Consensus rounds advances at least [`min_sign_attempts`](tycho_types::models::ConsensusConfig)
/// times the [`broadcast_retry_millis`].
/// We use the double of the broadcast duration as a time window because
/// two consecutive broadcasts may overlap due to [`crate::intercom::Broadcaster::run_continue`]
pub struct BcastRateLimit {
    map: FastDashMap<PeerId, BcastLimits>,
    window: Duration,
    bcast_limit: usize,
    sig_limit: usize,
}

struct BcastLimits {
    broadcasts: CallHistory,
    sig_queries: CallHistory,
}

impl BcastRateLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        let min_sign_attempts = conf.consensus.min_sign_attempts;
        Self {
            map: FastDashMap::default(),
            window: Duration::from_millis(
                min_sign_attempts.get() as u64 * conf.consensus.broadcast_retry_millis.get() as u64,
            ),
            // each broadcast cycle pattern is at most `sig?->bcast->sig?`
            // two consecutive `Broadcaster`s may overlap, so extra x2
            bcast_limit: min_sign_attempts.get() as usize * 2,
            sig_limit: min_sign_attempts.get() as usize * 2 * 2,
        }
    }

    pub fn broadcast(&self, peer_id: &PeerId) -> Result<(), ()> {
        self.history_of(peer_id, |entry| &mut entry.broadcasts)
    }

    pub fn sig_query(&self, peer_id: &PeerId) -> Result<(), ()> {
        self.history_of(peer_id, |entry| &mut entry.sig_queries)
    }

    fn history_of<F>(&self, peer_id: &PeerId, f: F) -> Result<(), ()>
    where
        F: FnOnce(&mut BcastLimits) -> &mut CallHistory,
    {
        let now = Instant::now();
        let mut entry = self.map.entry(*peer_id).or_insert_with(|| BcastLimits {
            broadcasts: CallHistory::new(self.bcast_limit, self.window, now),
            sig_queries: CallHistory::new(self.sig_limit, self.window, now),
        });
        f(&mut entry)
            .allows(self.window, now)
            .then_some(())
            .ok_or(())
    }
}

struct CallHistory {
    min_heap: BinaryHeap<cmp::Reverse<Instant>>,
}

impl CallHistory {
    fn new(len: usize, window: Duration, now: Instant) -> Self {
        let mut min_heap = BinaryHeap::with_capacity(len); // won't grow
        for _ in 0..len {
            min_heap.push(cmp::Reverse(now - window)); // length doesn't change; let it allow on start
        }
        Self { min_heap }
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
}
