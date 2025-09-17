use std::mem;
use std::time::{Duration, Instant};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;

/// Inside a window of a [`broadcast_retry_millis`](tycho_types::models::ConsensusConfig)
/// there might occur 1 broadcast and 2 signature queries.
/// But we have to use twice the values because
/// queries may occur close to the border of two neighbouring intervals:
/// in that case the opposite ends of those intervals must be empty.
pub struct BcastRateLimit {
    broadcasts: PeerCallTracker<2>,
    sig_queries: PeerCallTracker<4>,
    window: Duration,
}

impl BcastRateLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            broadcasts: PeerCallTracker::default(),
            sig_queries: PeerCallTracker::default(),
            window: Duration::from_millis(conf.consensus.broadcast_retry_millis.get() as u64 * 2),
        }
    }

    pub fn is_broadcast_ok(&self, peer_id: &PeerId) -> bool {
        self.broadcasts.is_allowed(peer_id, self.window)
    }

    pub fn is_sig_query_ok(&self, peer_id: &PeerId) -> bool {
        self.sig_queries.is_allowed(peer_id, self.window)
    }
}

#[derive(Default)]
struct PeerCallTracker<const N: usize>(FastDashMap<PeerId, CallTracker<N>>);

impl<const N: usize> PeerCallTracker<N> {
    fn is_allowed(&self, peer_id: &PeerId, window: Duration) -> bool {
        let now = Instant::now();
        let mut entry = match self.0.get_mut(peer_id) {
            Some(entry) => entry,
            None => (self.0.entry(*peer_id)).or_insert_with(|| CallTracker::new(now, window)),
        };
        entry.allows(now, window)
    }
}

struct CallTracker<const N: usize>([Instant; N]);

impl<const N: usize> CallTracker<N> {
    fn new(now: Instant, window: Duration) -> Self {
        Self(std::array::from_fn(|_| now - window))
    }

    fn allows(&mut self, now: Instant, window: Duration) -> bool {
        let (i, _) = (self.0.iter())
            .enumerate()
            .min_by_key(|(_, request_time)| *request_time)
            .expect("N cannot be zero");
        let oldest = mem::replace(&mut self.0[i], now);
        now.duration_since(oldest) >= window
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
        let mut tracker = CallTracker::<3>::new(start, window);

        ensure!(tracker.allows(start, window), "left 2 tries");
        ensure!(tracker.allows(Instant::now(), window), "left 1 try");
        ensure!(tracker.allows(Instant::now(), window), "left 0 tries");

        ensure!(!tracker.allows(Instant::now(), window), "shall not pass");
        ensure!(!tracker.allows(Instant::now(), window), "shall not pass");

        tokio::time::sleep(Duration::from_secs(1)).await;

        ensure!(!tracker.allows(Instant::now(), window), "shall not pass");
        ensure!(!tracker.allows(Instant::now(), window), "shall not pass");

        tokio::time::sleep(Duration::from_secs(1)).await;

        ensure!(
            tracker.allows(Instant::now(), window),
            "replenished 3-2=1 tries, 1 used, 0 left"
        );

        ensure!(!tracker.allows(Instant::now(), window), "shall not pass");

        Ok(())
    }
}
