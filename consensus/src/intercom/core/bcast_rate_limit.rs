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
#[derive(Default)]
pub struct BcasterLimiter {
    broadcasts: BcastRateLimit<2>,
    sig_queries: BcastRateLimit<4>,
}

impl BcasterLimiter {
    pub fn is_broadcast_ok(&self, peer_id: &PeerId, conf: &MempoolConfig) -> bool {
        self.broadcasts.is_allowed(peer_id, conf)
    }
    pub fn is_sig_query_ok(&self, peer_id: &PeerId, conf: &MempoolConfig) -> bool {
        self.sig_queries.is_allowed(peer_id, conf)
    }
}

#[derive(Default)]
struct BcastRateLimit<const N: usize>(FastDashMap<PeerId, SmallTracker<N>>);

impl<const N: usize> BcastRateLimit<N> {
    fn is_allowed(&self, peer_id: &PeerId, conf: &MempoolConfig) -> bool {
        let now = Instant::now();
        let window = Duration::from_millis(conf.consensus.broadcast_retry_millis.get() as u64 * 2);
        let mut entry = (self.0.entry(*peer_id)).or_insert_with(|| SmallTracker::new(now, window));
        entry.allows(now, window)
    }
}

struct SmallTracker<const N: usize>([Instant; N]);

impl<const N: usize> SmallTracker<N> {
    fn new(now: Instant, window: Duration) -> Self {
        Self(std::array::from_fn(|_| now - window))
    }

    fn allows(&mut self, now: Instant, window: Duration) -> bool {
        let (i, _) = (self.0.iter())
            .enumerate()
            .min_by(|(_, a), (_, b)| a.cmp(b))
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
        let mut tracker = SmallTracker::<3>::new(start, window);

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
