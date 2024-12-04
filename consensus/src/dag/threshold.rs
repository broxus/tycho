use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use tokio::sync::Semaphore;

use crate::models::{PeerCount, Round};

pub struct Threshold {
    round: Round,
    peer_count: PeerCount,
    semaphore: Semaphore,
    count: AtomicUsize,
}

impl Threshold {
    pub fn new(round: Round, peer_count: PeerCount) -> Self {
        Self {
            round,
            peer_count,
            semaphore: Semaphore::new(0),
            count: AtomicUsize::new(0),
        }
    }
    pub fn add(&self) {
        self.semaphore.add_permits(1);
        self.count.fetch_add(1, atomic::Ordering::Relaxed);
    }
    /// used in between calls to [`Self::reached`]
    pub fn count(&self) -> usize {
        self.count.load(atomic::Ordering::Relaxed)
    }
    pub async fn reached(&self) {
        // drop of permits return them to semaphore for other waiters
        // semaphore is closed only on drop of DagRound - release waiters
        match self
            .semaphore
            .acquire_many(self.peer_count.majority() as u32)
            .await
        {
            Ok(_permits) => {}
            Err(e) => {
                tracing::warn!("threshold for round {} will hang: {e}", self.round.0);
                futures_util::future::pending::<()>().await;
            }
        };
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::dag::threshold::Threshold;
    use crate::models::PeerCount;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let total_peers = 10;

        let thresh = Arc::new(Threshold::new(Round(0), PeerCount::try_from(total_peers)?));
        let thresh2 = thresh.clone();

        let handle = tokio::spawn(async move {
            thresh2.reached().await;
            println!("reached");
        });

        for i in 1..total_peers {
            tokio::time::sleep(Duration::from_millis(10)).await;
            thresh.add();
            println!("count {}", thresh.count());
            assert_eq!(i, thresh.count(), "must return count");
        }

        handle.await?;
        Ok(())
    }
}
