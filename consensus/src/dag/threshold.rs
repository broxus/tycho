use tokio::sync::Semaphore;

use crate::models::{PeerCount, Round};

pub struct Threshold {
    round: Round,
    peer_count: PeerCount,
    semaphore: Semaphore,
}

impl Threshold {
    pub fn new(round: Round, peer_count: PeerCount) -> Self {
        Self {
            round,
            peer_count,
            semaphore: Semaphore::new(0),
        }
    }
    pub fn add(&self) {
        self.semaphore.add_permits(1);
    }
    /// used in between calls to [`Self::reached`]
    pub fn count(&self) -> usize {
        self.semaphore.available_permits()
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
