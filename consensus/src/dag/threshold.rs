use tokio::sync::Semaphore;

use crate::models::PeerCount;

pub struct Threshold {
    peer_count: PeerCount,
    semaphore: Semaphore,
}

impl Threshold {
    pub fn new(peer_count: PeerCount) -> Self {
        Self {
            peer_count,
            semaphore: Semaphore::new(0),
        }
    }
    pub fn add(&self) {
        self.semaphore.add_permits(1);
    }
    pub async fn reached(&self) {
        // drop of permits return them to semaphore for other waiters
        // semaphore is closed only on drop of DagRound - release waiters
        self.semaphore
            .acquire_many(self.peer_count.majority() as u32)
            .await
            .ok();
    }
}
