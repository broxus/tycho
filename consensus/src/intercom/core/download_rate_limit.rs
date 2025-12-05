use std::num::NonZeroU8;
use std::sync::Arc;

use tokio::sync::{Semaphore, TryAcquireError};
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;

pub struct DownloaderRateLimit {
    // fair semaphore is enough: second layer of by-round priority like in `Limiter` makes to starve
    data: FastDashMap<PeerId, Arc<Semaphore>>,
    limit: NonZeroU8,
}

/// manual impl of [`tokio::sync::OwnedSemaphorePermit`] to not allocate on fast path
pub struct DownloadPermit(Arc<Semaphore>);

impl DownloaderRateLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            data: FastDashMap::default(),
            limit: conf.consensus.download_peer_queries,
        }
    }

    pub async fn get(&self, peer_id: &PeerId) -> DownloadPermit {
        let semaphore = match self.data.get(peer_id) {
            Some(semaphore) => semaphore.clone(),
            None => (self.data.entry(*peer_id))
                .or_insert_with(|| Arc::new(Semaphore::new(self.limit.get() as usize)))
                .clone(),
        };
        let permit_or_closed = match semaphore.try_acquire() {
            Ok(permit) => Some(permit), // no alloc on fast path
            Err(TryAcquireError::NoPermits) => semaphore.acquire().await.ok(),
            Err(TryAcquireError::Closed) => None,
        };
        let permit = permit_or_closed.expect("never closed");
        permit.forget();
        DownloadPermit(semaphore)
    }
}

impl Drop for DownloadPermit {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
