use std::num::NonZeroU8;
use std::sync::Arc;

use tokio::sync::{Semaphore, TryAcquireError};
use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;
use crate::intercom::core::query::permit::{QueryPermit, QueryPermits};

pub struct DownloaderLimits {
    map: FastDashMap<PeerId, DownloaderLimit>,
    download_peer_queries: NonZeroU8,
}

struct DownloaderLimit {
    semaphore: Arc<Semaphore>,
}

impl DownloaderLimits {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            download_peer_queries: conf.consensus.download_peer_queries,
        }
    }

    pub async fn get(&self, peer_id: &PeerId) -> DownloadPermit {
        let semaphore = {
            let entry = (self.map.entry(*peer_id)).or_insert_with(|| self.new_entry());
            entry.semaphore.clone()
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

    fn new_entry(&self) -> DownloaderLimit {
        DownloaderLimit {
            semaphore: Arc::new(Semaphore::new(self.download_peer_queries.get() as _)),
        }
    }

    pub fn remove(&self, to_remove: &[PeerId]) {
        for peer_id in to_remove {
            self.map.remove(peer_id);
        }
    }
}

/// manual impl of [`tokio::sync::OwnedSemaphorePermit`] to not allocate on fast path
pub struct DownloadPermit(Arc<Semaphore>);
impl Drop for DownloadPermit {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

pub struct UploaderLimits {
    map: FastDashMap<PeerId, UploaderLimit>,
    download_peer_queries: NonZeroU8,
}

/// Doubles the rate limit until hard rejection
struct UploaderLimit {
    permits: QueryPermits,
    soft_rejected: u8,
}

impl UploaderLimits {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            download_peer_queries: conf.consensus.download_peer_queries,
        }
    }

    /// `Ok(None)` is a soft rejection that does not require ban
    pub fn try_acquire(&self, peer_id: &PeerId) -> Result<Option<QueryPermit>, ()> {
        let mut entry = self.map.entry(*peer_id).or_insert_with(|| UploaderLimit {
            permits: QueryPermits::new(self.download_peer_queries),
            soft_rejected: 0,
        });
        match entry.permits.try_acquire() {
            Some(permit) => {
                entry.soft_rejected = 0;
                Ok(Some(permit))
            }
            None => {
                if entry.soft_rejected < self.download_peer_queries.get() {
                    entry.soft_rejected += 1;
                    Ok(None)
                } else {
                    Err(())
                }
            }
        }
    }

    pub fn remove(&self, to_remove: &[PeerId]) {
        for peer_id in to_remove {
            self.map.remove(peer_id);
        }
    }
}
