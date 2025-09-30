use std::num::NonZeroU8;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, atomic};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;

pub struct UploadRateLimit {
    map: FastDashMap<PeerId, UploadSemaphore>,
    limit: NonZeroU8,
}

impl UploadRateLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            limit: conf.consensus.download_peer_queries,
        }
    }

    pub fn try_acquire(&self, peer_id: &PeerId) -> Result<UploadPermit, ()> {
        let semaphore = match self.map.get(peer_id) {
            Some(entry) => entry.clone(),
            None => (self.map.entry(*peer_id))
                .or_insert_with(|| UploadSemaphore::new(self.limit))
                .clone(),
        };
        semaphore.try_acquire().ok_or(())
    }
}

#[derive(Clone)]
struct UploadSemaphore(Arc<AtomicU8>);

impl UploadSemaphore {
    pub fn new(limit: NonZeroU8) -> Self {
        Self(Arc::new(AtomicU8::new(limit.get())))
    }

    pub fn try_acquire(self) -> Option<UploadPermit> {
        self.0
            .fetch_update(
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
                |permits| permits.checked_sub(1),
            )
            .is_ok()
            .then(|| UploadPermit(self.0))
    }
}

pub struct UploadPermit(Arc<AtomicU8>);

impl Drop for UploadPermit {
    fn drop(&mut self) {
        self.0.fetch_add(1, atomic::Ordering::Relaxed);
    }
}
