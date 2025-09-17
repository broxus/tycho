use std::sync::atomic::AtomicU8;
use std::sync::{Arc, atomic};

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::engine::MempoolConfig;

pub struct UploadRateLimit {
    map: FastDashMap<PeerId, Arc<AtomicU8>>,
    default: u8,
}

pub struct UploadPermit(Arc<AtomicU8>);

impl UploadRateLimit {
    pub fn new(conf: &MempoolConfig) -> Self {
        Self {
            map: FastDashMap::default(),
            default: conf.consensus.download_peer_queries.get(),
        }
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<UploadPermit> {
        let counter = match self.map.get(peer_id) {
            Some(entry) => entry.clone(),
            None => (self.map.entry(*peer_id))
                .or_insert_with(|| Arc::new(AtomicU8::new(self.default)))
                .clone(),
        };
        let is_allowed = counter
            .fetch_update(
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
                |permits| permits.checked_sub(1),
            )
            .is_ok();
        is_allowed.then(|| UploadPermit(counter))
    }
}

impl Drop for UploadPermit {
    fn drop(&mut self) {
        self.0.fetch_add(1, atomic::Ordering::Relaxed);
    }
}
