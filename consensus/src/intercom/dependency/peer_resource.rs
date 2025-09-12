use std::num::NonZeroU16;

use parking_lot::RwLock;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::intercom::dependency::limiter::{Limiter, LimiterGuard};
use crate::models::Round;

pub struct PeerResource {
    data: RwLock<FastHashMap<PeerId, Limiter>>,
    limit: NonZeroU16,
}
pub struct PeerDownloadPermit {
    pub peer_id: PeerId,
    #[allow(unused, reason = "only the drop releases permit")]
    limiter_guard: LimiterGuard,
}

impl PeerResource {
    pub fn new(limit: NonZeroU16) -> Self {
        Self {
            data: RwLock::new(FastHashMap::default()),
            limit,
        }
    }

    pub async fn get(&self, peer_id: PeerId, round: Round) -> PeerDownloadPermit {
        let maybe_limiter = {
            let read = self.data.read();
            read.get(&peer_id).cloned()
        };
        let limiter = match maybe_limiter {
            Some(limiter) => limiter,
            None => {
                let mut write = self.data.write();
                let limiter = write
                    .entry(peer_id)
                    .or_insert_with(|| Limiter::new(self.limit));
                limiter.clone()
            }
        };
        let limiter_guard = limiter.enter(round).await;
        PeerDownloadPermit {
            peer_id,
            limiter_guard,
        }
    }
}
