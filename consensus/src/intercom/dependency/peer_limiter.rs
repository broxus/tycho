use std::num::NonZeroU16;
use std::sync::Arc;
use tokio::sync::{Semaphore, TryAcquireError};
use tycho_network::PeerId;
use tycho_util::FastDashMap;
pub struct PeerLimiter {
    data: FastDashMap<PeerId, Arc<Semaphore>>,
    limit: NonZeroU16,
}
pub struct PeerDownloadPermit {
    pub peer_id: PeerId,
    /// manual impl of [`tokio::sync::OwnedSemaphorePermit`] to not allocate on fast path
    semaphore: Arc<Semaphore>,
}
impl PeerLimiter {
    pub fn new(limit: NonZeroU16) -> Self {
        Self {
            data: FastDashMap::default(),
            limit,
        }
    }
    pub async fn get(&self, peer_id: PeerId) -> PeerDownloadPermit {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get)),
            file!(),
            27u32,
        );
        let peer_id = peer_id;
        let semaphore = match self.data.get(&peer_id) {
            Some(semaphore) => semaphore.clone(),
            None => {
                (self.data.entry(peer_id))
                    .or_insert_with(|| Arc::new(
                        Semaphore::new(self.limit.get() as usize),
                    ))
                    .clone()
            }
        };
        let permit_or_closed = match semaphore.try_acquire() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => {
                {
                    __guard.end_section(36u32);
                    let __result = semaphore.acquire().await;
                    __guard.start_section(36u32);
                    __result
                }
                    .ok()
            }
            Err(TryAcquireError::Closed) => None,
        };
        let permit = permit_or_closed.expect("never closed");
        permit.forget();
        PeerDownloadPermit {
            peer_id,
            semaphore,
        }
    }
}
impl Drop for PeerDownloadPermit {
    fn drop(&mut self) {
        self.semaphore.add_permits(1);
    }
}
