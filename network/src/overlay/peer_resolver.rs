use std::collections::{hash_map, BTreeMap};
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;
use tycho_util::FastHashMap;

use crate::network::{KnownPeerHandle, WeakKnownPeerHandle};
use crate::types::{PeerId, PeerInfo};

pub(crate) struct UpdateQueue {
    min_ttl_sec: u32,
    update_before_sec: u32,
    state: Mutex<UpdateQueueState>,
    next_updated: Notify,
}

impl UpdateQueue {
    pub fn push(&self, handle: &KnownPeerHandle) -> bool {
        // NOTE: Hold the peer info guard as short as possible
        let (peer_id, created_at, update_at) = {
            let peer_info = handle.peer_info();
            let peer_info = peer_info.as_ref();

            (
                peer_info.id,
                peer_info.created_at,
                self.compute_update_at(peer_info),
            )
        };

        // Update state
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;

        let next_updated = state
            .to_update
            .first_key_value()
            .map(|(index, _)| (index.update_at, index.created_at) < (update_at, created_at))
            .unwrap_or(true);

        match state.peer_ids.entry(peer_id) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(QueueIndexShort {
                    update_at,
                    created_at,
                });
            }
            hash_map::Entry::Occupied(mut entry) => {
                let index = entry.get_mut();
                if index.created_at >= created_at {
                    return false;
                }

                state.to_update.remove(&QueueIndex {
                    update_at: index.update_at,
                    created_at: index.created_at,
                    peer_id,
                });
                index.update_at = update_at;
                index.created_at = created_at;
            }
        }

        state.to_update.insert(
            QueueIndex {
                update_at,
                created_at,
                peer_id,
            },
            handle.downgrade(),
        );

        // Notify others that the closest time to update was updated
        if next_updated {
            self.next_updated.notify_waiters();
        }

        true
    }

    // TODO: Fill batch with a threshold instead of a single value.
    pub async fn pop(&self) -> KnownPeerHandle {
        const MIN_WAIT: Duration = Duration::from_secs(1);

        // Outer loop runs on each external update
        'outer: loop {
            // Find next value to wait
            let mut next_updated = self.next_updated.notified();
            let mut next = self.state.lock().unwrap().next();

            // Inner loop runs while we know what the next value is
            loop {
                let Some((index, weak)) = next else {
                    // Wait until some value appear if current state is empty
                    next_updated.await;
                    continue 'outer;
                };

                // Compute how much to wait until the next value
                let update_at = std::time::UNIX_EPOCH + Duration::from_secs(index.update_at as u64);
                let now = std::time::SystemTime::now();

                if let Ok(remaining) = update_at.duration_since(now) {
                    // Instantly use values which are close enough
                    if remaining > MIN_WAIT {
                        tokio::select! {
                            // Retry if the next value was updated
                            _ = next_updated => continue 'outer,
                            // Wait for the next value otherwise
                            _ = tokio::time::sleep(remaining) => {}
                        }
                    }
                }

                // Handle result
                let mut state = self.state.lock().unwrap();
                state.remove_old_version(&index);

                // Try to upgrade the handle
                if let Some(value) = weak.upgrade() {
                    return value;
                }

                // Fallback to the next value to wait
                next = state.next();
                next_updated = self.next_updated.notified();
            }
        }
    }

    fn compute_update_at(&self, peer_info: &PeerInfo) -> u32 {
        let real_ttl = peer_info
            .expires_at
            .saturating_sub(self.update_before_sec)
            .saturating_sub(peer_info.created_at);

        let adjusted_ttl = std::cmp::max(real_ttl, self.min_ttl_sec);
        peer_info.created_at.saturating_add(adjusted_ttl)
    }
}

struct UpdateQueueState {
    peer_ids: FastHashMap<PeerId, QueueIndexShort>,
    to_update: BTreeMap<QueueIndex, WeakKnownPeerHandle>,
}

impl UpdateQueueState {
    fn next(&self) -> Option<(QueueIndex, WeakKnownPeerHandle)> {
        self.to_update
            .first_key_value()
            .map(|(index, handle)| (*index, handle.clone()))
    }

    fn remove_old_version(&mut self, index: &QueueIndex) {
        if let Some(prev) = self
            .peer_ids
            .get(&index.peer_id)
            .filter(|prev| prev.update_at <= index.update_at)
            .copied()
        {
            self.peer_ids.remove(&index.peer_id);
            self.to_update.remove(&QueueIndex {
                update_at: prev.update_at,
                created_at: prev.created_at,
                peer_id: index.peer_id,
            });
        }
    }
}

#[derive(Clone, Copy)]
struct QueueIndexShort {
    update_at: u32,
    created_at: u32,
}

#[derive(Clone, Copy, PartialOrd, Ord)]
struct QueueIndex {
    update_at: u32,
    created_at: u32,
    peer_id: PeerId,
}

impl Eq for QueueIndex {}
impl PartialEq for QueueIndex {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
    }
}
