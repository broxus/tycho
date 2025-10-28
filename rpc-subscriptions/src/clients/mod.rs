use std::sync::Arc;

pub use id_pool::IdPool;
use parking_lot::{Mutex, MutexGuard};
use tycho_util::FastDashMap;
use uuid::Uuid;

use crate::SubscribeError;
use crate::memory::MemoryTracker;
use crate::types::ClientId;

mod id_pool;

#[derive(Clone, Debug)]
pub struct ClientEntry {
    pub(crate) id: ClientId,
    pub(crate) lock: Arc<Mutex<()>>,
}

pub type ClientLockGuard<'a> = MutexGuard<'a, ()>;

#[derive(Debug)]
pub struct Clients {
    uuid_to_id: FastDashMap<Uuid, ClientEntry>,
    id_pool: IdPool<ClientId>,
}

impl Clients {
    pub fn new(max_capacity: u32, tracker: Arc<MemoryTracker>) -> Self {
        let id_pool = IdPool::new(max_capacity, tracker.free_lists.clone());
        debug_assert_eq!(id_pool.capacity(), max_capacity);
        let uuid_to_id =
            FastDashMap::with_capacity_and_hasher(max_capacity as usize, Default::default());

        Self {
            uuid_to_id,
            id_pool,
        }
    }

    pub fn get_or_create_entry(&self, uuid: Uuid) -> Option<ClientEntry> {
        match self.uuid_to_id.entry(uuid) {
            dashmap::Entry::Occupied(e) => {
                let entry = e.get();
                Some(ClientEntry {
                    id: entry.id,
                    lock: Arc::clone(&entry.lock),
                })
            }
            dashmap::Entry::Vacant(e) => {
                let id = self.id_pool.alloc()?;
                let lock = Arc::new(Mutex::new(()));
                e.insert(ClientEntry {
                    id,
                    lock: Arc::clone(&lock),
                });
                Some(ClientEntry { id, lock })
            }
        }
    }

    pub fn register_client(&self, uuid: Uuid) -> Result<ClientEntry, SubscribeError> {
        match self.uuid_to_id.entry(uuid) {
            dashmap::Entry::Occupied(_) => Err(SubscribeError::Collision { client_id: uuid }),
            dashmap::Entry::Vacant(e) => {
                let id = self.id_pool.alloc().ok_or(SubscribeError::MaxClients {
                    max_clients: self.id_pool.capacity(),
                })?;
                let lock = Arc::new(Mutex::new(()));
                e.insert(ClientEntry {
                    id,
                    lock: Arc::clone(&lock),
                });
                Ok(ClientEntry { id, lock })
            }
        }
    }

    pub fn get_entry(&self, uuid: Uuid) -> Option<ClientEntry> {
        self.uuid_to_id.get(&uuid).map(|entry| ClientEntry {
            id: entry.id,
            lock: Arc::clone(&entry.lock),
        })
    }

    pub fn detach_client(
        &self,
        uuid: Uuid,
        lock: &Arc<Mutex<()>>,
        _guard: &ClientLockGuard<'_>,
    ) -> Option<ClientId> {
        let existing = self.uuid_to_id.get(&uuid)?;

        // Reject stale guards from a previous entry for the same UUID.
        if !Arc::ptr_eq(&existing.lock, lock) {
            return None;
        }

        drop(existing);
        let (_, entry) = self.uuid_to_id.remove(&uuid)?;
        Some(entry.id)
    }

    pub fn reclaim_id(&self, id: ClientId) {
        self.id_pool.free(id);
    }

    pub fn client_count(&self) -> usize {
        self.uuid_to_id.len()
    }

    pub(crate) fn uuid_to_id(&self) -> &FastDashMap<Uuid, ClientEntry> {
        &self.uuid_to_id
    }
}
