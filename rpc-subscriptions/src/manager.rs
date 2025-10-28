use std::sync::Arc;

use tycho_types::models::StdAddr;
use uuid::Uuid;

use crate::api::{SubscribeError, SubscriberManagerConfig, UnsubscribeError};
use crate::index::subscriptions::Subscriptions;
use crate::memory::MemorySnapshot;
use crate::types::{ClientId, ClientStats};

#[derive(Clone)]
pub struct SubscriberManager {
    inner: Arc<Subscriptions>,
    config: SubscriberManagerConfig,
}

impl SubscriberManager {
    pub fn new(config: SubscriberManagerConfig) -> Self {
        Self {
            inner: Arc::new(Subscriptions::new(config)),
            config,
        }
    }

    /// Subscribes `uuid` to `addr`.
    /// Returns `SubscribeError::ClientAtCapacity` once a client holds
    /// `MAX_ADDRS_PER_CLIENT` addresses.
    pub fn subscribe(&self, uuid: Uuid, addr: StdAddr) -> Result<(), SubscribeError> {
        self.subscribe_many(uuid, std::iter::once(addr))
    }

    /// Subscribes `uuid` to `addrs`.
    /// Returns `SubscribeError::ClientAtCapacity` once a client holds
    /// `MAX_ADDRS_PER_CLIENT` addresses.
    pub fn subscribe_many<I>(&self, uuid: Uuid, addrs: I) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = StdAddr>,
    {
        let entry =
            self.inner
                .clients
                .get_or_create_entry(uuid)
                .ok_or(SubscribeError::MaxClients {
                    max_clients: self.config.max_clients,
                })?;
        let guard = entry.lock.lock();

        self.inner.subscribe_many(addrs, entry.id, &guard)
    }

    pub fn unsubscribe(&self, uuid: Uuid, addr: StdAddr) -> Result<(), UnsubscribeError> {
        let entry = self
            .inner
            .clients
            .get_entry(uuid)
            .ok_or(UnsubscribeError::UnknownClient)?;
        let guard = entry.lock.lock();

        self.inner.unsubscribe(addr, entry.id, &guard)
    }

    /// Clears all subscriptions for the given `uuid`.
    /// Client *is not* removed from the manager.
    pub fn unsubscribe_all(&self, uuid: Uuid) -> Result<(), UnsubscribeError> {
        let entry = self
            .inner
            .clients
            .get_entry(uuid)
            .ok_or(UnsubscribeError::UnknownClient)?;
        let guard = entry.lock.lock();

        self.inner.client_to_int_addrs_remove(entry.id, &guard);
        Ok(())
    }

    /// Clears all subscriptions and makes `uuid` -> `ClientId` available for reuse.
    pub fn remove_client(&self, uuid: Uuid) {
        let Some(entry) = self.inner.clients.get_entry(uuid) else {
            return;
        };
        let guard = entry.lock.lock();

        if let Some(id) = self.inner.clients.detach_client(uuid, &entry.lock, &guard) {
            self.inner.client_to_int_addrs_remove(id, &guard);
            self.inner.clients.reclaim_id(id);
        }
    }

    pub fn clients_to_notify(&self, addr: StdAddr, out: &mut Vec<ClientId>) {
        self.inner.addr_to_subs_clients(addr, out);
    }

    pub fn client_count(&self) -> usize {
        self.inner.client_count()
    }

    pub fn subscription_count(&self) -> usize {
        self.inner.subscription_count()
    }

    pub fn client_stats(&self, uuid: Uuid) -> Option<ClientStats> {
        let entry = self.inner.clients.get_entry(uuid)?;
        Some(self.inner.client_stats(entry.id))
    }

    pub fn mem_snapshot(&self) -> MemorySnapshot {
        self.inner.load_stats()
    }

    #[cfg(test)]
    pub(crate) fn debug_check_invariants(&self) {
        self.inner.debug_check_invariants();
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::api::{MAX_ADDRS_PER_CLIENT, SubscriberManagerConfig};
    use crate::test_utils::{make_addr, make_uuid};

    #[test]
    fn zero_max_clients_rejects_subscribe() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(8, 0));
        let err = manager
            .subscribe_many(make_uuid(1), [make_addr(1)])
            .expect_err("expected max clients error");
        assert_eq!(err, SubscribeError::MaxClients { max_clients: 0 });
        assert_eq!(manager.client_count(), 0);
    }

    #[test]
    fn zero_max_addrs_rejects_subscribe() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(0, 2));
        let err = manager
            .subscribe_many(make_uuid(2), [make_addr(1)])
            .expect_err("expected max addrs error");
        assert_eq!(err, SubscribeError::MaxAddrs { max_addrs: 0 });
        assert_eq!(manager.subscription_count(), 0);
    }

    #[test]
    fn max_clients_boundary() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(8, 1));
        let first = manager.subscribe_many(make_uuid(1), [make_addr(1)]);
        assert!(first.is_ok());

        let err = manager
            .subscribe_many(make_uuid(2), [make_addr(2)])
            .expect_err("expected max clients error");
        assert_eq!(err, SubscribeError::MaxClients { max_clients: 1 });
        assert_eq!(manager.client_count(), 1);
    }

    #[test]
    fn max_addrs_boundary() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(1, 2));
        let uuid = make_uuid(3);
        manager.subscribe_many(uuid, [make_addr(1)]).unwrap();
        let err = manager
            .subscribe_many(uuid, [make_addr(2)])
            .expect_err("expected max addrs error");
        assert_eq!(err, SubscribeError::MaxAddrs { max_addrs: 1 });
        assert_eq!(manager.subscription_count(), 1);
    }

    #[test]
    fn per_client_limit_hits_255() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(300, 4));
        let uuid = make_uuid(4);
        for i in 0..MAX_ADDRS_PER_CLIENT {
            manager
                .subscribe_many(uuid, [make_addr(u32::from(i))])
                .unwrap();
        }

        let err = manager
            .subscribe_many(uuid, [make_addr(999)])
            .expect_err("expected per-client cap");
        assert_eq!(err, SubscribeError::ClientAtCapacity {
            client_id: ClientId::from(0u32),
            max_per_client: MAX_ADDRS_PER_CLIENT,
        });
        assert_eq!(
            manager.subscription_count(),
            usize::from(MAX_ADDRS_PER_CLIENT)
        );
    }

    #[test]
    fn remove_client_is_idempotent() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(16, 2));
        let uuid = make_uuid(5);
        manager.subscribe_many(uuid, [make_addr(1)]).unwrap();
        manager.subscribe_many(uuid, [make_addr(2)]).unwrap();

        manager.remove_client(uuid);
        manager.remove_client(uuid);

        assert_eq!(manager.client_count(), 0);
        assert_eq!(manager.subscription_count(), 0);
        manager.debug_check_invariants();
    }

    #[derive(Clone, Debug)]
    enum Op {
        Sub(u8, u8),
        Unsub(u8, u8),
        Remove(u8),
    }

    fn next_op(rng: &mut StdRng) -> Op {
        match rng.random_range(0u8..4) {
            // Heavy bias towards client 0 and addr 0-20 to force spills
            0 => Op::Sub(0, rng.random_range(0u8..20)),
            1 => Op::Unsub(0, rng.random_range(0u8..20)),
            // Occasional random noise
            2 => Op::Sub(rng.random_range(0u8..10), rng.random_range(0u8..10)),
            _ => Op::Remove(rng.random_range(0u8..10)),
        }
    }

    #[test]
    fn heavy_load_smoke() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(20, 5));
        let uuids: Vec<_> = (0..10).map(make_uuid).collect();
        let addrs: Vec<_> = (0..20).map(make_addr).collect();

        let mut rng = StdRng::seed_from_u64(1337);

        for _ in 0..200 {
            match next_op(&mut rng) {
                Op::Sub(u, a) => {
                    let uuid = uuids[u as usize];
                    let addr = addrs[a as usize].clone();
                    let _ = manager.subscribe_many(uuid, [addr]);
                }
                Op::Unsub(u, a) => {
                    let uuid = uuids[u as usize];
                    let addr = addrs[a as usize].clone();
                    let _ = manager.unsubscribe(uuid, addr);
                }
                Op::Remove(u) => {
                    let uuid = uuids[u as usize];
                    manager.remove_client(uuid);
                }
            }

            manager.debug_check_invariants();
        }
    }

    #[test]
    fn unsubscribe_all_clears_subscriptions() {
        let manager = SubscriberManager::new(SubscriberManagerConfig::new(32, 4));
        let uuid = make_uuid(2);
        let addrs: Vec<_> = (0..3).map(make_addr).collect();

        manager.subscribe_many(uuid, addrs.clone()).unwrap();
        manager.unsubscribe_all(uuid).unwrap();

        assert_eq!(manager.client_count(), 1);
        assert_eq!(manager.subscription_count(), 0);
        let stats = manager.client_stats(uuid).expect("client missing");
        assert_eq!(stats.subscription_count, 0);
    }
}
