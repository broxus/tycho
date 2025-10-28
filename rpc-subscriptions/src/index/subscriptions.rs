use std::sync::Arc;

use bytesize::ByteSize;
use dashmap::Entry;
use dashmap::mapref::entry::OccupiedEntry;
use tycho_types::models::StdAddr;
use tycho_util::FastDashMap;

use super::forward::ForwardSet;
use super::reverse::{AddResult, IntAddrs};
use crate::api::{MAX_ADDRS_PER_CLIENT, SubscribeError, SubscriberManagerConfig, UnsubscribeError};
use crate::clients::{ClientLockGuard, Clients, IdPool};
use crate::memory::{MemorySnapshot, MemoryTracker};
use crate::types::{Addr, ClientId, ClientStats, InternedAddrId};

/// Index invariants (see `debug_check_invariants` in tests):
/// - `addr_to_subs: Addr -> SubscriptionState` owns the canonical `intern_id` for each address.
/// - `int_addr_to_addr: InternedAddrId -> Addr` is a bijection with `addr_to_subs` by `intern_id`.
/// - `client_to_int_addrs: ClientId -> IntAddrs` mirrors the forward index: a client lists
///   exactly the `intern_id`s whose `ForwardSet` also contains that client.
///
/// These invariants must hold after every mutation in `subscribe`, `unsubscribe`, and
/// `purge_client`; the retry loop in `subscribe` re-establishs them after races.
#[derive(Debug)]
pub struct Subscriptions {
    pub(crate) clients: Clients,
    client_to_int_addrs: FastDashMap<ClientId, IntAddrs>,
    int_addr_to_addr: FastDashMap<InternedAddrId, Addr>,
    addr_to_subs: FastDashMap<Addr, SubscriptionState>,
    addr_ids: IdPool<InternedAddrId>,

    tracker: Arc<MemoryTracker>,
    config: SubscriberManagerConfig,
}

#[derive(Debug)]
pub(crate) struct SubscriptionState {
    pub(crate) intern_id: InternedAddrId,
    pub(crate) subscribed_clients: ForwardSet,
}

impl Subscriptions {
    pub fn new(config: SubscriberManagerConfig) -> Self {
        let tracker = Arc::new(MemoryTracker::default());
        let addr_to_subs = FastDashMap::default();
        let int_addr_to_addr = FastDashMap::default();
        let client_to_int_addrs = FastDashMap::default();
        let addr_ids = IdPool::new(config.max_addrs, tracker.free_lists.clone());
        let clients = Clients::new(config.max_clients, Arc::clone(&tracker));

        Self {
            clients,
            client_to_int_addrs,
            int_addr_to_addr,
            addr_to_subs,
            addr_ids,
            tracker,
            config,
        }
    }

    pub fn subscribe_many<I>(
        &self,
        addrs: I,
        client_id: ClientId,
        _client_guard: &ClientLockGuard<'_>,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = StdAddr>,
    {
        let mut addrs = addrs.into_iter().peekable();
        if addrs.peek().is_none() {
            return Ok(());
        }

        let mut client_to_addr_entry = match self.client_to_int_addrs.entry(client_id) {
            Entry::Occupied(occ) => occ,
            Entry::Vacant(vac) => vac.insert_entry(IntAddrs::new()),
        };

        for addr_to_subscribe in addrs {
            let addr_to_subscribe: Addr = addr_to_subscribe.into();

            // Retry if the forward index for this address changes after we update the reverse index.
            // This happens if the `addr_to_subscribe` entry is removed/re-interned concurrently
            loop {
                // Intern the address
                let Some((addr_id, created)) = self.addr_to_subs_ensure(addr_to_subscribe) else {
                    // FAILURE: Max addresses reached globally
                    // Cleanup: If have no subscriptions left for this client, remove kv pair.
                    if client_to_addr_entry.get().is_empty() {
                        let subs = client_to_addr_entry.remove();
                        self.tracker
                            .client_to_int_addrs
                            .sub_bytes(subs.heap_bytes());
                    }
                    return Err(SubscribeError::MaxAddrs {
                        max_addrs: self.config.max_addrs,
                    });
                };

                // UUpdate reverse index: ClientId -> InternedAddrId
                let (add_result, delta) = client_to_addr_entry.get_mut().add_with_delta(addr_id);
                self.tracker.client_to_int_addrs.add_bytes(delta);

                match add_result {
                    AddResult::Inserted | AddResult::AlreadyPresent => {
                        // Update forward index: Addr -> subscribed clients
                        if self.addr_to_subs_add_client(addr_to_subscribe, addr_id, client_id) {
                            // Success: break retry loop, move to next addr
                            break;
                        }

                        // ROLLBACK:
                        // Forward update failed (address entry changed or was removed),
                        // roll back the reverse index update and retry
                        let prev_bytes = client_to_addr_entry.get().heap_bytes();
                        client_to_addr_entry.get_mut().remove(addr_id);
                        let post_bytes = client_to_addr_entry.get().heap_bytes();

                        if prev_bytes > post_bytes {
                            self.tracker
                                .client_to_int_addrs
                                .sub_bytes(prev_bytes - post_bytes);
                        }
                    }
                    AddResult::AtCapacity => {
                        // FAILURE: client reached per-client limit
                        // If we just created an address entry, and it has no subscribers,
                        // remove it to avoid leaking an interned id
                        if created
                            && let Entry::Occupied(se) = self.addr_to_subs.entry(addr_to_subscribe)
                            && se.get().intern_id == addr_id
                            && se.get().subscribed_clients.is_empty()
                        {
                            self.addr_to_subs_remove(se);
                        }

                        if client_to_addr_entry.get().is_empty() {
                            let subs = client_to_addr_entry.remove();
                            self.tracker
                                .client_to_int_addrs
                                .sub_bytes(subs.heap_bytes());
                        }

                        return Err(SubscribeError::ClientAtCapacity {
                            client_id,
                            max_per_client: MAX_ADDRS_PER_CLIENT,
                        });
                    }
                }
            }
        }

        // drop an empty reverse-index entry (should be unreachable but just in case)
        if client_to_addr_entry.get().is_empty() {
            let subs = client_to_addr_entry.remove();
            self.tracker
                .client_to_int_addrs
                .sub_bytes(subs.heap_bytes());
        }

        Ok(())
    }

    pub fn unsubscribe(
        &self,
        addr: StdAddr,
        client_id: ClientId,
        _client_guard: &ClientLockGuard<'_>,
    ) -> Result<(), UnsubscribeError> {
        let addr = addr.into();

        let addr_id = match self.addr_to_subs.get(&addr) {
            Some(g) => g.intern_id,
            None => return Err(UnsubscribeError::UnknownAddress),
        };

        match self.client_to_int_addrs.entry(client_id) {
            Entry::Occupied(mut ce) => {
                let removed = ce.get_mut().remove(addr_id);

                if ce.get().is_empty() {
                    let subs = ce.remove();
                    let heap = subs.heap_bytes();
                    self.tracker.client_to_int_addrs.sub_bytes(heap);
                }

                if removed {
                    self.addr_to_subs_cleanup(addr, client_id);
                    Ok(())
                } else {
                    Err(UnsubscribeError::NotSubscribed)
                }
            }
            Entry::Vacant(_) => Err(UnsubscribeError::UnknownClient),
        }
    }

    pub(crate) fn addr_to_subs_clients(&self, addr: StdAddr, out: &mut Vec<ClientId>) {
        out.clear();
        let addr = addr.into();
        let Some(guard) = self.addr_to_subs.get(&addr) else {
            return;
        };
        guard.subscribed_clients.extend_into(out);
    }

    pub fn client_count(&self) -> usize {
        self.clients.client_count()
    }

    pub fn subscription_count(&self) -> usize {
        self.client_to_int_addrs
            .iter()
            .map(|entry| entry.value().len())
            .sum()
    }

    pub fn client_stats(&self, client_id: ClientId) -> ClientStats {
        let subscription_count = self
            .client_to_int_addrs
            .get(&client_id)
            .map_or(0, |subs| subs.len());
        ClientStats {
            client_id,
            subscription_count,
        }
    }

    /// Purges all subscriptions for `client_id`
    pub(crate) fn client_to_int_addrs_remove(
        &self,
        client_id: ClientId,
        _client_guard: &ClientLockGuard<'_>,
    ) {
        let Some((_, subs)) = self.client_to_int_addrs.remove(&client_id) else {
            return;
        };
        let heap = subs.heap_bytes();
        self.tracker.client_to_int_addrs.sub_bytes(heap);

        for addr_id in subs.iter() {
            if let Some(addr_ref) = self.int_addr_to_addr.get(&addr_id) {
                let addr = *addr_ref;
                drop(addr_ref);

                self.addr_to_subs_cleanup(addr, client_id);
            }
        }
    }

    #[inline]
    fn addr_to_subs_ensure(&self, addr: Addr) -> Option<(InternedAddrId, bool)> {
        match self.addr_to_subs.entry(addr) {
            Entry::Occupied(e) => Some((e.get().intern_id, false)),
            Entry::Vacant(e) => {
                let intern_id = self.addr_ids.alloc()?;
                self.int_addr_to_addr.insert(intern_id, addr);
                let state = SubscriptionState {
                    intern_id,
                    subscribed_clients: ForwardSet::new(),
                };
                let entry = e.insert(state);
                drop(entry);
                Some((intern_id, true))
            }
        }
    }

    fn addr_to_subs_remove(&self, entry: OccupiedEntry<'_, Addr, SubscriptionState>) {
        let state = entry.get();
        let set_bytes = state.subscribed_clients.heap_bytes();
        let intern_id = state.intern_id;

        entry.remove();

        self.tracker.subs_clients.sub_bytes(set_bytes);
        self.int_addr_to_addr.remove(&intern_id);
        self.addr_ids.free(intern_id);
    }

    #[inline]
    fn addr_to_subs_add_client(
        &self,
        addr: Addr,
        addr_id: InternedAddrId,
        client_id: ClientId,
    ) -> bool {
        if let Some(mut fe) = self.addr_to_subs.get_mut(&addr) {
            if fe.intern_id != addr_id {
                return false;
            }

            let delta = fe.subscribed_clients.add_with_delta(client_id);
            self.tracker.subs_clients.add_bytes(delta);
            return true;
        }

        false
    }

    fn addr_to_subs_cleanup(&self, addr: Addr, client_id: ClientId) {
        match self.addr_to_subs.entry(addr) {
            Entry::Occupied(mut se) => {
                let set = &mut se.get_mut().subscribed_clients;
                let _ = set.remove(client_id);
                if set.is_empty() {
                    self.addr_to_subs_remove(se);
                }
            }
            Entry::Vacant(_) => {}
        }
    }

    pub fn load_stats(&self) -> MemorySnapshot {
        fn cap_bytes<K, V, S>(map: &dashmap::DashMap<K, V, S>) -> u64
        where
            K: Eq + std::hash::Hash,
            S: std::hash::BuildHasher + Clone,
        {
            (map.capacity() * (size_of::<K>() + size_of::<V>())) as u64
        }

        let addr_to_subs = ByteSize::b(cap_bytes(&self.addr_to_subs));
        let int_addr_to_addr = ByteSize::b(cap_bytes(&self.int_addr_to_addr));
        let base = cap_bytes(&self.client_to_int_addrs);
        let reverse = self.tracker.client_to_int_addrs.load();
        let client_to_int_addrs = ByteSize::b(base + reverse.as_u64());
        let clients = ByteSize::b(cap_bytes(self.clients.uuid_to_id()));
        let subs_clients = self.tracker.subs_clients.load();
        let free_lists = self.tracker.free_lists.load();
        let total = addr_to_subs
            + subs_clients
            + int_addr_to_addr
            + client_to_int_addrs
            + clients
            + free_lists;

        MemorySnapshot {
            addr_to_subs,
            subs_clients,
            int_addr_to_addr,
            client_to_int_addrs,
            clients,
            free_lists,
            total,
        }
    }
}

#[cfg(test)]
impl Subscriptions {
    pub(crate) fn debug_check_invariants(&self) {
        // 1. Address -> InternedId and revers must be a perfect bijection
        println!("Stage 1");
        assert_eq!(
            self.addr_to_subs.len(),
            self.int_addr_to_addr.len(),
            "Map counts differ"
        );
        for entry in self.addr_to_subs.iter() {
            let (addr, state) = entry.pair();
            assert_eq!(
                self.int_addr_to_addr.get(&state.intern_id).as_deref(),
                Some(addr),
                "ID {} does not map back to {:?}",
                state.intern_id,
                addr
            );
        }

        // 2. Forward Set -> Reverse Index Symmetry
        println!("Stage 2");
        for entry in self.addr_to_subs.iter() {
            let (addr, state) = entry.pair();
            let mut clients = Vec::new();
            state.subscribed_clients.extend_into(&mut clients);

            for cid in clients {
                let rev = self
                    .client_to_int_addrs
                    .get(&cid)
                    .expect("Missing client reverse entry");
                assert!(
                    rev.iter().any(|id| id == state.intern_id),
                    "Client {:?} in forward set for {:?}, but missing from reverse index",
                    cid,
                    addr
                );
            }
        }

        // 3. Reverse Index -> Forward Set Symmetry
        println!("Stage 3");
        for entry in self.client_to_int_addrs.iter() {
            let (cid, subs) = entry.pair();
            for id in subs.iter() {
                let addr = self
                    .int_addr_to_addr
                    .get(&id)
                    .expect("Dangling ID in reverse index");
                let fwd = self
                    .addr_to_subs
                    .get(&addr)
                    .expect("Address in reverse index missing from addr_to_subs");

                assert!(
                    fwd.subscribed_clients.contains(*cid),
                    "Reverse index says {:?} owns {:?}, but forward set disagrees",
                    cid,
                    addr
                );
            }
        }
    }

    // total heap bytes in forward and reverse indices
    pub(crate) fn debug_recompute_dynamic(&self) -> (usize, usize) {
        let forward = self
            .addr_to_subs
            .iter()
            .map(|entry| entry.value().subscribed_clients.heap_bytes())
            .sum();
        let reverse = self
            .client_to_int_addrs
            .iter()
            .map(|entry| entry.value().heap_bytes())
            .sum();

        (forward, reverse)
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;
    use rand::prelude::IndexedRandom;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::api::{SubscribeError, SubscriberManagerConfig};
    use crate::test_utils::make_addr;

    #[test]
    fn no_leak_when_client_at_capacity() {
        let subs = Subscriptions::new(SubscriberManagerConfig::new(300, 1));
        let client = ClientId::from(1u32);
        let lock = Mutex::new(());

        for i in 0..255 {
            let addr = make_addr(i);
            let guard = lock.lock();
            assert!(subs.subscribe_many([addr], client, &guard).is_ok());
        }

        let overflow = make_addr(255);
        let guard = lock.lock();
        assert!(matches!(
            subs.subscribe_many([overflow.clone()], client, &guard),
            Err(SubscribeError::ClientAtCapacity { .. })
        ));

        subs.debug_check_invariants();

        let overflow_key: Addr = overflow.into();
        assert!(!subs.addr_to_subs.contains_key(&overflow_key));
        assert_eq!(subs.int_addr_to_addr.len(), 255);
        let id = subs.addr_ids.alloc().expect("expected recycled id");
        assert_eq!(id, InternedAddrId::from(255));
        assert_eq!(subs.addr_ids.free_len(), 0);
    }

    #[test]
    fn forward_reverse_invariants_survive_churn() {
        let subs = Subscriptions::new(SubscriberManagerConfig::new(1024, 16));
        let addrs: Vec<StdAddr> = (0..8).map(make_addr).collect();
        let clients: Vec<ClientId> = (1..=4).map(ClientId::from).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let lock = Mutex::new(());

        for _ in 0..500 {
            let client = *clients.choose(&mut rng).unwrap();
            let addr = addrs.choose(&mut rng).unwrap().clone();
            if rng.random_bool(0.6) {
                let guard = lock.lock();
                let _ = subs.subscribe_many([addr], client, &guard);
            } else {
                let guard = lock.lock();
                let _ = subs.unsubscribe(addr, client, &guard);
            }
        }

        subs.debug_check_invariants();
        let (forward, reverse) = subs.debug_recompute_dynamic();
        let tracked_forward = subs.tracker.subs_clients.load().as_u64() as usize;
        let tracked_reverse = subs.tracker.client_to_int_addrs.load().as_u64() as usize;

        assert_eq!(tracked_forward, forward);
        assert_eq!(tracked_reverse, reverse);
    }
}
