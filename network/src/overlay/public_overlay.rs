use std::borrow::Borrow;
use std::collections::hash_map;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use parking_lot::{RwLock, RwLockReadGuard};
use rand::seq::SliceRandom;
use rand::Rng;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::{FastDashSet, FastHashMap};

use crate::network::Network;
use crate::overlay::OverlayId;
use crate::proto::overlay::{rpc, PublicEntry, PublicEntryToSign};
use crate::types::{BoxService, PeerId, Request, Response, Service, ServiceExt, ServiceRequest};
use crate::util::NetworkExt;

pub struct PublicOverlayBuilder {
    overlay_id: OverlayId,
    min_capacity: usize,
    entry_ttl: Duration,
    banned_peer_ids: FastDashSet<PeerId>,
}

impl PublicOverlayBuilder {
    /// Minimum capacity for public overlay.
    /// Public overlay will use suggested peers from untrusted sources to fill the overlay
    /// until it reaches this capacity.
    ///
    /// Default: 100.
    pub fn with_min_capacity(mut self, min_capacity: usize) -> Self {
        self.min_capacity = min_capacity;
        self
    }

    /// Time-to-live for each entry in the overlay.
    ///
    /// Default: 1 hour.
    pub fn with_entry_ttl(mut self, entry_ttl: Duration) -> Self {
        self.entry_ttl = entry_ttl;
        self
    }

    pub fn with_banned_peers<I>(mut self, banned_peers: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<PeerId>,
    {
        self.banned_peer_ids
            .extend(banned_peers.into_iter().map(|id| *id.borrow()));
        self
    }

    pub fn build<S>(self, service: S) -> PublicOverlay
    where
        S: Send + Sync + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        let request_prefix = tl_proto::serialize(rpc::Prefix {
            overlay_id: self.overlay_id.as_bytes(),
        });

        let entry_ttl_sec = self.entry_ttl.as_secs().try_into().unwrap_or(u32::MAX);

        PublicOverlay {
            inner: Arc::new(Inner {
                overlay_id: self.overlay_id,
                min_capacity: self.min_capacity,
                entry_ttl_sec,
                entries: RwLock::new(Default::default()),
                entry_count: AtomicUsize::new(0),
                banned_peer_ids: self.banned_peer_ids,
                service: service.boxed(),
                request_prefix: request_prefix.into_boxed_slice(),
            }),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct PublicOverlay {
    inner: Arc<Inner>,
}

impl PublicOverlay {
    pub fn builder(overlay_id: OverlayId) -> PublicOverlayBuilder {
        PublicOverlayBuilder {
            overlay_id,
            min_capacity: 100,
            entry_ttl: Duration::from_secs(3600),
            banned_peer_ids: Default::default(),
        }
    }

    #[inline]
    pub fn overlay_id(&self) -> &OverlayId {
        &self.inner.overlay_id
    }

    pub async fn query(
        &self,
        network: &Network,
        peer_id: &PeerId,
        mut request: Request,
    ) -> Result<Response> {
        self.prepend_prefix_to_body(&mut request.body);
        network.query(peer_id, request).await
    }

    pub async fn send(
        &self,
        network: &Network,
        peer_id: &PeerId,
        mut request: Request,
    ) -> Result<()> {
        self.prepend_prefix_to_body(&mut request.body);
        network.send(peer_id, request).await
    }

    /// Bans the given peer from the overlay.
    ///
    /// Returns `true` if the peer was not already banned.
    pub fn ban_peer(&self, peer_id: PeerId) -> bool {
        self.inner.banned_peer_ids.insert(peer_id)
    }

    /// Unbans the given peer from the overlay.
    ///
    /// Returns `true` if the peer was banned.
    pub fn unban_peer(&self, peer_id: &PeerId) -> bool {
        self.inner.banned_peer_ids.remove(peer_id).is_some()
    }

    pub fn read_entries(&self) -> PublicOverlayEntriesReadGuard<'_> {
        PublicOverlayEntriesReadGuard {
            entries: self.inner.entries.read(),
        }
    }

    pub(crate) fn handle_query(&self, req: ServiceRequest) -> BoxFutureOrNoop<Option<Response>> {
        if !self.inner.banned_peer_ids.contains(&req.metadata.peer_id) {
            // TODO: add peer from metadata to the overlay
            BoxFutureOrNoop::future(self.inner.service.on_query(req))
        } else {
            BoxFutureOrNoop::Noop
        }
    }

    pub(crate) fn handle_message(&self, req: ServiceRequest) -> BoxFutureOrNoop<()> {
        if !self.inner.banned_peer_ids.contains(&req.metadata.peer_id) {
            // TODO: add peer from metadata to the overlay
            BoxFutureOrNoop::future(self.inner.service.on_message(req))
        } else {
            BoxFutureOrNoop::Noop
        }
    }

    /// Adds the given entries to the overlay.
    ///
    /// NOTE: Will deadlock if called while `PublicOverlayEntriesReadGuard` is held.
    pub(crate) fn add_untrusted_entries(&self, entries: &[Arc<PublicEntry>], now: u32) {
        if entries.is_empty() {
            return;
        }

        let this = self.inner.as_ref();

        // Check if we can add more entries to the overlay and optimistically
        // increase the entry count. (if no other thread has already done so).
        let to_add = entries.len();
        let mut entry_count = this.entry_count.load(Ordering::Acquire);
        let to_add = loop {
            let to_add = match this.min_capacity.checked_sub(entry_count) {
                Some(capacity) if capacity > 0 => std::cmp::min(to_add, capacity),
                _ => return,
            };

            let res = this.entry_count.compare_exchange_weak(
                entry_count,
                entry_count + to_add,
                Ordering::Release,
                Ordering::Acquire,
            );
            match res {
                Ok(_) => break to_add,
                Err(n) => entry_count = n,
            }
        };

        // Prepare validation state
        let mut is_valid = vec![false; entries.len()];
        let mut has_valid = false;

        // First pass: verify all entries
        for (entry, is_valid) in std::iter::zip(entries, is_valid.iter_mut()) {
            if entry.is_expired(now, this.entry_ttl_sec)
                || self.inner.banned_peer_ids.contains(&entry.peer_id)
            {
                // Skip expired or banned peers early
                continue;
            }

            let Some(pubkey) = entry.peer_id.as_public_key() else {
                // Skip entries with invalid public keys
                continue;
            };

            if !pubkey.verify(
                PublicEntryToSign {
                    overlay_id: this.overlay_id.as_bytes(),
                    peer_id: &entry.peer_id,
                    created_at: entry.created_at,
                },
                &entry.signature,
            ) {
                // Skip entries with invalid signatures
                continue;
            }

            // NOTE: check all entries, even if we have more than `to_add`.
            // We might need them if some are duplicates af known entries.
            *is_valid = true;
            has_valid = true;
        }

        // Second pass: insert all valid entries (if any)
        //
        // NOTE: two passes are necessary because public key parsing and
        // signature verification can be expensive and we want to avoid
        // holding the lock for too long.
        let mut added = 0;
        if has_valid {
            let mut stored = this.entries.write();
            for (entry, is_valid) in std::iter::zip(entries, is_valid) {
                if !is_valid {
                    continue;
                }

                added += stored.insert(entry) as usize;
                if added >= to_add {
                    break;
                }
            }
        }

        // Rollback entries that were not valid and not inserted
        if added < to_add {
            this.entry_count
                .fetch_sub(to_add - added, Ordering::Release);
        }
    }

    /// Removes all expired and banned entries from the overlay.
    pub(crate) fn remove_invalid_entries(&self, now: u32) {
        let this = self.inner.as_ref();

        let mut entries = this.entries.write();
        entries.retain(|entry| {
            !entry.is_expired(now, this.entry_ttl_sec)
                && !this.banned_peer_ids.contains(&entry.peer_id)
        });
    }

    fn prepend_prefix_to_body(&self, body: &mut Bytes) {
        let this = self.inner.as_ref();

        // TODO: reduce allocations
        let mut res = BytesMut::with_capacity(this.request_prefix.len() + body.len());
        res.extend_from_slice(&this.request_prefix);
        res.extend_from_slice(body);
        *body = res.freeze();
    }
}

impl std::fmt::Debug for PublicOverlay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicOverlay")
            .field("overlay_id", &self.inner.overlay_id)
            .finish()
    }
}

struct Inner {
    overlay_id: OverlayId,
    min_capacity: usize,
    entry_ttl_sec: u32,
    entries: RwLock<PublicOverlayEntries>,
    entry_count: AtomicUsize,
    banned_peer_ids: FastDashSet<PeerId>,
    service: BoxService<ServiceRequest, Response>,
    request_prefix: Box<[u8]>,
}

#[derive(Default)]
pub struct PublicOverlayEntries {
    peer_id_to_index: FastHashMap<PeerId, usize>,
    data: Vec<Arc<PublicEntry>>,
}

impl PublicOverlayEntries {
    /// Returns a reference to one random element of the slice,
    /// or `None` if the slice is empty.
    pub fn choose<R>(&self, rng: &mut R) -> Option<&Arc<PublicEntry>>
    where
        R: Rng + ?Sized,
    {
        self.data.choose(rng)
    }

    /// Chooses `n` entries from the set, without repetition,
    /// and in random order.
    pub fn choose_multiple<R>(
        &self,
        rng: &mut R,
        n: usize,
    ) -> rand::seq::SliceChooseIter<'_, [Arc<PublicEntry>], Arc<PublicEntry>>
    where
        R: Rng + ?Sized,
    {
        self.data.choose_multiple(rng, n)
    }

    fn insert(&mut self, item: &PublicEntry) -> bool {
        match self.peer_id_to_index.entry(item.peer_id) {
            // No entry for the peer_id, insert a new one
            hash_map::Entry::Vacant(entry) => {
                entry.insert(self.data.len());
                self.data.push(Arc::new(item.clone()));
                true
            }
            // Entry for the peer_id exists, update it if the new item is newer
            hash_map::Entry::Occupied(entry) => {
                let index = *entry.get();
                let existing = &mut self.data[index];
                if existing.created_at >= item.created_at {
                    return false;
                }

                // Try to reuse the existing Arc if possible
                match Arc::get_mut(existing) {
                    Some(existing) => existing.clone_from(item),
                    None => self.data[index] = Arc::new(item.clone()),
                }
                true
            }
        }
    }

    fn remove(&mut self, peer_id: &PeerId) -> bool {
        let Some(index) = self.peer_id_to_index.remove(peer_id) else {
            return false;
        };

        // Remove the entry from the data vector
        self.data.swap_remove(index);

        // Update the swapped entry's index
        let entry = self
            .peer_id_to_index
            .get_mut(&self.data[index].peer_id)
            .expect("inconsistent state");
        *entry = index;

        true
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Arc<PublicEntry>) -> bool,
    {
        self.data.retain(|entry| {
            let keep = f(entry);
            if !keep {
                self.peer_id_to_index.remove(&entry.peer_id);
            }
            keep
        });
    }
}

pub struct PublicOverlayEntriesReadGuard<'a> {
    entries: RwLockReadGuard<'a, PublicOverlayEntries>,
}

impl std::ops::Deref for PublicOverlayEntriesReadGuard<'_> {
    type Target = PublicOverlayEntries;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use everscale_crypto::ed25519;
    use tycho_util::time::now_sec;

    use super::*;

    fn generate_public_entry(overlay: &PublicOverlay, now: u32) -> Arc<PublicEntry> {
        let keypair = ed25519::KeyPair::generate(&mut rand::thread_rng());
        let peer_id: PeerId = keypair.public_key.into();
        let signature = keypair.sign(crate::proto::overlay::PublicEntryToSign {
            overlay_id: overlay.overlay_id().as_bytes(),
            peer_id: &peer_id,
            created_at: now,
        });
        Arc::new(PublicEntry {
            peer_id,
            created_at: now,
            signature: Box::new(signature),
        })
    }

    fn generate_invalid_public_entry(now: u32) -> Arc<PublicEntry> {
        let keypair = ed25519::KeyPair::generate(&mut rand::thread_rng());
        let peer_id: PeerId = keypair.public_key.into();
        Arc::new(PublicEntry {
            peer_id,
            created_at: now,
            signature: Box::new([0; 64]),
        })
    }

    fn generate_public_entries(
        overlay: &PublicOverlay,
        now: u32,
        n: usize,
    ) -> Vec<Arc<PublicEntry>> {
        (0..n)
            .map(|_| generate_public_entry(overlay, now))
            .collect()
    }

    fn count_entries(overlay: &PublicOverlay) -> usize {
        let tracked_count = overlay.inner.entry_count.load(Ordering::Acquire);
        let guard = overlay.read_entries();
        assert_eq!(
            guard.entries.data.len(),
            guard.entries.peer_id_to_index.len(),
        );
        assert_eq!(guard.entries.data.len(), tracked_count);
        tracked_count
    }

    fn make_overlay_with_min_capacity(min_capacity: usize) -> PublicOverlay {
        PublicOverlay::builder(rand::random())
            .with_min_capacity(min_capacity)
            .build(crate::service_query_fn(|_| {
                futures_util::future::ready(None)
            }))
    }

    #[test]
    fn min_capacity_works_with_single_thread() {
        let now = now_sec();

        // Add with small portions
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = generate_public_entries(&overlay, now, 10);

            overlay.add_untrusted_entries(&entries[..5], now);
            assert_eq!(count_entries(&overlay), 5);

            overlay.add_untrusted_entries(&entries[5..], now);
            assert_eq!(count_entries(&overlay), 10);
        }

        // Add exact
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = generate_public_entries(&overlay, now, 10);
            overlay.add_untrusted_entries(&entries, now);
            assert_eq!(count_entries(&overlay), 10);
        }

        // Add once but too much
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = generate_public_entries(&overlay, now, 20);
            overlay.add_untrusted_entries(&entries, now);
            assert_eq!(count_entries(&overlay), 10);
        }

        // Add once but zero capacity
        {
            let overlay = make_overlay_with_min_capacity(0);
            let entries = generate_public_entries(&overlay, now, 10);
            overlay.add_untrusted_entries(&entries, now);
            assert_eq!(count_entries(&overlay), 0);
        }

        // Add all invalid entries
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = (0..10)
                .map(|_| generate_invalid_public_entry(now))
                .collect::<Vec<_>>();
            overlay.add_untrusted_entries(&entries, now);
            assert_eq!(count_entries(&overlay), 0);
        }

        // Add mixed invalid entries
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = [
                generate_invalid_public_entry(now),
                generate_public_entry(&overlay, now),
                generate_invalid_public_entry(now),
                generate_public_entry(&overlay, now),
                generate_invalid_public_entry(now),
                generate_public_entry(&overlay, now),
                generate_invalid_public_entry(now),
                generate_public_entry(&overlay, now),
                generate_invalid_public_entry(now),
                generate_public_entry(&overlay, now),
            ];
            overlay.add_untrusted_entries(&entries, now);
            assert_eq!(count_entries(&overlay), 5);
        }

        // Add mixed invalid entries on edge
        {
            let overlay = make_overlay_with_min_capacity(3);
            let entries = [
                generate_invalid_public_entry(now),
                generate_invalid_public_entry(now),
                generate_invalid_public_entry(now),
                generate_invalid_public_entry(now),
                generate_invalid_public_entry(now),
                generate_public_entry(&overlay, now),
                generate_public_entry(&overlay, now),
                generate_public_entry(&overlay, now),
                generate_public_entry(&overlay, now),
                generate_public_entry(&overlay, now),
            ];
            overlay.add_untrusted_entries(&entries, now);
            assert_eq!(count_entries(&overlay), 3);
        }
    }

    #[test]
    fn min_capacity_works_with_multi_thread() {
        let now = now_sec();

        let overlay = make_overlay_with_min_capacity(201);
        let entries = generate_public_entries(&overlay, now, 7 * 3 * 10);

        std::thread::scope(|s| {
            for entries in entries.chunks_exact(7 * 3) {
                s.spawn(|| {
                    for entries in entries.chunks_exact(7) {
                        overlay.add_untrusted_entries(entries, now);
                    }
                });
            }
        });

        assert_eq!(count_entries(&overlay), 201);
    }
}
