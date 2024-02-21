use std::collections::hash_map;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use parking_lot::{RwLock, RwLockReadGuard};
use rand::seq::SliceRandom;
use rand::Rng;
use tycho_util::FastHashMap;

use crate::network::Network;
use crate::overlay::OverlayId;
use crate::proto::overlay::{rpc, PublicEntry, PublicEntryToSign};
use crate::types::{BoxService, PeerId, Request, Response, Service, ServiceExt, ServiceRequest};
use crate::util::NetworkExt;

pub struct PublicOverlayBuilder {
    overlay_id: OverlayId,
    min_capacity: usize,
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

    pub fn build<S>(self, service: S) -> PublicOverlay
    where
        S: Send + Sync + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        let request_prefix = tl_proto::serialize(rpc::Prefix {
            overlay_id: self.overlay_id.as_bytes(),
        });

        PublicOverlay {
            overlay_id: self.overlay_id,
            min_capacity: self.min_capacity,
            entries: RwLock::new(Default::default()),
            entry_count: AtomicUsize::new(0),
            service: service.boxed(),
            request_prefix: request_prefix.into_boxed_slice(),
        }
    }
}

pub struct PublicOverlay {
    overlay_id: OverlayId,
    min_capacity: usize,
    entries: RwLock<PublicOverlayEntries>,
    entry_count: AtomicUsize,
    service: BoxService<ServiceRequest, Response>,
    request_prefix: Box<[u8]>,
}

impl PublicOverlay {
    pub fn builder(overlay_id: OverlayId) -> PublicOverlayBuilder {
        PublicOverlayBuilder {
            overlay_id,
            min_capacity: 100,
        }
    }

    #[inline]
    pub fn overlay_id(&self) -> &OverlayId {
        &self.overlay_id
    }

    #[inline]
    pub fn service(&self) -> &BoxService<ServiceRequest, Response> {
        &self.service
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

    /// Adds the given entries to the overlay.
    pub fn add_untrusted_entries(&self, entries: &[Arc<PublicEntry>]) {
        if entries.is_empty() {
            return;
        }

        // Check if we can add more entries to the overlay and optimistically
        // increase the entry count. (if no other thread has already done so).
        let to_add = entries.len();
        let mut entry_count = self.entry_count.load(Ordering::Acquire);
        let to_add = loop {
            let to_add = match self.min_capacity.checked_sub(entry_count) {
                Some(capacity) if capacity > 0 => std::cmp::min(to_add, capacity),
                _ => return,
            };

            let res = self.entry_count.compare_exchange_weak(
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
        let mut valid_count = 0;

        // First pass: verify all entries
        for (entry, is_valid) in std::iter::zip(entries, is_valid.iter_mut()) {
            let Some(pubkey) = entry.peer_id.as_public_key() else {
                continue;
            };

            if !pubkey.verify(
                PublicEntryToSign {
                    overlay_id: self.overlay_id.as_bytes(),
                    peer_id: &entry.peer_id,
                    created_at: entry.created_at,
                },
                &entry.signature,
            ) {
                continue;
            }

            *is_valid = true;
            valid_count += 1;

            if valid_count >= to_add {
                break;
            }
        }

        // Second pass: insert all valid entries (if any)
        //
        // NOTE: two passes are necessary because public key parsing and
        // signature verification can be expensive and we want to avoid
        // holding the lock for too long.
        if valid_count > 0 {
            let mut stored = self.entries.write();
            for (entry, is_valid) in std::iter::zip(entries, is_valid) {
                if is_valid {
                    stored.insert(entry);
                }
            }
        }

        // Rollback entries that were not valid and not inserted
        if valid_count < to_add {
            self.entry_count
                .fetch_sub(to_add - valid_count, Ordering::Release);
        }
    }

    /// NOTE: WILL CAUSE DEADLOCK if guard will be held while calling
    /// `add_entries` or similar methods.
    pub(crate) fn read_entries(&self) -> PublicOverlayEntriesReadGuard<'_> {
        PublicOverlayEntriesReadGuard {
            entries: self.entries.read(),
        }
    }

    fn prepend_prefix_to_body(&self, body: &mut Bytes) {
        // TODO: reduce allocations
        let mut res = BytesMut::with_capacity(self.request_prefix.len() + body.len());
        res.extend_from_slice(&self.request_prefix);
        res.extend_from_slice(body);
        *body = res.freeze();
    }
}

#[derive(Default)]
struct PublicOverlayEntries {
    peer_id_to_index: FastHashMap<PeerId, usize>,
    data: Vec<Arc<PublicEntry>>,
}

impl PublicOverlayEntries {
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
}

pub(crate) struct PublicOverlayEntriesReadGuard<'a> {
    entries: RwLockReadGuard<'a, PublicOverlayEntries>,
}

impl PublicOverlayEntriesReadGuard<'_> {
    /// Returns a reference to one random element of the slice,
    /// or `None` if the slice is empty.
    pub fn choose<R>(&self, rng: &mut R) -> Option<&Arc<PublicEntry>>
    where
        R: Rng + ?Sized,
    {
        self.entries.data.choose(rng)
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
        self.entries.data.choose_multiple(rng, n)
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
            overlay_id: overlay.overlay_id.as_bytes(),
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
        let tracked_count = overlay.entry_count.load(Ordering::Acquire);
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

            overlay.add_untrusted_entries(&entries[..5]);
            assert_eq!(count_entries(&overlay), 5);

            overlay.add_untrusted_entries(&entries[5..]);
            assert_eq!(count_entries(&overlay), 10);
        }

        // Add exact
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = generate_public_entries(&overlay, now, 10);
            overlay.add_untrusted_entries(&entries);
            assert_eq!(count_entries(&overlay), 10);
        }

        // Add once but too much
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = generate_public_entries(&overlay, now, 20);
            overlay.add_untrusted_entries(&entries);
            assert_eq!(count_entries(&overlay), 10);
        }

        // Add once but zero capacity
        {
            let overlay = make_overlay_with_min_capacity(0);
            let entries = generate_public_entries(&overlay, now, 10);
            overlay.add_untrusted_entries(&entries);
            assert_eq!(count_entries(&overlay), 0);
        }

        // Add all invalid entries
        {
            let overlay = make_overlay_with_min_capacity(10);
            let entries = (0..10)
                .map(|_| generate_invalid_public_entry(now))
                .collect::<Vec<_>>();
            overlay.add_untrusted_entries(&entries);
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
            overlay.add_untrusted_entries(&entries);
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
            overlay.add_untrusted_entries(&entries);
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
                        overlay.add_untrusted_entries(entries);
                    }
                });
            }
        });

        assert_eq!(count_entries(&overlay), 201);
    }
}
