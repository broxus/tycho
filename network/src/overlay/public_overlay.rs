use std::collections::hash_map;
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};
use rand::seq::SliceRandom;
use rand::Rng;
use tycho_util::FastHashMap;

use crate::overlay::OverlayId;
use crate::proto::overlay::{PublicEntry, PublicEntryToSign};
use crate::types::{BoxService, PeerId, Response, Service, ServiceExt, ServiceRequest};

pub struct PublicOverlayBuilder {
    overlay_id: OverlayId,
}

impl PublicOverlayBuilder {
    pub fn build<S>(self, service: S) -> PublicOverlay
    where
        S: Send + Sync + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        PublicOverlay {
            overlay_id: self.overlay_id,
            entries: RwLock::new(Default::default()),
            service: service.boxed(),
        }
    }
}

pub struct PublicOverlay {
    overlay_id: OverlayId,
    entries: RwLock<PublicOverlayEntries>,
    service: BoxService<ServiceRequest, Response>,
}

impl PublicOverlay {
    pub fn builder(overlay_id: OverlayId) -> PublicOverlayBuilder {
        PublicOverlayBuilder { overlay_id }
    }

    #[inline]
    pub fn overlay_id(&self) -> &OverlayId {
        &self.overlay_id
    }

    #[inline]
    pub fn service(&self) -> &BoxService<ServiceRequest, Response> {
        &self.service
    }

    // TODO: fail entire insert if any entry fails to verify?
    /// Adds the given entries to the overlay.
    pub fn add_entries(&self, entries: &[Arc<PublicEntry>]) {
        let mut is_valid = vec![false; entries.len()];
        let mut has_valid = false;

        // First pass: verify all entries
        for (i, entry) in entries.iter().enumerate() {
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

            is_valid[i] = true;
            has_valid = true;
        }

        // Second pass: insert all valid entries (if any)
        //
        // NOTE: two passes are necessary because public key parsing and
        // signature verification can be expensive and we want to avoid
        // holding the lock for too long.
        if has_valid {
            let mut stored = self.entries.write();
            for (i, entry) in entries.iter().enumerate() {
                if is_valid[i] {
                    stored.insert(entry);
                }
            }
        }
    }

    /// NOTE: WILL CAUSE DEADLOCK if guard will be held while calling
    /// `add_entries` or similar methods.
    pub(crate) fn read_entries(&self) -> PublicOverlayEntriesReadGuard<'_> {
        PublicOverlayEntriesReadGuard {
            entries: self.entries.read(),
        }
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
    /// Chooses `n` entires from the set, without repetition,
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
