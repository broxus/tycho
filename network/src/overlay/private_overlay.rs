use std::borrow::Borrow;
use std::collections::hash_map;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::seq::SliceRandom;
use rand::Rng;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::{FastHashMap, FastHashSet};

use crate::network::Network;
use crate::overlay::OverlayId;
use crate::proto::overlay::rpc;
use crate::types::{BoxService, PeerId, Request, Response, Service, ServiceExt, ServiceRequest};
use crate::util::NetworkExt;

pub struct PrivateOverlayBuilder {
    overlay_id: OverlayId,
    entries: FastHashSet<PeerId>,
    resolve_peers: bool,
}

impl PrivateOverlayBuilder {
    /// Whether to resolve peer info in the background.
    ///
    /// Default: `false`.
    pub fn resolve_peers(mut self, resolve_peers: bool) -> Self {
        self.resolve_peers = resolve_peers;
        self
    }

    pub fn with_entries<I>(mut self, allowed_peers: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<PeerId>,
    {
        self.entries
            .extend(allowed_peers.into_iter().map(|p| *p.borrow()));
        self
    }

    pub fn build<S>(self, service: S) -> PrivateOverlay
    where
        S: Send + Sync + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        let request_prefix = tl_proto::serialize(rpc::Prefix {
            overlay_id: self.overlay_id.as_bytes(),
        });

        let mut entries = PrivateOverlayEntries {
            peer_id_to_index: Default::default(),
            data: Default::default(),
        };
        for peer_id in self.entries {
            entries.insert(&peer_id);
        }

        PrivateOverlay {
            inner: Arc::new(Inner {
                overlay_id: self.overlay_id,
                entries: RwLock::new(entries),
                service: service.boxed(),
                resolve_peers: self.resolve_peers,
                request_prefix: request_prefix.into_boxed_slice(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct PrivateOverlay {
    inner: Arc<Inner>,
}

impl PrivateOverlay {
    pub fn builder(overlay_id: OverlayId) -> PrivateOverlayBuilder {
        PrivateOverlayBuilder {
            overlay_id,
            entries: Default::default(),
            resolve_peers: false,
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

    pub fn write_entries(&self) -> PrivateOverlayEntriesWriteGuard<'_> {
        PrivateOverlayEntriesWriteGuard {
            entries: self.inner.entries.write(),
        }
    }

    pub fn read_entries(&self) -> PrivateOverlayEntriesReadGuard<'_> {
        PrivateOverlayEntriesReadGuard {
            entries: self.inner.entries.read(),
        }
    }

    pub(crate) fn handle_query(&self, req: ServiceRequest) -> BoxFutureOrNoop<Option<Response>> {
        if self.inner.entries.read().contains(&req.metadata.peer_id) {
            BoxFutureOrNoop::future(self.inner.service.on_query(req))
        } else {
            BoxFutureOrNoop::Noop
        }
    }

    pub(crate) fn handle_message(&self, req: ServiceRequest) -> BoxFutureOrNoop<()> {
        if self.inner.entries.read().contains(&req.metadata.peer_id) {
            BoxFutureOrNoop::future(self.inner.service.on_message(req))
        } else {
            BoxFutureOrNoop::Noop
        }
    }

    pub(crate) fn should_resolve_peers(&self) -> bool {
        self.inner.resolve_peers
    }

    fn prepend_prefix_to_body(&self, body: &mut Bytes) {
        // TODO: reduce allocations
        let mut res = BytesMut::with_capacity(self.inner.request_prefix.len() + body.len());
        res.extend_from_slice(&self.inner.request_prefix);
        res.extend_from_slice(body);
        *body = res.freeze();
    }
}

impl std::fmt::Debug for PrivateOverlay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateOverlay")
            .field("overlay_id", &self.inner.overlay_id)
            .finish()
    }
}

struct Inner {
    overlay_id: OverlayId,
    entries: RwLock<PrivateOverlayEntries>,
    service: BoxService<ServiceRequest, Response>,
    resolve_peers: bool,
    request_prefix: Box<[u8]>,
}

pub struct PrivateOverlayEntries {
    peer_id_to_index: FastHashMap<PeerId, usize>,
    data: Vec<PeerId>,
}

impl PrivateOverlayEntries {
    /// Returns an iterator over the entries.
    pub fn iter(&self) -> std::slice::Iter<'_, PeerId> {
        self.data.iter()
    }

    /// Returns one random peer, or `None` if set is empty.
    pub fn choose<R>(&self, rng: &mut R) -> Option<&PeerId>
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
    ) -> rand::seq::SliceChooseIter<'_, [PeerId], PeerId>
    where
        R: Rng + ?Sized,
    {
        self.data.choose_multiple(rng, n)
    }

    /// Returns true if the set contains the specified peer id.
    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.peer_id_to_index.contains_key(peer_id)
    }

    /// Adds a peer id to the set.
    ///
    /// Returns whether the value was newly inserted.
    pub fn insert(&mut self, peer_id: &PeerId) -> bool {
        match self.peer_id_to_index.entry(*peer_id) {
            // No entry for the peer_id, insert a new one
            hash_map::Entry::Vacant(entry) => {
                entry.insert(self.data.len());
                self.data.push(*peer_id);
                true
            }
            // Entry for the peer_id exists, do nothing
            hash_map::Entry::Occupied(_) => false,
        }
    }

    /// Removes a value from the set.
    ///
    /// Returns whether the value was present in the set.
    pub fn remove(&mut self, peer_id: &PeerId) -> bool {
        let Some(index) = self.peer_id_to_index.remove(peer_id) else {
            return false;
        };

        // Remove the entry from the data vector
        self.data.swap_remove(index);

        // Update the swapped entry's index
        let entry = self
            .peer_id_to_index
            .get_mut(&self.data[index])
            .expect("inconsistent state");
        *entry = index;

        true
    }
}

pub struct PrivateOverlayEntriesWriteGuard<'a> {
    entries: RwLockWriteGuard<'a, PrivateOverlayEntries>,
}

impl std::ops::Deref for PrivateOverlayEntriesWriteGuard<'_> {
    type Target = PrivateOverlayEntries;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl std::ops::DerefMut for PrivateOverlayEntriesWriteGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}

pub struct PrivateOverlayEntriesReadGuard<'a> {
    entries: RwLockReadGuard<'a, PrivateOverlayEntries>,
}

impl std::ops::Deref for PrivateOverlayEntriesReadGuard<'_> {
    type Target = PrivateOverlayEntries;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
