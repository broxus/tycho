use std::borrow::Borrow;
use std::sync::Arc;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::Rng;
use tokio::sync::broadcast;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::{FastHashSet, FastHasherState};
use crate::dht::{PeerResolver, PeerResolverHandle};
use crate::network::Network;
use crate::overlay::OverlayId;
use crate::overlay::metrics::Metrics;
use crate::proto::overlay::rpc;
use crate::types::{
    BoxService, PeerId, Request, Response, Service, ServiceExt, ServiceRequest,
};
use crate::util::NetworkExt;
pub struct PrivateOverlayBuilder {
    overlay_id: OverlayId,
    entries: FastHashSet<PeerId>,
    entry_events_channel_size: usize,
    peer_resolver: Option<PeerResolver>,
    name: Option<&'static str>,
}
impl PrivateOverlayBuilder {
    pub fn with_entries<I>(mut self, allowed_peers: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<PeerId>,
    {
        self.entries.extend(allowed_peers.into_iter().map(|p| *p.borrow()));
        self
    }
    /// The capacity of entries set events.
    ///
    /// Default: 100.
    pub fn with_entry_events_channel_size(
        mut self,
        entry_events_channel_size: usize,
    ) -> Self {
        self.entry_events_channel_size = entry_events_channel_size;
        self
    }
    /// Whether to resolve peers with the provided resolver.
    ///
    /// Does not resolve peers by default.
    pub fn with_peer_resolver(mut self, peer_resolver: PeerResolver) -> Self {
        self.peer_resolver = Some(peer_resolver);
        self
    }
    /// Name of the overlay used in metrics.
    pub fn named(mut self, name: &'static str) -> Self {
        self.name = Some(name);
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
            items: Default::default(),
            events_tx: broadcast::channel(self.entry_events_channel_size).0,
            peer_resolver: self.peer_resolver,
        };
        for peer_id in self.entries {
            entries.insert(&peer_id);
        }
        PrivateOverlay {
            inner: Arc::new(Inner {
                overlay_id: self.overlay_id,
                entries: RwLock::new(entries),
                service: service.boxed(),
                request_prefix: request_prefix.into_boxed_slice(),
                metrics: self
                    .name
                    .map(|label| Metrics::new("tycho_private_overlay", label))
                    .unwrap_or_default(),
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
            entry_events_channel_size: 100,
            peer_resolver: None,
            name: None,
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(query)),
            file!(),
            121u32,
        );
        let network = network;
        let peer_id = peer_id;
        let mut request = request;
        self.inner.metrics.record_rx(request.body.len());
        self.prepend_prefix_to_body(&mut request.body);
        {
            __guard.end_section(124u32);
            let __result = network.query(peer_id, request).await;
            __guard.start_section(124u32);
            __result
        }
    }
    pub async fn send(
        &self,
        network: &Network,
        peer_id: &PeerId,
        mut request: Request,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(send)),
            file!(),
            132u32,
        );
        let network = network;
        let peer_id = peer_id;
        let mut request = request;
        self.inner.metrics.record_rx(request.body.len());
        self.prepend_prefix_to_body(&mut request.body);
        {
            __guard.end_section(135u32);
            let __result = network.send(peer_id, request).await;
            __guard.start_section(135u32);
            __result
        }
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
    pub(crate) fn handle_query(
        &self,
        req: ServiceRequest,
    ) -> BoxFutureOrNoop<Option<Response>> {
        self.inner.metrics.record_rx(req.body.len());
        if self.inner.entries.read().contains(&req.metadata.peer_id) {
            BoxFutureOrNoop::future(self.inner.service.on_query(req))
        } else {
            BoxFutureOrNoop::Noop
        }
    }
    pub(crate) fn handle_message(&self, req: ServiceRequest) -> BoxFutureOrNoop<()> {
        self.inner.metrics.record_rx(req.body.len());
        if self.inner.entries.read().contains(&req.metadata.peer_id) {
            BoxFutureOrNoop::future(self.inner.service.on_message(req))
        } else {
            BoxFutureOrNoop::Noop
        }
    }
    fn prepend_prefix_to_body(&self, body: &mut Bytes) {
        let mut res = BytesMut::with_capacity(
            self.inner.request_prefix.len() + body.len(),
        );
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
    request_prefix: Box<[u8]>,
    metrics: Metrics,
}
pub struct PrivateOverlayEntries {
    items: OverlayItems,
    events_tx: broadcast::Sender<PrivateOverlayEntriesEvent>,
    peer_resolver: Option<PeerResolver>,
}
impl PrivateOverlayEntries {
    /// Subscribes to the set updates.
    pub fn subscribe(&self) -> broadcast::Receiver<PrivateOverlayEntriesEvent> {
        self.events_tx.subscribe()
    }
    /// Returns an iterator over the entry ids.
    ///
    /// The order is not random, but is not defined.
    pub fn iter(&self) -> indexmap::map::Values<'_, PeerId, PrivateOverlayEntryData> {
        self.items.values()
    }
    /// Returns one random peer, or `None` if set is empty.
    pub fn choose<R>(&self, rng: &mut R) -> Option<&PrivateOverlayEntryData>
    where
        R: Rng + ?Sized,
    {
        let index = rng.random_range(0..self.items.len());
        let (_, value) = self.items.get_index(index)?;
        Some(value)
    }
    /// Chooses `n` entries from the set, without repetition,
    /// and in random order.
    pub fn choose_multiple<R>(
        &self,
        rng: &mut R,
        n: usize,
    ) -> ChooseMultiplePrivateOverlayEntries<'_>
    where
        R: Rng + ?Sized,
    {
        let len = self.items.len();
        ChooseMultiplePrivateOverlayEntries {
            items: &self.items,
            indices: rand::seq::index::sample(rng, len, n.min(len)).into_iter(),
        }
    }
    /// Clears the set, removing all entries.
    pub fn clear(&mut self) {
        self.items.clear();
    }
    /// Returns `true` if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    /// Returns the number of elements in the set, also referred to as its 'length'.
    pub fn len(&self) -> usize {
        self.items.len()
    }
    /// Returns true if the set contains the specified peer id.
    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.items.contains_key(peer_id)
    }
    /// Returns the peer resolver handle for the specified peer id, if it exists.
    pub fn get_handle(&self, peer_id: &PeerId) -> Option<&PeerResolverHandle> {
        self.items.get(peer_id).map(|item| &item.resolver_handle)
    }
    /// Adds a peer id to the set.
    ///
    /// Returns whether the value was newly inserted.
    pub fn insert(&mut self, peer_id: &PeerId) -> bool {
        match self.items.entry(*peer_id) {
            indexmap::map::Entry::Vacant(entry) => {
                let handle = self
                    .peer_resolver
                    .as_ref()
                    .map_or_else(
                        || PeerResolverHandle::new_noop(peer_id),
                        |resolver| resolver.insert(peer_id, true),
                    );
                entry
                    .insert(PrivateOverlayEntryData {
                        peer_id: *peer_id,
                        resolver_handle: handle,
                    });
                _ = self.events_tx.send(PrivateOverlayEntriesEvent::Added(*peer_id));
                true
            }
            indexmap::map::Entry::Occupied(_) => false,
        }
    }
    /// Removes a value from the set.
    ///
    /// Returns whether the value was present in the set.
    pub fn remove(&mut self, peer_id: &PeerId) -> bool {
        let removed = self.items.swap_remove(peer_id).is_some();
        if removed {
            _ = self.events_tx.send(PrivateOverlayEntriesEvent::Removed(*peer_id));
        }
        removed
    }
}
#[derive(Clone)]
pub struct PrivateOverlayEntryData {
    pub peer_id: PeerId,
    pub resolver_handle: PeerResolverHandle,
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
impl<'a> PrivateOverlayEntriesWriteGuard<'a> {
    pub fn downgrade(self) -> PrivateOverlayEntriesReadGuard<'a> {
        let entries = RwLockWriteGuard::downgrade(self.entries);
        PrivateOverlayEntriesReadGuard {
            entries,
        }
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateOverlayEntriesEvent {
    /// A new entry was inserted.
    Added(PeerId),
    /// An existing entry was removed.
    Removed(PeerId),
}
pub struct ChooseMultiplePrivateOverlayEntries<'a> {
    items: &'a OverlayItems,
    indices: rand::seq::index::IndexVecIntoIter,
}
impl<'a> Iterator for ChooseMultiplePrivateOverlayEntries<'a> {
    type Item = &'a PrivateOverlayEntryData;
    fn next(&mut self) -> Option<Self::Item> {
        self.indices
            .next()
            .and_then(|i| {
                let (_, value) = self.items.get_index(i)?;
                Some(value)
            })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.indices.len(), Some(self.indices.len()))
    }
}
impl ExactSizeIterator for ChooseMultiplePrivateOverlayEntries<'_> {
    fn len(&self) -> usize {
        self.indices.len()
    }
}
type OverlayItems = IndexMap<PeerId, PrivateOverlayEntryData, FastHasherState>;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn entries_container_is_set() {
        let mut entries = PrivateOverlayEntries {
            items: Default::default(),
            peer_resolver: None,
            events_tx: broadcast::channel(100).0,
        };
        assert!(entries.is_empty());
        assert_eq!(entries.len(), 0);
        let peer_id = rand::random();
        assert!(entries.insert(& peer_id));
        assert!(! entries.is_empty());
        assert_eq!(entries.len(), 1);
        assert!(! entries.insert(& peer_id));
        assert_eq!(entries.len(), 1);
        entries.clear();
        assert!(entries.is_empty());
        assert_eq!(entries.len(), 0);
    }
    #[test]
    fn remove_from_entries_container() {
        let (events_tx, mut events_rx) = broadcast::channel(100);
        let mut entries = PrivateOverlayEntries {
            items: Default::default(),
            peer_resolver: None,
            events_tx,
        };
        let peer_ids = std::array::from_fn::<PeerId, 10, _>(|_| rand::random());
        for (i, peer_id) in peer_ids.iter().enumerate() {
            assert!(entries.insert(peer_id));
            assert_eq!(entries.len(), i + 1);
            assert_eq!(
                events_rx.try_recv().unwrap(), PrivateOverlayEntriesEvent::Added(*
                peer_id)
            );
        }
        for peer_id in &peer_ids {
            assert!(entries.remove(peer_id));
            assert_eq!(
                events_rx.try_recv().unwrap(), PrivateOverlayEntriesEvent::Removed(*
                peer_id)
            );
            assert!(! entries.items.contains_key(peer_id));
        }
        assert!(entries.is_empty());
        assert!(! entries.remove(& rand::random()));
        assert!(events_rx.try_recv().is_err());
    }
}
