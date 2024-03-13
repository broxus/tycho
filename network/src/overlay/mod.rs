use std::collections::hash_map;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use anyhow::Result;
use bytes::Buf;
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt};
use tl_proto::{TlError, TlRead};
use tokio::sync::{Notify, Semaphore};
use tokio::task::{AbortHandle, JoinSet};
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::time::{now_sec, shifted_interval};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use crate::dht::{DhtClient, DhtService};
use crate::network::{Network, WeakNetwork};
use crate::proto::overlay::{rpc, PublicEntriesResponse, PublicEntry, PublicEntryToSign};
use crate::types::{PeerId, PeerInfo, Request, Response, Service, ServiceRequest};
use crate::util::{NetworkExt, Routable};

pub use self::config::OverlayConfig;
pub use self::overlay_id::OverlayId;
pub use self::private_overlay::{
    PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesIter, PrivateOverlayEntriesReadGuard, PrivateOverlayEntriesWriteGuard,
};
pub use self::public_overlay::{
    PublicOverlay, PublicOverlayBuilder, PublicOverlayEntries, PublicOverlayEntriesReadGuard,
};

mod config;
mod overlay_id;
mod peer_resolver;
mod private_overlay;
mod public_overlay;

pub struct OverlayServiceBackgroundTasks {
    inner: Arc<OverlayServiceInner>,
    dht: Option<DhtService>,
}

impl OverlayServiceBackgroundTasks {
    pub fn spawn(self, network: Network) {
        self.inner
            .start_background_tasks(Network::downgrade(&network), self.dht);
    }
}

pub struct OverlayServiceBuilder {
    local_id: PeerId,
    config: Option<OverlayConfig>,
    dht: Option<DhtService>,
    private_overlays: FastDashMap<OverlayId, PrivateOverlay>,
    public_overlays: FastDashMap<OverlayId, PublicOverlay>,
}

impl OverlayServiceBuilder {
    pub fn with_config(mut self, config: OverlayConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_dht_service(mut self, dht: DhtService) -> Self {
        self.dht = Some(dht);
        self
    }

    pub fn with_private_overlay(self, overlay: &PrivateOverlay) -> Self {
        assert!(
            !self.public_overlays.contains_key(overlay.overlay_id()),
            "public overlay with id {} already exists",
            overlay.overlay_id()
        );

        let prev = self
            .private_overlays
            .insert(*overlay.overlay_id(), overlay.clone());
        if let Some(prev) = prev {
            panic!(
                "private overlay with id {} already exists",
                prev.overlay_id()
            );
        }
        self
    }

    pub fn with_public_overlay(self, overlay: &PublicOverlay) -> Self {
        assert!(
            !self.private_overlays.contains_key(overlay.overlay_id()),
            "private overlay with id {} already exists",
            overlay.overlay_id()
        );

        let prev = self
            .public_overlays
            .insert(*overlay.overlay_id(), overlay.clone());
        if let Some(prev) = prev {
            panic!(
                "public overlay with id {} already exists",
                prev.overlay_id()
            );
        }
        self
    }

    pub fn build(self) -> (OverlayServiceBackgroundTasks, OverlayService) {
        let config = self.config.unwrap_or_default();

        let inner = Arc::new(OverlayServiceInner {
            local_id: self.local_id,
            config,
            private_overlays: self.private_overlays,
            public_overlays: self.public_overlays,
            public_overlays_changed: Arc::new(Notify::new()),
            private_overlays_changed: Arc::new(Notify::new()),
        });

        let background_tasks = OverlayServiceBackgroundTasks {
            inner: inner.clone(),
            dht: self.dht,
        };

        (background_tasks, OverlayService(inner))
    }
}

#[derive(Clone)]
pub struct OverlayService(Arc<OverlayServiceInner>);

impl OverlayService {
    pub fn builder(local_id: PeerId) -> OverlayServiceBuilder {
        OverlayServiceBuilder {
            local_id,
            config: None,
            dht: None,
            private_overlays: Default::default(),
            public_overlays: Default::default(),
        }
    }

    pub fn try_add_private_overlay(&self, overlay: &PrivateOverlay) -> bool {
        self.0.try_add_private_overlay(overlay)
    }

    pub fn try_add_public_overlay(&self, overlay: &PublicOverlay) -> bool {
        self.0.try_add_public_overlay(overlay)
    }
}

impl Service<ServiceRequest> for OverlayService {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_overlay_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, mut req: ServiceRequest) -> Self::OnQueryFuture {
        let e = 'req: {
            if req.body.len() < 4 {
                break 'req TlError::UnexpectedEof;
            }

            // NOTE: `req.body` is untouched while reading the constructor
            // and `as_ref` here is exactly for that.
            let mut offset = 0;
            let overlay_id = match req.body.as_ref().get_u32_le() {
                rpc::Prefix::TL_ID => match rpc::Prefix::read_from(&req.body, &mut offset) {
                    Ok(rpc::Prefix { overlay_id }) => overlay_id,
                    Err(e) => break 'req e,
                },
                rpc::ExchangeRandomPublicEntries::TL_ID => {
                    let req = match tl_proto::deserialize::<rpc::ExchangeRandomPublicEntries>(
                        &req.body,
                    ) {
                        Ok(req) => req,
                        Err(e) => break 'req e,
                    };
                    tracing::debug!("exchange_random_public_entries");

                    let res = self.0.handle_exchange_public_entries(&req);
                    return BoxFutureOrNoop::future(futures_util::future::ready(Some(
                        Response::from_tl(res),
                    )));
                }
                _ => break 'req TlError::UnknownConstructor,
            };

            if req.body.len() < offset + 4 {
                // Definitely an invalid request (not enough bytes for the constructor)
                break 'req TlError::UnexpectedEof;
            }

            if let Some(private_overlay) = self.0.private_overlays.get(overlay_id) {
                req.body.advance(offset);
                return private_overlay.handle_query(req);
            } else if let Some(public_overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return public_overlay.handle_query(req);
            }

            tracing::debug!(
                overlay_id = %OverlayId::wrap(overlay_id),
                "unknown overlay id"
            );
            return BoxFutureOrNoop::Noop;
        };

        tracing::debug!("failed to deserialize query: {e:?}");
        BoxFutureOrNoop::Noop
    }

    #[tracing::instrument(
        level = "debug",
        name = "on_overlay_message",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_message(&self, mut req: ServiceRequest) -> Self::OnMessageFuture {
        // TODO: somehow refactor with one method for both query and message

        let e = 'req: {
            if req.body.len() < 4 {
                break 'req TlError::UnexpectedEof;
            }

            // NOTE: `req.body` is untouched while reading the constructor
            // and `as_ref` here is exactly for that.
            let mut offset = 0;
            let overlay_id = match req.body.as_ref().get_u32_le() {
                rpc::Prefix::TL_ID => match rpc::Prefix::read_from(&req.body, &mut offset) {
                    Ok(rpc::Prefix { overlay_id }) => overlay_id,
                    Err(e) => break 'req e,
                },
                _ => break 'req TlError::UnknownConstructor,
            };

            if req.body.len() < offset + 4 {
                // Definitely an invalid request (not enough bytes for the constructor)
                break 'req TlError::UnexpectedEof;
            }

            if let Some(private_overlay) = self.0.private_overlays.get(overlay_id) {
                req.body.advance(offset);
                return private_overlay.handle_message(req);
            } else if let Some(public_overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return public_overlay.handle_message(req);
            }

            tracing::debug!(
                overlay_id = %OverlayId::wrap(overlay_id),
                "unknown overlay id"
            );
            return BoxFutureOrNoop::Noop;
        };

        tracing::debug!("failed to deserialize message: {e:?}");
        BoxFutureOrNoop::Noop
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

impl Routable for OverlayService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::ExchangeRandomPublicEntries::TL_ID, rpc::Prefix::TL_ID]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::Prefix::TL_ID]
    }
}

struct OverlayServiceInner {
    local_id: PeerId,
    config: OverlayConfig,
    public_overlays: FastDashMap<OverlayId, PublicOverlay>,
    private_overlays: FastDashMap<OverlayId, PrivateOverlay>,
    public_overlays_changed: Arc<Notify>,
    private_overlays_changed: Arc<Notify>,
}

impl OverlayServiceInner {
    fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork, dht: Option<DhtService>) {
        enum Action<'a> {
            UpdatePublicOverlaysList(&'a mut PublicOverlaysState),
            UpdatePrivateOverlaysList(&'a mut PrivateOverlaysState),
            ExchangePublicOverlayEntries {
                overlay_id: OverlayId,
                exchange: &'a mut OverlayTaskSet,
            },
            ResolvePublicOverlayPeers {
                overlay_id: OverlayId,
                resolve: &'a mut OverlayTaskSet,
            },
            ResolvePrivateOverlayPeers {
                overlay_id: OverlayId,
                resolve: &'a mut OverlayTaskSet,
            },
        }

        struct PublicOverlaysState {
            exchange: OverlayTaskSet,
            resolve: OverlayTaskSet,
        }

        struct PrivateOverlaysState {
            resolve: OverlayTaskSet,
        }

        let public_overlays_notify = self.public_overlays_changed.clone();
        let private_overlays_notify = self.private_overlays_changed.clone();

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background overlay loop started");

            let mut public_overlays_changed = Box::pin(public_overlays_notify.notified());
            let mut private_overlays_changed = Box::pin(private_overlays_notify.notified());

            let mut public_overlays_state = None::<PublicOverlaysState>;
            let mut private_overlays_state = None::<PrivateOverlaysState>;

            fn make_dht_client(dht: &Option<DhtService>, network: &Network) -> Option<DhtClient> {
                match dht {
                    Some(dht) => Some(dht.make_client(network.clone())),
                    None => {
                        tracing::warn!(
                            "DHT service was not provided, \
                            skipping private overlay peers resolution"
                        );
                        None
                    }
                }
            }

            loop {
                let action = match (&mut public_overlays_state, &mut private_overlays_state) {
                    // Initial update for public overlays list
                    (None, _) => Action::UpdatePublicOverlaysList(public_overlays_state.insert(
                        PublicOverlaysState {
                            exchange: OverlayTaskSet::new("exchange public overlay peers"),
                            resolve: OverlayTaskSet::new("resolve public overlay peers"),
                        },
                    )),
                    (_, None) => Action::UpdatePrivateOverlaysList(private_overlays_state.insert(
                        PrivateOverlaysState {
                            resolve: OverlayTaskSet::new("resolve private overlay peers"),
                        },
                    )),
                    // Default actions
                    (Some(public_overlays_state), Some(private_overlays_state)) => {
                        tokio::select! {
                            _ = &mut public_overlays_changed => {
                                public_overlays_changed = Box::pin(public_overlays_notify.notified());
                                Action::UpdatePublicOverlaysList(public_overlays_state)
                            },
                            _ = &mut private_overlays_changed => {
                                private_overlays_changed = Box::pin(private_overlays_notify.notified());
                                Action::UpdatePrivateOverlaysList(private_overlays_state)
                            },
                            overlay_id = public_overlays_state.exchange.next() => match overlay_id {
                                Some(id) => Action::ExchangePublicOverlayEntries {
                                    overlay_id: id,
                                    exchange: &mut public_overlays_state.exchange,
                                },
                                None => continue,
                            },
                            overlay_id = public_overlays_state.resolve.next() => match overlay_id {
                                Some(id) => Action::ResolvePublicOverlayPeers {
                                    overlay_id: id,
                                    resolve: &mut public_overlays_state.resolve,
                                },
                                None => continue,
                            },
                            overlay_id = private_overlays_state.resolve.next() => match overlay_id {
                                Some(id) => Action::ResolvePrivateOverlayPeers {
                                    overlay_id: id,
                                    resolve: &mut private_overlays_state.resolve,
                                },
                                None => continue,
                            },
                        }
                    }
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::UpdatePublicOverlaysList(PublicOverlaysState { exchange, resolve }) => {
                        let iter = this.public_overlays.iter().map(|item| *item.key());
                        exchange.rebuild(iter.clone(), |_| {
                            shifted_interval(
                                this.config.public_overlay_peer_exchange_period,
                                this.config.public_overlay_peer_exchange_max_jitter,
                            )
                        });
                        resolve.rebuild(iter, |_| {
                            shifted_interval(
                                this.config.public_overlay_peer_resolve_period,
                                this.config.public_overlay_peer_resolve_max_jitter,
                            )
                        });
                    }
                    Action::UpdatePrivateOverlaysList(PrivateOverlaysState { resolve }) => {
                        let iter = this.private_overlays.iter().filter_map(|item| {
                            item.value().should_resolve_peers().then(|| *item.key())
                        });
                        resolve.rebuild(iter, |_| {
                            shifted_interval(
                                this.config.private_overlay_peer_resolve_period,
                                this.config.private_overlay_peer_resolve_max_jitter,
                            )
                        });
                    }
                    Action::ExchangePublicOverlayEntries {
                        exchange: exchange_state,
                        overlay_id,
                    } => {
                        exchange_state.spawn(&overlay_id, move || async move {
                            this.exchange_public_entries(&network, &overlay_id).await
                        });
                    }
                    Action::ResolvePublicOverlayPeers {
                        resolve,
                        overlay_id,
                    } => {
                        let Some(dht_client) = make_dht_client(&dht, &network) else {
                            continue;
                        };
                        resolve.spawn(&overlay_id, move || async move {
                            this.resolve_public_overlay_peers(&network, &dht_client, &overlay_id)
                                .await
                        });
                    }
                    Action::ResolvePrivateOverlayPeers {
                        resolve,
                        overlay_id,
                    } => {
                        let Some(dht_client) = make_dht_client(&dht, &network) else {
                            continue;
                        };
                        resolve.spawn(&overlay_id, move || async move {
                            this.resolve_private_overlay_peers(&network, &dht_client, &overlay_id)
                                .await
                        });
                    }
                }
            }

            tracing::debug!("background overlay loop stopped");
        });
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn resolve_public_overlay_peers(
        &self,
        network: &Network,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        todo!()
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn resolve_private_overlay_peers(
        &self,
        network: &Network,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        use crate::proto::dht;

        const EXPIRATION_OFFSET: u32 = 120;

        let overlay = if let Some(overlay) = self.private_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::debug!(%overlay_id, "overlay not found");
            return Ok(());
        };

        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_resolver_requests));
        let mut futures = FuturesUnordered::new();

        {
            let entries = overlay.read_entries();

            let now = now_sec();
            for (peer_id, handle) in entries.iter_with_resolved() {
                if let Some(handle) = handle {
                    if !handle.peer_info().is_expired(now + EXPIRATION_OFFSET) {
                        // Skip non-expired resolved items
                        continue;
                    }
                }

                let peer_id = *peer_id;
                let semaphore = semaphore.clone();
                futures.push(async move {
                    let _permit = semaphore.acquire().await.unwrap();

                    let res = dht_client
                        .entry(dht::PeerValueKeyName::NodeInfo)
                        .find_value::<PeerInfo>(&peer_id)
                        .await;
                    (peer_id, res)
                });
            }
        };

        while let Some((peer_id, res)) = futures.next().await {
            let now = now_sec();

            let info = match res {
                Ok(info) if info.is_valid(now) && info.id == peer_id => info,
                Ok(_) => {
                    tracing::debug!(%peer_id, "received an invalid peer info");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(%peer_id, "failed to resolve a peer info: {e:?}");
                    continue;
                }
            };

            match network.known_peers().insert(Arc::new(info), true) {
                Ok(handle) => {
                    overlay.write_entries().set_resolved(handle);
                }
                Err(e) => {
                    tracing::debug!(%peer_id, "failed to insert a new peer info: {e:?}");
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn exchange_public_entries(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::debug!(%overlay_id, "overlay not found");
            return Ok(());
        };

        overlay.remove_invalid_entries(now_sec());

        let n = std::cmp::max(self.config.exchange_public_entries_batch, 1);
        let mut entries = Vec::with_capacity(n);

        // Always include us in the response
        entries.push(Arc::new(self.make_local_public_overlay_entry(
            network,
            overlay_id,
            now_sec(),
        )));

        // Choose a random target to send the request and additional random entries
        let peer_id = {
            let rng = &mut rand::thread_rng();

            let all_entries = overlay.read_entries();
            let mut iter = all_entries.choose_multiple(rng, n);

            // TODO: search for target in known peers. This is a stub which will not work.
            let peer_id = match iter.next() {
                Some(entry) => entry.peer_id,
                None => anyhow::bail!("empty overlay, no peers to exchange entries with"),
            };

            // Add additional random entries to the response
            entries.extend(iter.cloned());

            // Use this peer id for the request
            peer_id
        };

        // Send request
        let response = network
            .query(
                &peer_id,
                Request::from_tl(rpc::ExchangeRandomPublicEntries {
                    overlay_id: overlay_id.to_bytes(),
                    entries,
                }),
            )
            .await?
            .parse_tl::<PublicEntriesResponse>()?;

        // Populate the overlay with the response
        match response {
            PublicEntriesResponse::PublicEntries(entries) => {
                tracing::debug!(
                    %peer_id,
                    count = entries.len(),
                    "received public entries"
                );
                overlay.add_untrusted_entries(&entries, now_sec());
            }
            PublicEntriesResponse::OverlayNotFound => {
                tracing::debug!(%peer_id, "overlay not found");
            }
        }

        // Done
        Ok(())
    }

    fn make_local_public_overlay_entry(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
        now: u32,
    ) -> PublicEntry {
        let signature = Box::new(network.sign_tl(PublicEntryToSign {
            overlay_id: overlay_id.as_bytes(),
            peer_id: &self.local_id,
            created_at: now,
        }));
        PublicEntry {
            peer_id: self.local_id,
            created_at: now,
            signature,
        }
    }

    pub fn try_add_private_overlay(&self, overlay: &PrivateOverlay) -> bool {
        use dashmap::mapref::entry::Entry;

        if self.public_overlays.contains_key(overlay.overlay_id()) {
            return false;
        }
        match self.private_overlays.entry(*overlay.overlay_id()) {
            Entry::Vacant(entry) => {
                entry.insert(overlay.clone());
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn try_add_public_overlay(&self, overlay: &PublicOverlay) -> bool {
        use dashmap::mapref::entry::Entry;

        if self.private_overlays.contains_key(overlay.overlay_id()) {
            return false;
        }
        match self.public_overlays.entry(*overlay.overlay_id()) {
            Entry::Vacant(entry) => {
                entry.insert(overlay.clone());
                self.public_overlays_changed.notify_waiters();
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn handle_exchange_public_entries(
        &self,
        req: &rpc::ExchangeRandomPublicEntries,
    ) -> PublicEntriesResponse {
        // NOTE: validation is done in the TL parser.
        debug_assert!(req.entries.len() <= 20);

        // Find the overlay
        let overlay = match self.public_overlays.get(&req.overlay_id) {
            Some(overlay) => overlay,
            None => return PublicEntriesResponse::OverlayNotFound,
        };

        // Add proposed entries to the overlay
        overlay.add_untrusted_entries(&req.entries, now_sec());

        // Collect proposed entries to exclude from the response
        let requested_ids = req
            .entries
            .iter()
            .map(|id| id.peer_id)
            .collect::<FastHashSet<_>>();

        let entries = {
            let entries = overlay.read_entries();

            // Choose additional random entries to ensure we have enough new entries to send back
            let n = self.config.exchange_public_entries_batch;
            entries
                .choose_multiple(&mut rand::thread_rng(), n + requested_ids.len())
                .filter_map(|entry| {
                    let is_new = !requested_ids.contains(&entry.peer_id);
                    is_new.then(|| entry.clone())
                })
                .take(n)
                .collect::<Vec<_>>()
        };

        PublicEntriesResponse::PublicEntries(entries)
    }
}

struct OverlayTaskSet {
    name: &'static str,
    stream: OverlayActionsStream,
    handles: FastHashMap<OverlayId, (AbortHandle, bool)>,
    join_set: JoinSet<OverlayId>,
}

impl OverlayTaskSet {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            stream: Default::default(),
            handles: Default::default(),
            join_set: Default::default(),
        }
    }

    async fn next(&mut self) -> Option<OverlayId> {
        use futures_util::future::{select, Either};

        loop {
            // Wait until the next interval or completed task
            let res = {
                let next = std::pin::pin!(self.stream.next());
                let joined = std::pin::pin!(self.join_set.join_next());
                match select(next, joined).await {
                    // Handle interval events first
                    Either::Left((id, _)) => return id,
                    // Handled task completion otherwise
                    Either::Right((joined, fut)) => match joined {
                        Some(res) => res,
                        None => return fut.await,
                    },
                }
            };

            // If some task was joined
            match res {
                // Task was completed successfully
                Ok(overlay_id) => {
                    return if matches!(self.handles.remove(&overlay_id), Some((_, true))) {
                        // Reset interval and execute task immediately
                        self.stream.reset_interval(&overlay_id);
                        Some(overlay_id)
                    } else {
                        None
                    };
                }
                // Propagate task panic
                Err(e) if e.is_panic() => {
                    tracing::error!(task = self.name, "task panicked");
                    std::panic::resume_unwind(e.into_panic());
                }
                // Task cancelled, loop once more with the next task
                Err(_) => continue,
            }
        }
    }

    fn rebuild<I, F>(&mut self, iter: I, f: F)
    where
        I: Iterator<Item = OverlayId>,
        for<'a> F: FnMut(&'a OverlayId) -> tokio::time::Interval,
    {
        self.stream.rebuild(iter, f, |overlay_id| {
            if let Some((handle, _)) = self.handles.remove(overlay_id) {
                tracing::debug!(task = self.name, %overlay_id, "task cancelled");
                handle.abort();
            }
        });
    }

    fn spawn<F, Fut>(&mut self, overlay_id: &OverlayId, f: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        match self.handles.entry(*overlay_id) {
            hash_map::Entry::Vacant(entry) => {
                let fut = {
                    let fut = f();
                    let task = self.name;
                    let overlay_id = *overlay_id;
                    async move {
                        if let Err(e) = fut.await {
                            tracing::error!(task, %overlay_id, "task failed: {e:?}");
                        }
                        overlay_id
                    }
                };
                entry.insert((self.join_set.spawn(fut), false));
            }
            hash_map::Entry::Occupied(mut entry) => {
                tracing::warn!(
                    task = self.name,
                    %overlay_id,
                    "task is running longer than expected",
                );
                entry.get_mut().1 = true;
            }
        }
    }
}

#[derive(Default)]
struct OverlayActionsStream {
    intervals: Vec<(tokio::time::Interval, OverlayId)>,
    waker: Option<Waker>,
}

impl OverlayActionsStream {
    fn reset_interval(&mut self, overlay_id: &OverlayId) {
        if let Some((interval, _)) = self.intervals.iter_mut().find(|(_, id)| id == overlay_id) {
            interval.reset();
        }
    }

    fn rebuild<I: Iterator<Item = OverlayId>, A, R>(
        &mut self,
        iter: I,
        mut on_add: A,
        mut on_remove: R,
    ) where
        for<'a> A: FnMut(&'a OverlayId) -> tokio::time::Interval,
        for<'a> R: FnMut(&'a OverlayId),
    {
        let mut new_overlays = iter.collect::<FastHashSet<_>>();
        self.intervals.retain(|(_, id)| {
            let retain = new_overlays.remove(id);
            if !retain {
                on_remove(id);
            }
            retain
        });

        for id in new_overlays {
            self.intervals.push((on_add(&id), id));
        }

        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl Stream for OverlayActionsStream {
    type Item = OverlayId;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Always register the waker to resume the stream even if there were
        // changes in the intervals.
        if !matches!(&self.waker, Some(waker) if cx.waker().will_wake(waker)) {
            self.waker = Some(cx.waker().clone());
        }

        for (interval, data) in self.intervals.iter_mut() {
            if interval.poll_tick(cx).is_ready() {
                return Poll::Ready(Some(*data));
            }
        }

        Poll::Pending
    }
}
