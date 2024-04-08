use std::sync::Arc;

use anyhow::Result;
use bytes::Buf;
use rand::Rng;
use tl_proto::{TlError, TlRead};
use tokio::sync::Notify;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::time::{now_sec, shifted_interval};
use tycho_util::{FastDashMap, FastHashSet};

use self::entries_merger::PublicOverlayEntriesMerger;
use self::tasks_stream::TasksStream;
use crate::dht::{DhtClient, DhtService};
use crate::network::{KnownPeerHandle, Network, WeakNetwork};
use crate::proto::overlay::{rpc, PublicEntriesResponse, PublicEntry, PublicEntryToSign};
use crate::types::{PeerId, Request, Response, Service, ServiceRequest};
use crate::util::{NetworkExt, Routable};

pub use self::config::OverlayConfig;
pub use self::overlay_id::OverlayId;
pub use self::private_overlay::{
    PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard, PrivateOverlayEntriesWriteGuard,
};
pub use self::public_overlay::{
    PublicOverlay, PublicOverlayBuilder, PublicOverlayEntries, PublicOverlayEntriesReadGuard,
};

mod config;
mod entries_merger;
mod overlay_id;
mod private_overlay;
mod public_overlay;
mod tasks_stream;

pub struct OverlayServiceBackgroundTasks {
    inner: Arc<OverlayServiceInner>,
    dht: Option<DhtService>,
}

impl OverlayServiceBackgroundTasks {
    pub fn spawn(self, network: &Network) {
        self.inner
            .start_background_tasks(Network::downgrade(network), self.dht);
    }
}

pub struct OverlayServiceBuilder {
    local_id: PeerId,
    config: Option<OverlayConfig>,
    dht: Option<DhtService>,
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

    pub fn build(self) -> (OverlayServiceBackgroundTasks, OverlayService) {
        let config = self.config.unwrap_or_default();

        let inner = Arc::new(OverlayServiceInner {
            local_id: self.local_id,
            config,
            private_overlays: Default::default(),
            public_overlays: Default::default(),
            public_overlays_changed: Arc::new(Notify::new()),
            private_overlays_changed: Arc::new(Notify::new()),
            public_entries_merger: Arc::new(PublicOverlayEntriesMerger),
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
        }
    }

    pub fn add_private_overlay(&self, overlay: &PrivateOverlay) -> bool {
        self.0.add_private_overlay(overlay)
    }

    pub fn remove_private_overlay(&self, overlay_id: &OverlayId) -> bool {
        self.0.remove_private_overlay(overlay_id)
    }

    pub fn add_public_overlay(&self, overlay: &PublicOverlay) -> bool {
        self.0.add_public_overlay(overlay)
    }

    pub fn remove_public_overlay(&self, overlay_id: &OverlayId) -> bool {
        self.0.remove_public_overlay(overlay_id)
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
    public_entries_merger: Arc<PublicOverlayEntriesMerger>,
}

impl OverlayServiceInner {
    fn start_background_tasks(
        self: &Arc<Self>,
        network: WeakNetwork,
        dht_service: Option<DhtService>,
    ) {
        // TODO: Store public overlay entries in the DHT.

        enum Action<'a> {
            UpdatePublicOverlaysList(&'a mut PublicOverlaysState),
            ExchangePublicOverlayEntries {
                overlay_id: OverlayId,
                tasks: &'a mut TasksStream,
            },
            StorePublicEntries {
                overlay_id: OverlayId,
                tasks: &'a mut TasksStream,
            },
        }

        struct PublicOverlaysState {
            exchange: TasksStream,
            store: TasksStream,
        }

        let public_overlays_notify = self.public_overlays_changed.clone();

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background overlay loop started");

            let mut public_overlays_changed = Box::pin(public_overlays_notify.notified());
            let mut public_overlays_state = None::<PublicOverlaysState>;

            loop {
                let action = match &mut public_overlays_state {
                    // Initial update for public overlays list
                    None => Action::UpdatePublicOverlaysList(public_overlays_state.insert(
                        PublicOverlaysState {
                            exchange: TasksStream::new("exchange public overlay peers"),
                            store: TasksStream::new("store public overlay entries in DHT"),
                        },
                    )),
                    // Default actions
                    Some(public_overlays_state) => {
                        tokio::select! {
                            _ = &mut public_overlays_changed => {
                                public_overlays_changed = Box::pin(public_overlays_notify.notified());
                                Action::UpdatePublicOverlaysList(public_overlays_state)
                            },
                            overlay_id = public_overlays_state.exchange.next() => match overlay_id {
                                Some(id) => Action::ExchangePublicOverlayEntries {
                                    overlay_id: id,
                                    tasks: &mut public_overlays_state.exchange,
                                },
                                None => continue,
                            },
                            overlay_id = public_overlays_state.store.next() => match overlay_id {
                                Some(id) => Action::StorePublicEntries {
                                    overlay_id: id,
                                    tasks: &mut public_overlays_state.store,
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
                    Action::UpdatePublicOverlaysList(PublicOverlaysState { exchange, store }) => {
                        let iter = this.public_overlays.iter().map(|item| *item.key());
                        exchange.rebuild(iter.clone(), |_| {
                            shifted_interval(
                                this.config.public_overlay_peer_exchange_period,
                                this.config.public_overlay_peer_exchange_max_jitter,
                            )
                        });
                        store.rebuild_ext(
                            iter,
                            |overlay_id| {
                                // Insert merger for new overlays
                                if let Some(dht) = &dht_service {
                                    dht.insert_merger(
                                        overlay_id.as_bytes(),
                                        this.public_entries_merger.clone(),
                                    );
                                }

                                shifted_interval(
                                    this.config.public_overlay_peer_store_period,
                                    this.config.public_overlay_peer_store_max_jitter,
                                )
                            },
                            |overlay_id| {
                                // Remove merger for removed overlays
                                if let Some(dht) = &dht_service {
                                    dht.remove_merger(overlay_id.as_bytes());
                                }
                            },
                        );
                    }
                    Action::ExchangePublicOverlayEntries { overlay_id, tasks } => {
                        tasks.spawn(&overlay_id, move || async move {
                            this.exchange_public_entries(&network, &overlay_id).await
                        });
                    }
                    Action::StorePublicEntries { overlay_id, tasks } => {
                        let Some(dht_service) = dht_service.clone() else {
                            continue;
                        };

                        tasks.spawn(&overlay_id, move || async move {
                            this.store_public_entries(
                                &dht_service.make_client(&network),
                                &overlay_id,
                            )
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
    async fn exchange_public_entries(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::debug!("overlay not found");
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
        let target_peer_handle;
        let target_peer_id;
        {
            let rng = &mut rand::thread_rng();

            let all_entries = overlay.read_entries();

            match choose_random_resolved_peer(&all_entries, rng) {
                Some(handle) => {
                    target_peer_handle = handle;
                    target_peer_id = target_peer_handle.load_peer_info().id;
                }
                None => {
                    tracing::warn!("no resolved peers in the overlay to exchange entries with");
                    return Ok(());
                }
            }

            // Add additional random entries to the response.
            // NOTE: `n` instead of `n - 1` because we might ignore the target peer
            entries.extend(
                all_entries
                    .choose_multiple(rng, n)
                    .filter(|&item| (item.entry.peer_id != target_peer_id))
                    .map(|item| item.entry.clone())
                    .take(n - 1),
            );
        };

        // Send request
        let response = network
            .query(
                &target_peer_id,
                Request::from_tl(rpc::ExchangeRandomPublicEntries {
                    overlay_id: overlay_id.to_bytes(),
                    entries,
                }),
            )
            .await?
            .parse_tl::<PublicEntriesResponse>()?;

        // NOTE: Ensure that resolved peer handle is alive for enough time
        drop(target_peer_handle);

        // Populate the overlay with the response
        match response {
            PublicEntriesResponse::PublicEntries(entries) => {
                tracing::debug!(
                    peer_id = %target_peer_id,
                    count = entries.len(),
                    "received public entries"
                );
                overlay.add_untrusted_entries(&entries, now_sec());
            }
            PublicEntriesResponse::OverlayNotFound => {
                tracing::debug!(
                    peer_id = %target_peer_id,
                    "peer does not have the overlay",
                );
            }
        }

        // Done
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn store_public_entries(
        &self,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        use crate::proto::dht;

        const DEFAULT_TTL: u32 = 3600; // 1 hour

        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::debug!(%overlay_id, "overlay not found");
            return Ok(());
        };

        let now = now_sec();
        let mut n = std::cmp::max(self.config.public_overlay_peer_store_max_entries, 1);

        let data = {
            let rng = &mut rand::thread_rng();

            let mut entries = Vec::<Arc<PublicEntry>>::with_capacity(n);

            // Always include us in the list
            entries.push(Arc::new(self.make_local_public_overlay_entry(
                dht_client.network(),
                overlay_id,
                now,
            )));

            // Fill with random entries
            entries.extend(
                overlay
                    .read_entries()
                    .choose_multiple(rng, n - 1)
                    .map(|item| item.entry.clone()),
            );

            n = entries.len();

            // Serialize entries
            tl_proto::serialize(&entries)
        };

        // Store entries in the DHT
        let value = dht::ValueRef::Merged(dht::MergedValueRef {
            key: dht::MergedValueKeyRef {
                name: dht::MergedValueKeyName::PublicOverlayEntries,
                group_id: overlay_id.as_bytes(),
            },
            data: &data,
            expires_at: now + DEFAULT_TTL,
        });

        // TODO: Store the value on other nodes as well?
        dht_client.service().store_value_locally(&value)?;

        tracing::debug!(
            count = n,
            "stored public entries in the DHT",
        );
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

    fn add_private_overlay(&self, overlay: &PrivateOverlay) -> bool {
        use dashmap::mapref::entry::Entry;

        if self.public_overlays.contains_key(overlay.overlay_id()) {
            return false;
        }
        match self.private_overlays.entry(*overlay.overlay_id()) {
            Entry::Vacant(entry) => {
                entry.insert(overlay.clone());
                self.private_overlays_changed.notify_waiters();
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn remove_private_overlay(&self, overlay_id: &OverlayId) -> bool {
        let removed = self.private_overlays.remove(overlay_id).is_some();
        if removed {
            self.private_overlays_changed.notify_waiters();
        }
        removed
    }

    fn add_public_overlay(&self, overlay: &PublicOverlay) -> bool {
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

    fn remove_public_overlay(&self, overlay_id: &OverlayId) -> bool {
        let removed = self.public_overlays.remove(overlay_id).is_some();
        if removed {
            self.public_overlays_changed.notify_waiters();
        }
        removed
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
                .filter_map(|item| {
                    let is_new = !requested_ids.contains(&item.entry.peer_id);
                    is_new.then(|| item.entry.clone())
                })
                .take(n)
                .collect::<Vec<_>>()
        };

        PublicEntriesResponse::PublicEntries(entries)
    }
}

fn choose_random_resolved_peer<R>(
    entries: &PublicOverlayEntries,
    rng: &mut R,
) -> Option<KnownPeerHandle>
where
    R: Rng + ?Sized,
{
    entries
        .choose_all(rng)
        .find(|item| item.resolver_handle.is_resolved())
        .map(|item| {
            item.resolver_handle
                .load_handle()
                .expect("invalid resolved flag state")
        })
}
