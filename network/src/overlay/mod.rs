use std::sync::Arc;

use bytes::Buf;
use tl_proto::{TlError, TlRead};
use tokio::sync::Notify;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::time::now_sec;
use tycho_util::{FastDashMap, FastHashSet};

pub use self::config::OverlayConfig;
use self::entries_merger::PublicOverlayEntriesMerger;
pub use self::overlay_id::OverlayId;
pub use self::private_overlay::{
    ChooseMultiplePrivateOverlayEntries, PrivateOverlay, PrivateOverlayBuilder,
    PrivateOverlayEntries, PrivateOverlayEntriesEvent, PrivateOverlayEntriesReadGuard,
    PrivateOverlayEntriesWriteGuard, PrivateOverlayEntryData,
};
pub use self::public_overlay::{
    ChooseMultiplePublicOverlayEntries, PublicOverlay, PublicOverlayBuilder, PublicOverlayEntries,
    PublicOverlayEntriesReadGuard, PublicOverlayEntryData, UnknownPeersQueue,
};
use crate::dht::DhtService;
use crate::network::Network;
use crate::proto::overlay::{rpc, PublicEntriesResponse, PublicEntry, PublicEntryResponse};
use crate::types::{PeerId, Response, Service, ServiceRequest};
use crate::util::Routable;

mod background_tasks;
mod config;
mod entries_merger;
mod metrics;
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

    #[tracing::instrument(
        level = "debug",
        name = "on_overlay_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, mut req: ServiceRequest) -> Self::OnQueryFuture {
        let e = 'req: {
            let mut req_body = req.body.as_ref();
            if req_body.len() < 4 {
                break 'req TlError::UnexpectedEof;
            }

            let overlay_id = match std::convert::identity(req_body).get_u32_le() {
                rpc::Prefix::TL_ID => match rpc::Prefix::read_from(&mut req_body) {
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
                rpc::GetPublicEntry::TL_ID => {
                    let req = match tl_proto::deserialize::<rpc::GetPublicEntry>(&req.body) {
                        Ok(req) => req,
                        Err(e) => break 'req e,
                    };
                    tracing::debug!("get_public_entry");

                    let res = self.0.handle_get_public_entry(&req);
                    return BoxFutureOrNoop::future(futures_util::future::ready(Some(
                        Response::from_tl(res),
                    )));
                }
                _ => break 'req TlError::UnknownConstructor,
            };

            if req_body.len() < 4 {
                // Definitely an invalid request (not enough bytes for the constructor)
                break 'req TlError::UnexpectedEof;
            }
            let offset = req.body.len() - req_body.len();

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
            let mut req_body = req.body.as_ref();
            if req_body.len() < 4 {
                break 'req TlError::UnexpectedEof;
            }

            let overlay_id = match std::convert::identity(req_body).get_u32_le() {
                rpc::Prefix::TL_ID => match rpc::Prefix::read_from(&mut req_body) {
                    Ok(rpc::Prefix { overlay_id }) => overlay_id,
                    Err(e) => break 'req e,
                },
                _ => break 'req TlError::UnknownConstructor,
            };

            if req_body.len() < 4 {
                // Definitely an invalid request (not enough bytes for the constructor)
                break 'req TlError::UnexpectedEof;
            }
            let offset = req.body.len() - req_body.len();

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
}

impl Routable for OverlayService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [
            rpc::ExchangeRandomPublicEntries::TL_ID,
            rpc::GetPublicEntry::TL_ID,
            rpc::Prefix::TL_ID,
        ]
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
        overlay.add_untrusted_entries(&self.local_id, &req.entries, now_sec());

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

    fn handle_get_public_entry(&self, req: &rpc::GetPublicEntry) -> PublicEntryResponse {
        // Find the overlay
        let overlay = match self.public_overlays.get(&req.overlay_id) {
            Some(overlay) => overlay,
            None => return PublicEntryResponse::OverlayNotFound,
        };

        let Some(entry) = overlay.own_signed_entry() else {
            // NOTE: We return `OverlayNotFound` because if there is no signed entry
            // stored, then the background tasks are not running at all and this is
            // kind of "shadow" mode which is identical to not being in the overlay.
            return PublicEntryResponse::OverlayNotFound;
        };

        PublicEntryResponse::Found(entry)
    }
}
