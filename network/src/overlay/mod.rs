use std::sync::Arc;

use bytes::Buf;
use tl_proto::{TlError, TlRead};
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::{FastDashMap, FastHashSet};

use crate::proto::overlay::{rpc, PublicEntriesResponse};
use crate::types::{PeerId, Response, Service, ServiceRequest};
use crate::util::Routable;

pub use self::overlay_id::OverlayId;
pub use self::public_overlay::{PublicOverlay, PublicOverlayBuilder};

mod overlay_id;
mod public_overlay;

pub struct OverlayServiceBuilder {
    local_id: PeerId,
    public_overlays: FastDashMap<OverlayId, PublicOverlay>,
}

impl OverlayServiceBuilder {
    pub fn with_public_overlay(self, overlay: PublicOverlay) -> Self {
        let prev = self.public_overlays.insert(*overlay.overlay_id(), overlay);
        if let Some(prev) = prev {
            panic!("overlay with id {} already exists", prev.overlay_id());
        }
        self
    }

    pub fn build(self) -> OverlayService {
        let inner = Arc::new(OverlayServiceInner {
            local_id: self.local_id,
            public_overlays: self.public_overlays,
        });

        // TODO: add overlay client builder
        OverlayService(inner)
    }
}

#[derive(Clone)]
pub struct OverlayService(Arc<OverlayServiceInner>);

impl OverlayService {
    pub fn builder(local_id: PeerId) -> OverlayServiceBuilder {
        OverlayServiceBuilder {
            local_id,
            public_overlays: Default::default(),
        }
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

            // TODO: search for private overlay too

            if let Some(overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(overlay.service().on_query(req));
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

            // TODO: search for private overlay too

            if let Some(overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(overlay.service().on_message(req));
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
    public_overlays: FastDashMap<OverlayId, PublicOverlay>,
}

impl OverlayServiceInner {
    fn start_background_tasks(&self) {
        // TODO
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

        // TODO: skip adding entires past the limit
        // Add proposed entries to the overlay
        overlay.add_entries(&req.entries);

        // Collect proposed entries to exclude from the response
        let requested_ids = req
            .entries
            .iter()
            .map(|id| id.peer_id)
            .collect::<FastHashSet<_>>();

        let entries = {
            let entries = overlay.read_entries();

            // Choose 2x random entries to ensure we have enough new entires to send back
            // TODO: use response size limit from the config
            entries
                .choose_multiple(&mut rand::thread_rng(), requested_ids.len() * 2)
                .filter_map(|entry| {
                    let is_new = !requested_ids.contains(&entry.peer_id);
                    is_new.then(|| entry.clone())
                })
                .collect::<Vec<_>>()
        };

        PublicEntriesResponse::PublicEntries(entries)
    }
}
