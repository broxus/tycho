use anyhow::{anyhow, Result};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_network::{DhtClient, Network, OverlayId, OverlayService, PeerId, PrivateOverlay};

use crate::intercom::core::dto::{MPQuery, MPResponse};
use crate::intercom::core::responder::Responder;
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    pub overlay: PrivateOverlay,
    network: Network,
}

impl Dispatcher {
    const PRIVATE_OVERLAY_ID: OverlayId = OverlayId(*b"ac87b6945b4f6f736963f7f65d025943");

    pub fn new(
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        all_peers: &[PeerId],
        responder: Responder,
    ) -> Self {
        let dht_service = dht_client.service();
        let peer_resolver = dht_service.make_peer_resolver().build(dht_client.network());

        let private_overlay = PrivateOverlay::builder(Self::PRIVATE_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .with_entries(all_peers)
            .build(responder);

        overlay_service.add_private_overlay(&private_overlay);

        Self {
            overlay: private_overlay,
            network: dht_client.network().clone(),
        }
    }

    pub fn point_by_id_request(&self, id: &PointId) -> tycho_network::Request {
        (&MPQuery::PointById(id.clone())).into()
    }

    pub fn signature_request(round: &Round) -> tycho_network::Request {
        (&MPQuery::Signature(round.clone())).into()
    }

    pub fn query<T>(
        &self,
        peer_id: &PeerId,
        request: &tycho_network::Request,
    ) -> BoxFuture<'static, (PeerId, Result<T>)>
    where
        T: TryFrom<MPResponse, Error = anyhow::Error>,
    {
        let peer_id = peer_id.clone();
        let request = request.clone();
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        async move {
            overlay
                .query(&network, &peer_id, request)
                .map(move |response| {
                    let response = response
                        .and_then(|r| MPResponse::try_from(&r))
                        .and_then(T::try_from)
                        .map_err(|e| anyhow!("response from peer {peer_id}: {e}"));
                    (peer_id, response)
                })
                .await
        }
        .boxed()
    }

    pub fn broadcast_request(point: &Point) -> tycho_network::Request {
        point.into()
    }

    pub fn send(
        &self,
        peer_id: &PeerId,
        request: &tycho_network::Request,
    ) -> BoxFuture<'static, (PeerId, Result<()>)> {
        let peer_id = peer_id.clone();
        let request = request.clone();
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        async move {
            overlay
                .send(&network, &peer_id, request)
                .map(move |response| {
                    let response =
                        response.map_err(|e| anyhow!("response from peer {peer_id}: {e}"));
                    (peer_id, response)
                })
                .await
        }
        .boxed()
    }
}
