use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_network::{DhtClient, Network, OverlayId, OverlayService, PeerId, PrivateOverlay};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::dto::{MPQuery, MPResponse};
use crate::intercom::core::responder::Responder;
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}

impl Dispatcher {
    const PRIVATE_OVERLAY_ID: OverlayId = OverlayId(*b"ac87b6945b4f6f736963f7f65d025943");

    pub fn new(
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        responder: Responder,
    ) -> (Self, PrivateOverlay) {
        let dht_service = dht_client.service();
        let peer_resolver = dht_service.make_peer_resolver().build(dht_client.network());

        let private_overlay = PrivateOverlay::builder(Self::PRIVATE_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .build(responder);

        overlay_service.add_private_overlay(&private_overlay);

        let this = Self {
            overlay: private_overlay.clone(),
            network: dht_client.network().clone(),
        };

        (this, private_overlay)
    }

    pub fn broadcast_request(point: &Point) -> QueryKind {
        QueryKind::Broadcast((&MPQuery::Broadcast(point.clone())).into())
    }

    pub fn signature_request(round: Round) -> QueryKind {
        QueryKind::Signature((&MPQuery::Signature(round)).into())
    }

    pub fn point_by_id_request(id: &PointId) -> QueryKind {
        QueryKind::PointById((&MPQuery::PointById(id.clone())).into())
    }

    pub fn query<T>(
        &self,
        peer_id: &PeerId,
        request: &QueryKind,
    ) -> BoxFuture<'static, (PeerId, Result<T>)>
    where
        T: TryFrom<MPResponse, Error = anyhow::Error>,
    {
        let peer_id = *peer_id;
        let metric_name = request.metric_name();
        let request = request.data().clone();
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        async move {
            let _task_duration = HistogramGuard::begin(metric_name);
            overlay
                .query(&network, &peer_id, request)
                .map(move |response| {
                    let response = response
                        .and_then(|r| MPResponse::try_from(&r))
                        .and_then(T::try_from);
                    (peer_id, response)
                })
                .await
        }
        .boxed()
    }
}

pub enum QueryKind {
    Broadcast(tycho_network::Request),
    Signature(tycho_network::Request),
    PointById(tycho_network::Request),
}

impl QueryKind {
    fn data(&self) -> &tycho_network::Request {
        match self {
            QueryKind::Broadcast(data)
            | QueryKind::Signature(data)
            | QueryKind::PointById(data) => data,
        }
    }
    fn metric_name(&self) -> &'static str {
        match self {
            QueryKind::Broadcast(_) => "tycho_mempool_broadcast_query_dispatcher_time",
            QueryKind::Signature(_) => "tycho_mempool_signature_query_dispatcher_time",
            QueryKind::PointById(_) => "tycho_mempool_download_query_dispatcher_time",
        }
    }
}
