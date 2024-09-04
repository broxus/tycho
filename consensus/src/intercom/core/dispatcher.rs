use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_network::{Network, PeerId, PrivateOverlay};
use tycho_util::metrics::HistogramGuard;

use crate::intercom::core::dto::{MPQuery, MPResponse};
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    overlay: PrivateOverlay,
    network: Network,
}

impl Dispatcher {
    pub fn new(network: &Network, private_overlay: &PrivateOverlay) -> Self {
        Self {
            overlay: private_overlay.clone(),
            network: network.clone(),
        }
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
        let metric = request.metric();
        let request = request.data().clone();
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        async move {
            let _task_duration = metric;
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
    fn metric(&self) -> HistogramGuard {
        match self {
            QueryKind::Broadcast(_) => {
                HistogramGuard::begin("tycho_mempool_broadcast_query_dispatcher_time")
            }
            QueryKind::Signature(_) => {
                HistogramGuard::begin("tycho_mempool_signature_query_dispatcher_time")
            }
            QueryKind::PointById(_) => {
                HistogramGuard::begin("tycho_mempool_download_query_dispatcher_time")
            }
        }
    }
}
