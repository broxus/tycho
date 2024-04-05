use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::overlay::neighbours::{NeighbourCollection, Neighbours};
use tycho_network::Network;
use tycho_network::__internal::tl_proto::{TlRead, TlWrite};
use tycho_network::{NetworkExt, PeerId, PublicOverlay};
use tycho_util::FastDashMap;

trait OverlayClient {
    async fn send<R: TlWrite>(&self, data: R) -> Box<dyn Future<Output = ()>>;

    async fn query<R, A>(&self, data: R) -> Box<dyn Future<Output = Option<A>>>
    where
        R: TlWrite,
        for<'a> A: TlRead<'a>;
}

#[derive(Clone)]
pub struct PublicOverlayClient(Arc<OverlayClientState>);

impl PublicOverlayClient {
    pub fn new(network: Network, overlay: PublicOverlay, neighbours: NeighbourCollection) -> Self {
        Self(Arc::new(OverlayClientState {
            network,
            overlay,
            neighbours,
        }))
    }

    pub fn ping_neighbour(&self, peer: &PeerId) {}
}

impl OverlayClient for PublicOverlayClient {
    async fn send<R: TlWrite>(&self, data: R) -> Box<dyn Future<Output = ()>> {
        let neighbour = self.0.neighbours.0.choose().await;
        self.0.overlay.send(&self.0.network)
    }

    async fn query<R: TlWrite, A: TlRead>(&self, data: R) -> Box<dyn Future<Output = Option<A>>> {
        todo!()
    }
}

struct OverlayClientState {
    network: Network,
    overlay: PublicOverlay,
    neighbours: NeighbourCollection,
}
