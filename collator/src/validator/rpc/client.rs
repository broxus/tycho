use std::sync::Arc;

use anyhow::Result;
use tycho_network::{Body, Network, PeerId, PrivateOverlay, Response};

#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorClient {
    inner: Arc<Inner>,
}

impl ValidatorClient {
    pub fn new(network: Network, overlay: PrivateOverlay) -> Self {
        Self {
            inner: Arc::new(Inner { network, overlay }),
        }
    }

    pub fn network(&self) -> &Network {
        &self.inner.network
    }

    pub fn overlay(&self) -> &PrivateOverlay {
        &self.inner.overlay
    }

    pub fn peer_id(&self) -> &PeerId {
        self.inner.network.peer_id()
    }

    pub async fn query(&self, peer_id: &PeerId, req: Body) -> Result<Response> {
        self.inner
            .overlay
            .query(&self.inner.network, peer_id, req)
            .await
    }
}

struct Inner {
    network: Network,
    overlay: PrivateOverlay,
}
