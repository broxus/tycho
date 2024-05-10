pub mod retry;

use std::sync::Arc;
use std::time::Duration;
use tycho_network::{Network, PeerId, PrivateOverlay, Request};
use crate::types::ValidatorNetwork;
use crate::validator::network::dto::SignaturesQuery;
use backon::{Retryable, ExponentialBuilder, Backoff};
use anyhow::{anyhow, Result};

pub struct ValidatorClient {
    network: Network,
    private_overlay: PrivateOverlay,
    peer_id: PeerId,
}

impl ValidatorClient {
    pub fn new(network: Network, private_overlay: PrivateOverlay, peer_id: PeerId) -> Self {
        ValidatorClient {
            network,
            private_overlay,
            peer_id,
        }
    }

    pub async fn request_signatures(
        &self,
        payload: SignaturesQuery,
        timeout_duration: Duration,
    ) -> Result<SignaturesQuery> {
        let payload = Request::from_tl(payload);
        match tokio::time::timeout(timeout_duration, self.private_overlay.query(&self.network, &self.peer_id, payload)).await {
            Ok(Ok(response)) => {
                response.parse_tl::<SignaturesQuery>().map_err(Into::into)
            },
            Ok(Err(e)) => {
                Err(anyhow!("Network error: {}", e))
            },
            Err(elapsed) => {
                Err(anyhow!("Timeout during request. Elapsed: {:?}", elapsed))
            },
        }
    }
}


