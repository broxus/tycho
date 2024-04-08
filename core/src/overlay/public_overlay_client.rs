use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use tl_proto::{TlRead, TlWrite};

use tycho_network::Network;
use tycho_network::{NetworkExt, PeerId, PublicOverlay};

use crate::overlay::neighbours::{NeighbourCollection, Neighbours};
use crate::proto::overlay::{Ping, Pong};

trait OverlayClient {
    async fn send<R: TlWrite>(&self, data: R) -> Result<()>;

    async fn query<R, A>(&self, data: R) -> Result<Option<A>>
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

    pub async fn ping_random_neighbour(&self) -> Result<()> {
        let Some(neighbour) = self.0.neighbours.0.choose().await else {
            tracing::error!("No neighbours found to ping");
            return Err(Error::msg("Failed to ping"));
        };

        tracing::info!(
            peer_id = %neighbour.peer_id(),
            stats = ?neighbour.get_stats(),
            "Selected neighbour to ping",
        );
        let start_time = tycho_util::time::now_millis();
        let ping = Ping { value: start_time };

        let pong_res = self
            .0
            .overlay
            .query(&self.0.network, neighbour.peer_id(), ping.into())
            .await;

        let end_time = tycho_util::time::now_millis();

        let success = match pong_res {
            Ok(response) => {
                let pong: Pong = response.parse_tl::<Pong>()?;

                tracing::info!(peer_id = %neighbour.peer_id(), "Pong received", );

                // Ok(NeighbourPingResult {
                //     peer: *neighbour.peer_id(),
                //     request_time: (pong.value - start_time) as u32,
                //     rt_time: (end_time - start_time) as u32,
                // })
                true
            }
            Err(e) => {
                tracing::error!(peer_id = %neighbour.peer_id(), "Failed to received pong. Error: {e:?}");
                false
            }
        };

        neighbour.track_request(end_time - start_time, success);
        self.0.neighbours.0.update_selection_index().await;

        Ok(())
    }
}

impl OverlayClient for PublicOverlayClient {
    async fn send<R: TlWrite>(&self, data: R) -> Result<()> {
        let Some(neighbour) = self.0.neighbours().choose().await else {
            tracing::error!("No neighbours found to send request");
            return Err(Error::msg("Failed to ping")); //TODO: proper error
        };

        self.0
            .overlay
            .send(&self.0.network, neighbour.peer_id(), data)
            .await
    }

    async fn query<R, A>(&self, data: R) -> Result<Option<A>>
    where
        R: TlWrite,
        for<'a> A: TlRead<'a>,
    {
        let Some(neighbour) = self.0.neighbours().choose().await else {
            tracing::error!("No neighbours found to send request");
            return Err(Error::msg("Failed to ping")); //TODO: proper error
        };

        self.0
            .overlay
            .query(&self.0.network, neighbour.peer_id(), data)
            .await
    }
}

pub struct NeighbourPingResult {
    peer: PeerId,
    request_time: u32,
    rt_time: u32,
}

struct OverlayClientState {
    network: Network,
    overlay: PublicOverlay,
    neighbours: NeighbourCollection,
}
