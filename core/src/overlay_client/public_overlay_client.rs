use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use tl_proto::{Repr, TlRead, TlWrite};

use crate::overlay_client::neighbour::Neighbour;
use tycho_network::Network;
use tycho_network::{NetworkExt, PeerId, PublicOverlay, Request};

use crate::overlay_client::neighbour::Neighbour;
use crate::overlay_client::neighbours::{NeighbourCollection, Neighbours};
use crate::overlay_client::settings::{OverlayClientSettings, OverlayOptions};
use crate::proto::overlay::{Ping, Pong};

trait OverlayClient {
    async fn send<R>(&self, data: R) -> Result<()>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>;

    async fn query<R, A>(&self, data: R) -> Result<QueryResponse<A>>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>;
}

#[derive(Clone)]
pub struct PublicOverlayClient(Arc<OverlayClientState>);

impl PublicOverlayClient {

    pub async fn new(network: Network, overlay: PublicOverlay, settings: OverlayClientSettings) -> Self {
        let neighbours = Neighbours::new(overlay.clone(), settings.neighbours_options).await;
        let neighbours_collection = NeighbourCollection(neighbours);
        Self(Arc::new(OverlayClientState {
            network,
            overlay,
            neighbours: neighbours_collection,
            settings: settings.overlay_options,
        }))
    }

    pub async fn update_neighbours(&self) -> Result<()> {
        let neighbours = self.0.overlay.read_entries()
            .choose_multiple(&mut rand::thread_rng(), 10);
        //self.0.neighbours.0.update()
        Ok(())
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

        let start_time = Instant::now();

        let pong_res = self
            .0
            .overlay
            .query(&self.0.network, neighbour.peer_id(), Request::from_tl(Ping))
            .await;

        let end_time = Instant::now();

        let (success) = match pong_res {
            Ok(response) => {
                let pong = response.parse_tl::<Pong>()?;
                tracing::info!(peer_id = %neighbour.peer_id(), "Pong received", );
                true
            }
            Err(e) => {
                tracing::error!(peer_id = %neighbour.peer_id(), "Failed to received pong. Error: {e:?}");
                false
            }
        };

        neighbour.track_request(
            end_time.duration_since(start_time).as_millis() as u64,
            success,
        );
        self.0.neighbours.0.update_selection_index().await;

        Ok(())
    }

    pub fn update_interval(&self) -> u64 {
        self.0.settings.neighbours_update_interval
    }

    pub fn ping_interval(&self) -> u64 {
        self.0.settings.neighbours_ping_interval
    }

}

impl OverlayClient for PublicOverlayClient {
    async fn send<R>(&self, data: R) -> Result<()>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.0.neighbours.0.choose().await else {
            tracing::error!("No neighbours found to send request");
            return Err(Error::msg("Failed to ping")); //TODO: proper error
        };

        //let boxed = tl_proto::serialize(data);
        self.0
            .overlay
            .send(&self.0.network, neighbour.peer_id(), Request::from_tl(data))
            .await?;
        Ok(())
    }

    async fn query<R, A>(&self, data: R) -> Result<QueryResponse<A>>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.0.neighbours.0.choose().await else {
            tracing::error!("No neighbours found to send request");
            return Err(Error::msg("Failed to ping")); //TODO: proper error
        };

        let start_time = Instant::now();
        let response_opt = self
            .0
            .overlay
            .query(&self.0.network, neighbour.peer_id(), Request::from_tl(data))
            .await;
        let end_time = Instant::now();

        match response_opt {
            Ok(response) => {
                let response_model = response.parse_tl::<A>()?;

                Ok(QueryResponse {
                    data: response_model,
                    roundtrip: start_time.duration_since(end_time).as_millis() as u64,
                    neighbour: neighbour.clone(),
                    _market: PhantomData,
                })
            }
            Err(e) => {
                tracing::error!(peer_id = %neighbour.peer_id(), "Failed to get response from peer. Err: {e:?}");
                Err(e)
            }
        }
    }
}

struct OverlayClientState {
    network: Network,
    overlay: PublicOverlay,
    neighbours: NeighbourCollection,

    settings: OverlayOptions
}



pub struct QueryResponse<'a, A: TlRead<'a>> {
    pub data: A,
    neighbour: Neighbour,
    roundtrip: u64,
    _market: PhantomData<&'a ()>,
}

impl<'a, A> QueryResponse<'a, A>
where
    A: TlRead<'a, Repr = tl_proto::Boxed>,
{
    pub fn data(&self) -> &A {
        &self.data
    }
    pub fn mark_response(&self, success: bool) {
        self.neighbour.track_request(self.roundtrip, success);
    }
}
