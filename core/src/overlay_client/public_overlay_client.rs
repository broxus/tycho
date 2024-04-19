use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Error, Result};
use itertools::any;
use tl_proto::TlRead;

use crate::overlay_client::neighbour::{Neighbour, NeighbourOptions};
use tycho_network::{Network, PeerId};
use tycho_network::{PublicOverlay, Request};

use crate::overlay_client::neighbours::{NeighbourCollection, Neighbours};
use crate::overlay_client::settings::{OverlayClientSettings, OverlayOptions};
use crate::proto::overlay::{Ping, Pong, Response};

pub trait OverlayClient {
    async fn send<R>(&self, data: R) -> Result<()>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>;

    async fn query<R, A>(&self, data: R) -> Result<QueryResponse<'_, A>>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>;
}

#[derive(Clone)]
pub struct PublicOverlayClient(Arc<OverlayClientState>);

#[derive(Clone)]
pub struct Peer {
    pub id: PeerId,
    pub expires_at: u32,
}

impl PublicOverlayClient {
    pub async fn new(
        network: Network,
        overlay: PublicOverlay,
        settings: OverlayClientSettings,
    ) -> Self {
        let ttl = overlay.entry_ttl_sec();
        let peers = {
            overlay
                .read_entries()
                .choose_multiple(
                    &mut rand::thread_rng(),
                    settings.neighbours_options.max_neighbours,
                )
                .map(|entry_data| Peer {
                    id: entry_data.entry.peer_id,
                    expires_at: entry_data.expires_at(ttl),
                })
                .collect::<Vec<_>>()
        };

        let neighbours = Neighbours::new(peers, settings.neighbours_options).await;
        let neighbours_collection = NeighbourCollection(neighbours);
        Self(Arc::new(OverlayClientState {
            network,
            overlay,
            neighbours: neighbours_collection,
            settings: settings.overlay_options,
        }))
    }

    fn neighbours(&self) -> &Arc<Neighbours> {
        &self.0.neighbours.0
    }

    pub async fn entries_removed(&self) {
        self.0.overlay.entries_removed().notified().await
    }
    pub fn neighbour_update_interval_ms(&self) -> u64 {
        self.0.settings.neighbours_update_interval
    }
    pub fn neighbour_ping_interval_ms(&self) -> u64 {
        self.0.settings.neighbours_ping_interval
    }

    pub async fn update_neighbours(&self) {
        let active_neighbours = self.neighbours().get_active_neighbours().await.len();
        let max_neighbours = self.neighbours().options().max_neighbours;

        let neighbours_to_get = max_neighbours + (max_neighbours - active_neighbours);
        let neighbour_options = self.neighbours().options().clone();

        let neighbour_options = NeighbourOptions {
            default_roundtrip_ms: neighbour_options.default_roundtrip_ms,
        };
        let neighbours = {
            self.0
                .overlay
                .read_entries()
                .choose_multiple(&mut rand::thread_rng(), neighbours_to_get)
                .map(|x| {
                    Neighbour::new(
                        x.entry.peer_id,
                        x.expires_at(self.0.overlay.entry_ttl_sec()),
                        neighbour_options,
                    )
                })
                .collect::<Vec<_>>()
        };
        self.neighbours().update(neighbours).await;
    }

    pub async fn remove_outdated_neighbours(&self) {
        self.neighbours().remove_outdated_neighbours().await;
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

        let success = match pong_res {
            Ok(response) => {
                response.parse_tl::<Pong>()?;
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
        self.neighbours().update_selection_index().await;

        Ok(())
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

    async fn query<R, A>(&self, data: R) -> Result<QueryResponse<'_, A>>
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
                let response = response.parse_tl::<Response<A>>()?;
                let response_model = match response {
                    Response::Ok(r) => r,
                    Response::Err(code) => {
                        return Err(Error::msg(format!("Failed to get response: {code}")))
                    }
                };

                Ok(QueryResponse {
                    data: response_model,
                    roundtrip: start_time.duration_since(end_time).as_millis() as u64,
                    neighbour: neighbour.clone(),
                    _marker: PhantomData,
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

    settings: OverlayOptions,
}

pub struct QueryResponse<'a, A: TlRead<'a>> {
    data: A,
    neighbour: Neighbour,
    roundtrip: u64,
    _marker: PhantomData<&'a ()>,
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
