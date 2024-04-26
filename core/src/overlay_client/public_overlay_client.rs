use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Error, Result};
use tycho_network::Network;
use tycho_network::{PublicOverlay, Request};

use crate::overlay_client::neighbour::{Neighbour, NeighbourOptions};
use crate::overlay_client::neighbours::Neighbours;
use crate::overlay_client::settings::{OverlayClientSettings, OverlayOptions};
use crate::proto::overlay::{Ping, Pong, Response};

#[derive(Clone)]
#[repr(transparent)]
pub struct PublicOverlayClient(Arc<OverlayClientState>);

impl PublicOverlayClient {
    pub fn new(network: Network, overlay: PublicOverlay, settings: OverlayClientSettings) -> Self {
        let neighbour_options = NeighbourOptions {
            default_roundtrip_ms: settings.neighbours_options.default_roundtrip_ms,
        };

        let ttl = overlay.entry_ttl_sec();
        let entries = {
            overlay
                .read_entries()
                .choose_multiple(
                    &mut rand::thread_rng(),
                    settings.neighbours_options.max_neighbours,
                )
                .map(|entry_data| {
                    Neighbour::new(
                        entry_data.entry.peer_id,
                        entry_data.expires_at(ttl),
                        neighbour_options,
                    )
                })
                .collect::<Vec<_>>()
        };

        let neighbours = Neighbours::new(entries, settings.neighbours_options);

        let inner = Arc::new(OverlayClientState {
            network,
            overlay,
            neighbours,
            settings: settings.overlay_options,
        });

        Self(inner)
    }

    pub fn neighbours(&self) -> &Neighbours {
        &self.0.neighbours
    }

    pub async fn send<R>(&self, data: R) -> Result<()>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.0.neighbours.choose().await else {
            tracing::error!("No neighbours found to send request");
            return Err(Error::msg("Failed to ping")); //TODO: proper error
        };

        self.0
            .overlay
            .send(&self.0.network, neighbour.peer_id(), Request::from_tl(data))
            .await?;
        Ok(())
    }

    pub async fn query<R, A>(&self, data: R) -> Result<QueryResponse<A>>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.0.neighbours.choose().await else {
            tracing::error!("No neighbours found to send request");
            return Err(Error::msg("Failed to ping")); //TODO: proper error
        };

        let start_time = Instant::now();
        let response_opt = self
            .0
            .overlay
            .query(&self.0.network, neighbour.peer_id(), Request::from_tl(data))
            .await;
        let roundtrip = start_time.elapsed();

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
                    roundtrip: roundtrip.as_millis() as u64,
                    neighbour: neighbour.clone(),
                })
            }
            Err(e) => {
                tracing::error!(peer_id = %neighbour.peer_id(), "Failed to get response from peer. Err: {e:?}");
                Err(e)
            }
        }
    }

    pub async fn wait_entries_removed(&self) {
        self.0.overlay.entries_removed().notified().await;
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
        let Some(neighbour) = self.0.neighbours.choose().await else {
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

async fn start_neighbours_ping(client: PublicOverlayClient) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(client.neighbour_update_interval_ms()));

    loop {
        interval.tick().await;
        if let Err(e) = client.ping_random_neighbour().await {
            tracing::error!("Failed to ping random neighbour. Error: {e:?}");
        }
    }
}

async fn start_neighbours_update(client: PublicOverlayClient) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(client.neighbour_update_interval_ms()));
    loop {
        interval.tick().await;
        client.update_neighbours().await;
    }
}

async fn wait_update_neighbours(client: PublicOverlayClient) {
    loop {
        client.wait_entries_removed().await;
        client.remove_outdated_neighbours().await;
    }
}

struct OverlayClientState {
    network: Network,
    overlay: PublicOverlay,
    neighbours: Neighbours,

    settings: OverlayOptions,
}

pub struct QueryResponse<A> {
    data: A,
    neighbour: Neighbour,
    roundtrip: u64,
}

impl<A> QueryResponse<A> {
    pub fn data(&self) -> &A {
        &self.data
    }

    pub fn mark_response(&self, success: bool) {
        self.neighbour.track_request(self.roundtrip, success);
    }
}
