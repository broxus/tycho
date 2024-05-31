use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use tokio::task::AbortHandle;
use tycho_network::{Network, PublicOverlay, Request};

pub use self::config::PublicOverlayClientConfig;
pub use self::neighbour::{Neighbour, NeighbourStats};
pub use self::neighbours::Neighbours;
use crate::proto::overlay;

mod config;
mod neighbour;
mod neighbours;

#[derive(Clone)]
#[repr(transparent)]
pub struct PublicOverlayClient {
    inner: Arc<Inner>,
}

impl PublicOverlayClient {
    pub fn new(
        network: Network,
        overlay: PublicOverlay,
        config: PublicOverlayClientConfig,
    ) -> Self {
        let ttl = overlay.entry_ttl_sec();

        let entries = overlay
            .read_entries()
            .choose_multiple(&mut rand::thread_rng(), config.max_neighbours)
            .map(|entry_data| {
                Neighbour::new(
                    entry_data.entry.peer_id,
                    entry_data.expires_at(ttl),
                    &config.default_roundtrip,
                )
            })
            .collect::<Vec<_>>();

        let neighbours = Neighbours::new(entries, config.max_neighbours);

        let mut res = Inner {
            network,
            overlay,
            neighbours,
            config,
            ping_task: None,
            update_task: None,
            cleanup_task: None,
        };

        // NOTE: Reuse same `Inner` type to avoid introducing a new type for shard state
        // NOTE: Clone does not clone the tasks
        res.ping_task = Some(tokio::spawn(res.clone().ping_neighbours_task()).abort_handle());
        res.update_task = Some(tokio::spawn(res.clone().update_neighbours_task()).abort_handle());
        res.cleanup_task = Some(tokio::spawn(res.clone().cleanup_neighbours_task()).abort_handle());

        Self {
            inner: Arc::new(res),
        }
    }

    pub fn config(&self) -> &PublicOverlayClientConfig {
        &self.inner.config
    }

    pub fn neighbours(&self) -> &Neighbours {
        &self.inner.neighbours
    }

    pub fn overlay(&self) -> &PublicOverlay {
        &self.inner.overlay
    }

    pub fn network(&self) -> &Network {
        &self.inner.network
    }

    pub async fn send<R>(&self, data: R) -> Result<(), Error>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        self.inner.send(data).await
    }

    #[inline]
    pub async fn send_raw(&self, neighbour: Neighbour, req: Request) -> Result<(), Error> {
        self.inner.send_impl(neighbour, req).await
    }

    pub async fn query<R, A>(&self, data: R) -> Result<QueryResponse<A>, Error>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        self.inner.query(data).await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("no active neighbours found")]
    NoNeighbours,
    #[error("network error: {0}")]
    NetworkError(#[source] anyhow::Error),
    #[error("invalid response: {0}")]
    InvalidResponse(#[source] tl_proto::TlError),
    #[error("request failed with code: {0}")]
    RequestFailed(u32),
}

struct Inner {
    network: Network,
    overlay: PublicOverlay,
    neighbours: Neighbours,
    config: PublicOverlayClientConfig,

    ping_task: Option<AbortHandle>,
    update_task: Option<AbortHandle>,
    cleanup_task: Option<AbortHandle>,
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            overlay: self.overlay.clone(),
            neighbours: self.neighbours.clone(),
            config: self.config.clone(),
            ping_task: None,
            update_task: None,
            cleanup_task: None,
        }
    }
}

impl Inner {
    async fn ping_neighbours_task(self) {
        let req = Request::from_tl(overlay::Ping);

        // Start pinging neighbours
        let mut interval = tokio::time::interval(self.config.neighbours_ping_interval);
        loop {
            interval.tick().await;

            let Some(neighbour) = self.neighbours.choose().await else {
                continue;
            };

            let peer_id = *neighbour.peer_id();
            match self.query_impl(neighbour.clone(), req.clone()).await {
                Ok(res) => match tl_proto::deserialize::<overlay::Pong>(&res.data) {
                    Ok(_) => {
                        res.accept();
                        tracing::debug!(%peer_id, "pinged neighbour");
                    }
                    Err(e) => {
                        tracing::warn!(
                            %peer_id,
                            "received an invalid ping response: {e}",
                        );
                        res.reject();
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        %peer_id,
                        "failed to ping neighbour: {e}",
                    );
                    continue;
                }
            }
        }
    }

    async fn update_neighbours_task(self) {
        let ttl = self.overlay.entry_ttl_sec();
        let max_neighbours = self.config.max_neighbours;
        let default_roundtrip = self.config.default_roundtrip;

        let mut overlay_peers_added = self.overlay.entires_added().notified();
        let mut overlay_peer_count = self.overlay.read_entries().len();

        let mut interval = tokio::time::interval(self.config.neighbours_update_interval);

        loop {
            if overlay_peer_count < max_neighbours {
                tracing::info!("not enough neighbours, waiting for more");

                overlay_peers_added.await;
                overlay_peers_added = self.overlay.entires_added().notified();

                overlay_peer_count = self.overlay.read_entries().len();
            } else {
                interval.tick().await;
            }

            let active_neighbours = self.neighbours.get_active_neighbours().await.len();
            let neighbours_to_get = max_neighbours + (max_neighbours - active_neighbours);

            let neighbours = {
                self.overlay
                    .read_entries()
                    .choose_multiple(&mut rand::thread_rng(), neighbours_to_get)
                    .map(|x| Neighbour::new(x.entry.peer_id, x.expires_at(ttl), &default_roundtrip))
                    .collect::<Vec<_>>()
            };
            self.neighbours.update(neighbours).await;
        }
    }

    async fn cleanup_neighbours_task(self) {
        loop {
            self.overlay.entries_removed().notified().await;
            self.neighbours.remove_outdated_neighbours().await;
        }
    }

    async fn send<R>(&self, data: R) -> Result<(), Error>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.neighbours.choose().await else {
            return Err(Error::NoNeighbours);
        };

        self.send_impl(neighbour, Request::from_tl(data)).await
    }

    async fn query<R, A>(&self, data: R) -> Result<QueryResponse<A>, Error>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.neighbours.choose().await else {
            return Err(Error::NoNeighbours);
        };

        let res = self.query_impl(neighbour, Request::from_tl(data)).await?;

        let response = match tl_proto::deserialize::<overlay::Response<A>>(&res.data) {
            Ok(r) => r,
            Err(e) => {
                res.reject();
                return Err(Error::InvalidResponse(e));
            }
        };

        match response {
            overlay::Response::Ok(data) => Ok(QueryResponse {
                data,
                roundtrip_ms: res.roundtrip_ms,
                neighbour: res.neighbour,
            }),
            overlay::Response::Err(code) => {
                res.reject();
                Err(Error::RequestFailed(code))
            }
        }
    }

    async fn send_impl(&self, neighbour: Neighbour, req: Request) -> Result<(), Error> {
        let started_at = Instant::now();

        let res = self
            .overlay
            .send(&self.network, neighbour.peer_id(), req)
            .await;

        let roundtrip = started_at.elapsed() * 2; // Multiply by 2 to estimate the roundtrip time
        neighbour.track_request(&roundtrip, res.is_ok());

        res.map_err(Error::NetworkError)
    }

    async fn query_impl(
        &self,
        neighbour: Neighbour,
        req: Request,
    ) -> Result<QueryResponse<Bytes>, Error> {
        let started_at = Instant::now();

        let res = self
            .overlay
            .query(&self.network, neighbour.peer_id(), req)
            .await;

        let roundtrip = started_at.elapsed();

        match res {
            Ok(response) => Ok(QueryResponse {
                data: response.body,
                roundtrip_ms: roundtrip.as_millis() as u64,
                neighbour,
            }),
            Err(e) => {
                neighbour.track_request(&roundtrip, false);
                Err(Error::NetworkError(e))
            }
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self.ping_task.take() {
            handle.abort();
        }

        if let Some(handle) = self.update_task.take() {
            handle.abort();
        }

        if let Some(handle) = self.cleanup_task.take() {
            handle.abort();
        }
    }
}

pub struct QueryResponse<A> {
    data: A,
    neighbour: Neighbour,
    roundtrip_ms: u64,
}

impl<A> QueryResponse<A> {
    pub fn data(&self) -> &A {
        &self.data
    }

    pub fn accept(self) -> (Neighbour, A) {
        self.track_request(true);
        (self.neighbour, self.data)
    }

    pub fn reject(self) -> (Neighbour, A) {
        self.track_request(false);
        (self.neighbour, self.data)
    }

    fn track_request(&self, success: bool) {
        self.neighbour
            .track_request(&Duration::from_millis(self.roundtrip_ms), success);
    }
}
