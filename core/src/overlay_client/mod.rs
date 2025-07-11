use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use tokio::task::AbortHandle;
use tycho_network::{ConnectionError, Network, PublicOverlay, Request, UnknownPeerError};

pub use self::config::{NeighborsConfig, PublicOverlayClientConfig, ValidatorsConfig};
pub use self::neighbour::{Neighbour, NeighbourStats, PunishReason};
pub use self::neighbours::{NeighbourType, Neighbours};
pub use self::validators::{Validator, ValidatorSetPeers, ValidatorsResolver};
use crate::proto::overlay;

mod config;
mod neighbour;
mod neighbours;
mod validators;

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

        let neighbors_config = &config.neighbors;

        let entries = overlay
            .read_entries()
            .choose_multiple(&mut rand::rng(), neighbors_config.keep)
            .map(|entry_data| {
                Neighbour::new(
                    entry_data.entry.peer_id,
                    entry_data.expires_at(ttl),
                    &neighbors_config.default_roundtrip,
                )
            })
            .collect::<Vec<_>>();

        let neighbours = Neighbours::new(entries, config.neighbors.keep);
        let validators_resolver =
            ValidatorsResolver::new(network.clone(), overlay.clone(), config.validators.clone());

        let mut res = Inner {
            network,
            overlay,
            neighbours,
            config,
            validators_resolver,
            ping_task: None,
            update_task: None,
            score_task: None,
            cleanup_task: None,
        };

        // NOTE: Reuse same `Inner` type to avoid introducing a new type for shard state
        // NOTE: Clone does not clone the tasks
        res.ping_task = Some(tokio::spawn(res.clone().ping_neighbours_task()).abort_handle());
        res.update_task = Some(tokio::spawn(res.clone().update_neighbours_task()).abort_handle());
        res.score_task = Some(tokio::spawn(res.clone().apply_score_task()).abort_handle());
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

    pub fn update_validator_set<T: ValidatorSetPeers>(&self, vset: &T) {
        self.inner.validators_resolver.update_validator_set(vset);
    }

    // Returns a small random subset of possibly alive validators.
    pub fn get_broadcast_targets(&self) -> Arc<Vec<Validator>> {
        self.inner.validators_resolver.get_broadcast_targets()
    }

    pub fn validators_resolver(&self) -> &ValidatorsResolver {
        &self.inner.validators_resolver
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

    pub async fn send_to_validator(
        &self,
        validator: Validator,
        data: Request,
    ) -> Result<(), Error> {
        self.inner.send_to_validator(validator.clone(), data).await
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

    #[inline]
    pub async fn query_raw<A>(
        &self,
        neighbour: Neighbour,
        req: Request,
    ) -> Result<QueryResponse<A>, Error>
    where
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        self.inner.query_impl(neighbour, req).await?.parse()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("no active neighbours found")]
    NoNeighbours,
    #[error("no neighbour has the requested data")]
    NotFound,
    #[error("network error: {0}")]
    NetworkError(#[source] anyhow::Error),
    #[error("invalid response: {0}")]
    InvalidResponse(#[source] tl_proto::TlError),
    #[error("request failed with code: {0}")]
    RequestFailed(u32),
    #[error("internal error: {0}")]
    Internal(#[source] anyhow::Error),
    #[error("timeout")]
    Timeout,
}

struct Inner {
    network: Network,
    overlay: PublicOverlay,
    neighbours: Neighbours,
    config: PublicOverlayClientConfig,

    validators_resolver: ValidatorsResolver,

    ping_task: Option<AbortHandle>,
    update_task: Option<AbortHandle>,
    score_task: Option<AbortHandle>,
    cleanup_task: Option<AbortHandle>,
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            overlay: self.overlay.clone(),
            neighbours: self.neighbours.clone(),
            config: self.config.clone(),
            validators_resolver: self.validators_resolver.clone(),
            ping_task: None,
            update_task: None,
            score_task: None,
            cleanup_task: None,
        }
    }
}

impl Inner {
    #[tracing::instrument(name = "ping_neighbours", skip_all)]
    async fn ping_neighbours_task(self) {
        tracing::info!("started");
        scopeguard::defer! { tracing::info!("finished"); };

        let req = Request::from_tl(overlay::Ping);

        // Start pinging neighbours
        let mut interval = tokio::time::interval(self.config.neighbors.ping_interval);
        loop {
            interval.tick().await;

            let Some(neighbour) = self.neighbours.choose() else {
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
                }
            }
        }
    }

    #[tracing::instrument(name = "update_neighbours", skip_all)]
    async fn update_neighbours_task(self) {
        tracing::info!("started");
        scopeguard::defer! { tracing::info!("finished"); };

        let ttl = self.overlay.entry_ttl_sec();
        let max_neighbours = self.config.neighbors.keep;
        let default_roundtrip = self.config.neighbors.default_roundtrip;

        let mut overlay_peers_added = self.overlay.entires_added().notified();
        let mut overlay_peer_count = self.overlay.read_entries().len();

        let mut interval = tokio::time::interval(self.config.neighbors.update_interval);

        loop {
            if overlay_peer_count < max_neighbours {
                tracing::info!("not enough neighbours, waiting for more");

                overlay_peers_added.await;
                overlay_peers_added = self.overlay.entires_added().notified();

                overlay_peer_count = self.overlay.read_entries().len();
            } else {
                interval.tick().await;
            }

            let active_neighbours = self.neighbours.get_active_neighbours().len();
            let neighbours_to_get = max_neighbours + (max_neighbours - active_neighbours);

            let neighbours = {
                self.overlay
                    .read_entries()
                    .choose_multiple(&mut rand::rng(), neighbours_to_get)
                    .map(|x| Neighbour::new(x.entry.peer_id, x.expires_at(ttl), &default_roundtrip))
                    .collect::<Vec<_>>()
            };
            self.neighbours.update(neighbours);
        }
    }

    #[tracing::instrument(name = "apply_score", skip_all)]
    async fn apply_score_task(self) {
        tracing::info!("started");
        scopeguard::defer! { tracing::info!("finished"); };

        let mut interval = tokio::time::interval(self.config.neighbors.apply_score_interval);

        loop {
            interval.tick().await;

            let now = tycho_util::time::now_sec();
            let applied = self.neighbours.try_apply_score(now);
            tracing::debug!(now, applied, "tried to apply neighbours score");
        }
    }

    #[tracing::instrument(name = "cleanup_neighbours", skip_all)]
    async fn cleanup_neighbours_task(self) {
        tracing::info!("started");
        scopeguard::defer! { tracing::info!("finished"); };

        loop {
            self.overlay.entries_removed().notified().await;

            let now = tycho_util::time::now_sec();
            let applied = self.neighbours.try_apply_score(now);
            tracing::debug!(
                now,
                applied,
                "tried to apply neighbours score after some overlay entry was removed"
            );
        }
    }

    async fn send<R>(&self, data: R) -> Result<(), Error>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.neighbours.choose() else {
            return Err(Error::NoNeighbours);
        };

        self.send_impl(neighbour, Request::from_tl(data)).await
    }

    async fn send_to_validator(&self, validator: Validator, data: Request) -> Result<(), Error> {
        let res = self
            .overlay
            .send(&self.network, &validator.peer_id(), data)
            .await;
        res.map_err(Error::NetworkError)
    }

    async fn query<R, A>(&self, data: R) -> Result<QueryResponse<A>, Error>
    where
        R: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        let Some(neighbour) = self.neighbours.choose() else {
            return Err(Error::NoNeighbours);
        };

        self.query_impl(neighbour, Request::from_tl(data))
            .await?
            .parse()
    }

    async fn send_impl(&self, neighbour: Neighbour, req: Request) -> Result<(), Error> {
        let started_at = Instant::now();

        let res = tokio::time::timeout(
            self.config.neighbors.send_timeout,
            self.overlay.send(&self.network, neighbour.peer_id(), req),
        )
        .await;

        let roundtrip = started_at.elapsed() * 2; // Multiply by 2 to estimate the roundtrip time

        match res {
            Ok(response) => {
                neighbour.track_request(&roundtrip, response.is_ok());

                if let Err(e) = &response {
                    apply_network_error(e, &neighbour);
                }

                response.map_err(Error::NetworkError)
            }
            Err(_) => {
                neighbour.track_request(&roundtrip, false);
                neighbour.punish(PunishReason::Slow);
                Err(Error::Timeout)
            }
        }
    }

    async fn query_impl(
        &self,
        neighbour: Neighbour,
        req: Request,
    ) -> Result<QueryResponse<Bytes>, Error> {
        let started_at = Instant::now();

        let res = tokio::time::timeout(
            self.config.neighbors.query_timeout,
            self.overlay.query(&self.network, neighbour.peer_id(), req),
        )
        .await;

        let roundtrip = started_at.elapsed();

        match res {
            Ok(Ok(response)) => Ok(QueryResponse {
                data: response.body,
                roundtrip_ms: roundtrip.as_millis() as u64,
                neighbour,
            }),
            Ok(Err(e)) => {
                neighbour.track_request(&roundtrip, false);
                apply_network_error(&e, &neighbour);
                Err(Error::NetworkError(e))
            }
            Err(_) => {
                neighbour.track_request(&roundtrip, false);
                neighbour.punish(PunishReason::Slow);
                Err(Error::Timeout)
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

    pub fn split(self) -> (QueryResponseHandle, A) {
        let handle = QueryResponseHandle::with_roundtrip_ms(self.neighbour, self.roundtrip_ms);
        (handle, self.data)
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

impl QueryResponse<Bytes> {
    pub fn parse<A>(self) -> Result<QueryResponse<A>, Error>
    where
        for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        let response = match tl_proto::deserialize::<overlay::Response<A>>(&self.data) {
            Ok(r) => r,
            Err(e) => {
                self.reject();
                return Err(Error::InvalidResponse(e));
            }
        };

        match response {
            overlay::Response::Ok(data) => Ok(QueryResponse {
                data,
                roundtrip_ms: self.roundtrip_ms,
                neighbour: self.neighbour,
            }),
            overlay::Response::Err(code) => {
                self.reject();
                Err(Error::RequestFailed(code))
            }
        }
    }
}

pub struct QueryResponseHandle {
    neighbour: Neighbour,
    roundtrip_ms: u64,
}

impl QueryResponseHandle {
    pub fn with_roundtrip_ms(neighbour: Neighbour, roundtrip_ms: u64) -> Self {
        Self {
            neighbour,
            roundtrip_ms,
        }
    }

    pub fn accept(self) -> Neighbour {
        self.track_request(true);
        self.neighbour
    }

    pub fn reject(self) -> Neighbour {
        self.track_request(false);
        self.neighbour
    }

    fn track_request(&self, success: bool) {
        self.neighbour
            .track_request(&Duration::from_millis(self.roundtrip_ms), success);
    }
}

fn apply_network_error(error: &anyhow::Error, neighbour: &Neighbour) {
    // NOTE: `(*error)` is a non-recurisve downcast
    let Some(error) = (*error).downcast_ref() else {
        if let Some(UnknownPeerError { .. }) = (*error).downcast_ref() {
            neighbour.punish(PunishReason::Malicious);
        }

        // TODO: Handle other errors as well
        return;
    };

    match error {
        ConnectionError::InvalidAddress | ConnectionError::InvalidCertificate => {
            neighbour.punish(PunishReason::Malicious);
        }
        ConnectionError::Timeout => {
            neighbour.punish(PunishReason::Slow);
        }
        _ => {}
    }
}
