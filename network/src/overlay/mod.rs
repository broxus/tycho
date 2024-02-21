use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use bytes::Buf;
use futures_util::{Stream, StreamExt};
use tl_proto::{TlError, TlRead};
use tokio::task::JoinHandle;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::time::{now_sec, shifted_interval};
use tycho_util::{FastHashMap, FastHashSet};

use crate::network::{Network, WeakNetwork};
use crate::proto::overlay::{rpc, PublicEntriesResponse, PublicEntry, PublicEntryToSign};
use crate::types::{PeerId, Request, Response, Service, ServiceRequest};
use crate::util::{NetworkExt, Routable};

pub use self::config::OverlayConfig;
pub use self::overlay_id::OverlayId;
pub use self::public_overlay::{PublicOverlay, PublicOverlayBuilder};

mod config;
mod overlay_id;
mod public_overlay;

pub struct OverlayServiceBackgroundTasks {
    inner: Arc<OverlayServiceInner>,
}

impl OverlayServiceBackgroundTasks {
    pub fn spawn(self, network: Network) {
        self.inner
            .start_background_tasks(Network::downgrade(&network));
    }
}

pub struct OverlayServiceBuilder {
    local_id: PeerId,
    config: Option<OverlayConfig>,
    public_overlays: FastHashMap<OverlayId, PublicOverlay>,
}

impl OverlayServiceBuilder {
    pub fn with_config(mut self, config: OverlayConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_public_overlay(mut self, overlay: PublicOverlay) -> Self {
        let prev = self.public_overlays.insert(*overlay.overlay_id(), overlay);
        if let Some(prev) = prev {
            panic!("overlay with id {} already exists", prev.overlay_id());
        }
        self
    }

    pub fn build(self) -> (OverlayServiceBackgroundTasks, OverlayService) {
        let config = self.config.unwrap_or_default();

        let inner = Arc::new(OverlayServiceInner {
            local_id: self.local_id,
            config,
            public_overlays: self.public_overlays,
        });

        let background_tasks = OverlayServiceBackgroundTasks {
            inner: inner.clone(),
        };

        (background_tasks, OverlayService(inner))
    }
}

#[derive(Clone)]
pub struct OverlayService(Arc<OverlayServiceInner>);

impl OverlayService {
    pub fn builder(local_id: PeerId) -> OverlayServiceBuilder {
        OverlayServiceBuilder {
            local_id,
            config: None,
            public_overlays: Default::default(),
        }
    }
}

impl Service<ServiceRequest> for OverlayService {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_overlay_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, mut req: ServiceRequest) -> Self::OnQueryFuture {
        let e = 'req: {
            if req.body.len() < 4 {
                break 'req TlError::UnexpectedEof;
            }

            // NOTE: `req.body` is untouched while reading the constructor
            // and `as_ref` here is exactly for that.
            let mut offset = 0;
            let overlay_id = match req.body.as_ref().get_u32_le() {
                rpc::Prefix::TL_ID => match rpc::Prefix::read_from(&req.body, &mut offset) {
                    Ok(rpc::Prefix { overlay_id }) => overlay_id,
                    Err(e) => break 'req e,
                },
                rpc::ExchangeRandomPublicEntries::TL_ID => {
                    let req = match tl_proto::deserialize::<rpc::ExchangeRandomPublicEntries>(
                        &req.body,
                    ) {
                        Ok(req) => req,
                        Err(e) => break 'req e,
                    };
                    tracing::debug!("exchange_random_public_entries");

                    let res = self.0.handle_exchange_public_entries(&req);
                    return BoxFutureOrNoop::future(futures_util::future::ready(Some(
                        Response::from_tl(res),
                    )));
                }
                _ => break 'req TlError::UnknownConstructor,
            };

            if req.body.len() < offset + 4 {
                // Definitely an invalid request (not enough bytes for the constructor)
                break 'req TlError::UnexpectedEof;
            }

            // TODO: search for private overlay too

            if let Some(overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(overlay.service().on_query(req));
            }

            tracing::debug!(
                overlay_id = %OverlayId::wrap(overlay_id),
                "unknown overlay id"
            );
            return BoxFutureOrNoop::Noop;
        };

        tracing::debug!("failed to deserialize query: {e:?}");
        BoxFutureOrNoop::Noop
    }

    #[tracing::instrument(
        level = "debug",
        name = "on_overlay_message",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_message(&self, mut req: ServiceRequest) -> Self::OnMessageFuture {
        // TODO: somehow refactor with one method for both query and message

        let e = 'req: {
            if req.body.len() < 4 {
                break 'req TlError::UnexpectedEof;
            }

            // NOTE: `req.body` is untouched while reading the constructor
            // and `as_ref` here is exactly for that.
            let mut offset = 0;
            let overlay_id = match req.body.as_ref().get_u32_le() {
                rpc::Prefix::TL_ID => match rpc::Prefix::read_from(&req.body, &mut offset) {
                    Ok(rpc::Prefix { overlay_id }) => overlay_id,
                    Err(e) => break 'req e,
                },
                _ => break 'req TlError::UnknownConstructor,
            };

            if req.body.len() < offset + 4 {
                // Definitely an invalid request (not enough bytes for the constructor)
                break 'req TlError::UnexpectedEof;
            }

            // TODO: search for private overlay too

            if let Some(overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(overlay.service().on_message(req));
            }

            tracing::debug!(
                overlay_id = %OverlayId::wrap(overlay_id),
                "unknown overlay id"
            );
            return BoxFutureOrNoop::Noop;
        };

        tracing::debug!("failed to deserialize message: {e:?}");
        BoxFutureOrNoop::Noop
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

impl Routable for OverlayService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::ExchangeRandomPublicEntries::TL_ID, rpc::Prefix::TL_ID]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::Prefix::TL_ID]
    }
}

struct OverlayServiceInner {
    local_id: PeerId,
    config: OverlayConfig,
    public_overlays: FastHashMap<OverlayId, PublicOverlay>,
}

impl OverlayServiceInner {
    fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork) {
        enum Action {
            ExchangePublicEntries(OverlayId),
        }

        let mut exchange_public_entries_intervals = self
            .public_overlays
            .keys()
            .map(|overlay_id| {
                let interval = shifted_interval(
                    self.config.public_overlay_peer_exchange_period,
                    self.config.public_overlay_peer_exchange_max_jitter,
                );
                (interval, *overlay_id)
            })
            .collect::<PublicOverlayActionsStream<_>>();

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background overlay loop started");

            let mut exchange_futures =
                FastHashMap::<OverlayId, Option<JoinHandle<()>>>::with_capacity_and_hasher(
                    exchange_public_entries_intervals.len(),
                    Default::default(),
                );

            loop {
                let action = tokio::select! {
                    overlay_id = exchange_public_entries_intervals.next() => match overlay_id {
                        Some(id) => Action::ExchangePublicEntries(id),
                        None => continue,
                    }
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::ExchangePublicEntries(overlay_id) => {
                        let fut_entry = exchange_futures.entry(overlay_id).or_default();

                        // Wait for the previous exchange to finish.
                        if let Some(fut) = fut_entry.take() {
                            if let Err(e) = fut.await {
                                if e.is_panic() {
                                    std::panic::resume_unwind(e.into_panic());
                                }
                            }
                        }

                        // Spawn a new exchange
                        *fut_entry = Some(tokio::spawn(async move {
                            let res = this.exchange_public_entries(&network, &overlay_id).await;
                            if let Err(e) = res {
                                tracing::error!(%overlay_id, "failed to exchange public entries: {e:?}");
                            };
                        }));
                    }
                }
            }

            tracing::debug!("background overlay loop stopped");
        });
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn exchange_public_entries(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let Some(overlay) = self.public_overlays.get(overlay_id) else {
            anyhow::bail!("overlay not found");
        };

        let n = std::cmp::max(self.config.exchange_public_entries_batch, 1);
        let mut entries = Vec::with_capacity(n);

        // Always include us in the response
        entries.push(Arc::new(self.make_local_public_overlay_entry(
            network,
            overlay_id,
            now_sec(),
        )));

        // Choose a random target to send the request and additional random entries
        let peer_id = {
            let rng = &mut rand::thread_rng();

            let all_entries = overlay.read_entries();
            let mut iter = all_entries.choose_multiple(rng, n);

            // TODO: search for target in known peers. This is a stub which will not work.
            let peer_id = match iter.next() {
                Some(entry) => entry.peer_id,
                None => anyhow::bail!("empty overlay, no peers to exchange entries with"),
            };

            // Add additional random entries to the response
            entries.extend(iter.cloned());

            // Use this peer id for the request
            peer_id
        };

        // Send request
        let response = network
            .query(
                &peer_id,
                Request::from_tl(rpc::ExchangeRandomPublicEntries {
                    overlay_id: overlay_id.to_bytes(),
                    entries,
                }),
            )
            .await?
            .parse_tl::<PublicEntriesResponse>()?;

        // Populate the overlay with the response
        match response {
            PublicEntriesResponse::PublicEntries(entries) => {
                tracing::debug!(
                    %peer_id,
                    count = entries.len(),
                    "received public entries"
                );
                overlay.add_untrusted_entries(&entries);
            }
            PublicEntriesResponse::OverlayNotFound => {
                tracing::debug!(%peer_id, "overlay not found");
            }
        }

        // Done
        Ok(())
    }

    fn make_local_public_overlay_entry(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
        now: u32,
    ) -> PublicEntry {
        let signature = Box::new(network.sign_tl(PublicEntryToSign {
            overlay_id: overlay_id.as_bytes(),
            peer_id: &self.local_id,
            created_at: now,
        }));
        PublicEntry {
            peer_id: self.local_id,
            created_at: now,
            signature,
        }
    }

    fn handle_exchange_public_entries(
        &self,
        req: &rpc::ExchangeRandomPublicEntries,
    ) -> PublicEntriesResponse {
        // NOTE: validation is done in the TL parser.
        debug_assert!(req.entries.len() <= 20);

        // Find the overlay
        let overlay = match self.public_overlays.get(&req.overlay_id) {
            Some(overlay) => overlay,
            None => return PublicEntriesResponse::OverlayNotFound,
        };

        // Add proposed entries to the overlay
        overlay.add_untrusted_entries(&req.entries);

        // Collect proposed entries to exclude from the response
        let requested_ids = req
            .entries
            .iter()
            .map(|id| id.peer_id)
            .collect::<FastHashSet<_>>();

        let entries = {
            let entries = overlay.read_entries();

            // Choose additional random entries to ensure we have enough new entries to send back
            let n = self.config.exchange_public_entries_batch;
            entries
                .choose_multiple(&mut rand::thread_rng(), n + requested_ids.len())
                .filter_map(|entry| {
                    let is_new = !requested_ids.contains(&entry.peer_id);
                    is_new.then(|| entry.clone())
                })
                .take(n)
                .collect::<Vec<_>>()
        };

        PublicEntriesResponse::PublicEntries(entries)
    }
}

struct PublicOverlayActionsStream<T> {
    intervals: Vec<(tokio::time::Interval, T)>,
}

impl<T> PublicOverlayActionsStream<T> {
    fn len(&self) -> usize {
        self.intervals.len()
    }
}

impl<T> FromIterator<(tokio::time::Interval, T)> for PublicOverlayActionsStream<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (tokio::time::Interval, T)>,
    {
        Self {
            intervals: iter.into_iter().collect(),
        }
    }
}

impl<T: Clone> Stream for PublicOverlayActionsStream<T>
where
    (tokio::time::Interval, T): Unpin,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for (interval, data) in self.intervals.iter_mut() {
            if interval.poll_tick(cx).is_ready() {
                return Poll::Ready(Some(data.clone()));
            }
        }

        Poll::Pending
    }
}
