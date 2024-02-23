use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use anyhow::Result;
use bytes::Buf;
use futures_util::{Stream, StreamExt};
use tl_proto::{TlError, TlRead};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::time::{now_sec, shifted_interval};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use crate::network::{Network, WeakNetwork};
use crate::proto::overlay::{rpc, PublicEntriesResponse, PublicEntry, PublicEntryToSign};
use crate::types::{PeerId, Request, Response, Service, ServiceRequest};
use crate::util::{NetworkExt, Routable};

pub use self::config::OverlayConfig;
pub use self::overlay_id::OverlayId;
pub use self::private_overlay::{
    PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries, PrivateOverlayEntriesReadGuard,
    PrivateOverlayEntriesWriteGuard,
};
pub use self::public_overlay::{
    PublicOverlay, PublicOverlayBuilder, PublicOverlayEntries, PublicOverlayEntriesReadGuard,
};

mod config;
mod overlay_id;
mod private_overlay;
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
    private_overlays: FastDashMap<OverlayId, PrivateOverlay>,
    public_overlays: FastDashMap<OverlayId, PublicOverlay>,
}

impl OverlayServiceBuilder {
    pub fn with_config(mut self, config: OverlayConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_private_overlay(self, overlay: PrivateOverlay) -> Self {
        assert!(
            !self.public_overlays.contains_key(overlay.overlay_id()),
            "public overlay with id {} already exists",
            overlay.overlay_id()
        );

        let prev = self.private_overlays.insert(*overlay.overlay_id(), overlay);
        if let Some(prev) = prev {
            panic!(
                "private overlay with id {} already exists",
                prev.overlay_id()
            );
        }
        self
    }

    pub fn with_public_overlay(self, overlay: PublicOverlay) -> Self {
        assert!(
            !self.private_overlays.contains_key(overlay.overlay_id()),
            "private overlay with id {} already exists",
            overlay.overlay_id()
        );

        let prev = self.public_overlays.insert(*overlay.overlay_id(), overlay);
        if let Some(prev) = prev {
            panic!(
                "public overlay with id {} already exists",
                prev.overlay_id()
            );
        }
        self
    }

    pub fn build(self) -> (OverlayServiceBackgroundTasks, OverlayService) {
        let config = self.config.unwrap_or_default();

        let inner = Arc::new(OverlayServiceInner {
            local_id: self.local_id,
            config,
            private_overlays: self.private_overlays,
            public_overlays: self.public_overlays,
            public_overlays_changed: Arc::new(Notify::new()),
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
            private_overlays: Default::default(),
            public_overlays: Default::default(),
        }
    }

    pub fn try_add_private_overlay(&self, overlay: PrivateOverlay) -> Result<(), PrivateOverlay> {
        self.0.try_add_private_overlay(overlay)
    }

    pub fn try_add_public_overlay(&self, overlay: PublicOverlay) -> Result<(), PublicOverlay> {
        self.0.try_add_public_overlay(overlay)
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

            if let Some(private_overlay) = self.0.private_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(private_overlay.service().on_query(req));
            } else if let Some(public_overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(public_overlay.service().on_query(req));
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

            if let Some(private_overlay) = self.0.private_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(private_overlay.service().on_message(req));
            } else if let Some(public_overlay) = self.0.public_overlays.get(overlay_id) {
                req.body.advance(offset);
                return BoxFutureOrNoop::future(public_overlay.service().on_message(req));
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
    public_overlays: FastDashMap<OverlayId, PublicOverlay>,
    private_overlays: FastDashMap<OverlayId, PrivateOverlay>,
    public_overlays_changed: Arc<Notify>,
}

impl OverlayServiceInner {
    fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork) {
        enum Action<'a> {
            UpdatePublicOverlaysList {
                exchange_state: &'a mut ExchangeState,
            },
            ExchangePublicEntries {
                exchange_state: &'a mut ExchangeState,
                overlay_id: OverlayId,
            },
        }

        #[derive(Default)]
        struct ExchangeState {
            stream: PublicOverlayActionsStream,
            futures: FastHashMap<OverlayId, Option<JoinHandle<()>>>,
        }

        let public_overlays_notify = self.public_overlays_changed.clone();

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background overlay loop started");

            let mut exchange_state = None::<ExchangeState>;
            let mut public_overlays_changed = Box::pin(public_overlays_notify.notified());

            loop {
                let action = match &mut exchange_state {
                    // Initial update
                    None => Action::UpdatePublicOverlaysList {
                        exchange_state: exchange_state.get_or_insert_with(Default::default),
                    },
                    // Default actions
                    Some(exchange_state) => {
                        tokio::select! {
                            _ = &mut public_overlays_changed => {
                                public_overlays_changed = Box::pin(public_overlays_notify.notified());
                                Action::UpdatePublicOverlaysList {
                                    exchange_state
                                }
                            },
                            overlay_id = exchange_state.stream.next() => match overlay_id {
                                Some(id) => Action::ExchangePublicEntries {
                                    exchange_state,
                                    overlay_id: id
                                },
                                None => continue,
                            }
                        }
                    }
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::UpdatePublicOverlaysList { exchange_state } => exchange_state.stream.rebuild(
                        this.public_overlays.iter().map(|item| *item.key()),
                        |_| {
                            shifted_interval(
                                this.config.public_overlay_peer_exchange_period,
                                this.config.public_overlay_peer_exchange_max_jitter,
                            )
                        },
                        |overlay_id| {
                            if let Some(fut) = exchange_state.futures.remove(overlay_id).flatten() {
                                tracing::debug!(%overlay_id, "cancelling exchange public entries task");
                                fut.abort();
                            }
                        },
                    ),
                    Action::ExchangePublicEntries { exchange_state, overlay_id } => {
                        let fut_entry = exchange_state.futures.entry(overlay_id).or_default();

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

    pub fn try_add_private_overlay(&self, overlay: PrivateOverlay) -> Result<(), PrivateOverlay> {
        use dashmap::mapref::entry::Entry;

        if self.public_overlays.contains_key(overlay.overlay_id()) {
            return Err(overlay);
        }
        match self.private_overlays.entry(*overlay.overlay_id()) {
            Entry::Vacant(entry) => {
                entry.insert(overlay);
                Ok(())
            }
            Entry::Occupied(_) => Err(overlay),
        }
    }

    pub fn try_add_public_overlay(&self, overlay: PublicOverlay) -> Result<(), PublicOverlay> {
        use dashmap::mapref::entry::Entry;

        if self.private_overlays.contains_key(overlay.overlay_id()) {
            return Err(overlay);
        }
        match self.public_overlays.entry(*overlay.overlay_id()) {
            Entry::Vacant(entry) => {
                entry.insert(overlay);
                self.public_overlays_changed.notify_waiters();
                Ok(())
            }
            Entry::Occupied(_) => Err(overlay),
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

#[derive(Default)]
struct PublicOverlayActionsStream {
    intervals: Vec<(tokio::time::Interval, OverlayId)>,
    waker: Option<Waker>,
}

impl PublicOverlayActionsStream {
    fn rebuild<I: Iterator<Item = OverlayId>, A, R>(
        &mut self,
        iter: I,
        mut on_add: A,
        mut on_remove: R,
    ) where
        for<'a> A: FnMut(&'a OverlayId) -> tokio::time::Interval,
        for<'a> R: FnMut(&'a OverlayId),
    {
        let mut new_overlays = iter.collect::<FastHashSet<_>>();
        self.intervals.retain(|(_, id)| {
            let retain = new_overlays.remove(id);
            if !retain {
                on_remove(id);
            }
            retain
        });

        for id in new_overlays {
            self.intervals.push((on_add(&id), id));
        }

        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl Stream for PublicOverlayActionsStream {
    type Item = OverlayId;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Always register the waker to resume the stream even if there were
        // changes in the intervals.
        if !matches!(&self.waker, Some(waker) if cx.waker().will_wake(waker)) {
            self.waker = Some(cx.waker().clone());
        }

        for (interval, data) in self.intervals.iter_mut() {
            if interval.poll_tick(cx).is_ready() {
                return Poll::Ready(Some(*data));
            }
        }

        Poll::Pending
    }
}
