use std::borrow::Borrow;
use std::sync::Arc;
use arc_swap::ArcSwap;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tycho_network::{
    KnownPeerHandle, Network, PeerId, PeerResolver, PublicOverlay, Request,
};
use tycho_types::models::ValidatorSet;
use tycho_util::FastHashSet;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use crate::block_strider::{BlockSubscriber, BlockSubscriberContext};
use crate::overlay_client::config::ValidatorsConfig;
use crate::proto::overlay;
pub trait ValidatorSetPeers {
    fn get_peers(&self) -> FastHashSet<PeerId>;
}
impl ValidatorSetPeers for ValidatorSet {
    fn get_peers(&self) -> FastHashSet<PeerId> {
        self.list.iter().map(|x| PeerId(x.public_key.0)).collect()
    }
}
impl<T: Borrow<PeerId>> ValidatorSetPeers for [T] {
    fn get_peers(&self) -> FastHashSet<PeerId> {
        self.iter().map(|x| *x.borrow()).collect()
    }
}
impl<T: Borrow<PeerId>> ValidatorSetPeers for Vec<T> {
    fn get_peers(&self) -> FastHashSet<PeerId> {
        self.iter().map(|x| *x.borrow()).collect()
    }
}
impl ValidatorSetPeers for FastHashSet<PeerId> {
    fn get_peers(&self) -> FastHashSet<PeerId> {
        self.clone()
    }
}
#[derive(Clone)]
pub struct ValidatorsResolver {
    inner: Arc<Inner>,
}
impl ValidatorsResolver {
    pub fn new(
        network: Network,
        overlay: PublicOverlay,
        config: ValidatorsConfig,
    ) -> Self {
        let (peers_tx, peers_rx) = mpsc::unbounded_channel();
        let peer_resolver = overlay.peer_resolver().clone();
        let validators = Arc::new(Validators {
            config,
            resolved: Default::default(),
            targets: Default::default(),
            network,
            overlay,
            current_epoch: Default::default(),
            target_validators_gauge: metrics::gauge!(
                "tycho_core_overlay_client_target_validators"
            ),
            resolved_validators_gauge: metrics::gauge!(
                "tycho_core_overlay_client_resolved_validators"
            ),
        });
        let resolver_worker_handle = tokio::spawn({
            let validators = validators.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    73u32,
                );
                if let Some(peer_resolver) = peer_resolver {
                    {
                        __guard.end_section(75u32);
                        let __result = validators.listen(peers_rx, peer_resolver).await;
                        __guard.start_section(75u32);
                        __result
                    };
                }
            }
        });
        Self {
            inner: Arc::new(Inner {
                validators,
                peers_tx,
                resolver_worker_handle: resolver_worker_handle.abort_handle(),
            }),
        }
    }
    pub fn update_validator_set<T: ValidatorSetPeers>(&self, vset: &T) {
        let new_peers = vset.get_peers();
        self.inner.peers_tx.send(new_peers).ok();
    }
    pub fn get_broadcast_targets(&self) -> Arc<Vec<Validator>> {
        self.inner.validators.targets.load_full()
    }
}
impl BlockSubscriber for ValidatorsResolver {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    fn prepare_block<'a>(
        &'a self,
        _: &'a BlockSubscriberContext,
    ) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }
    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        if !cx.is_key_block {
            return futures_util::future::ready(Ok(()));
        }
        tracing::info!("updating validators");
        let config = match cx.block.load_custom() {
            Ok(extra) => &extra.config,
            Err(e) => {
                return futures_util::future::ready(
                    Err(anyhow::anyhow!("failed to load mc block extra: {e:?}")),
                );
            }
        };
        if let Some(config) = config {
            match config.get_current_validator_set() {
                Ok(vset) => self.update_validator_set(&vset),
                Err(e) => {
                    tracing::error!(
                        "failed to get validator set from blockchain config: {e:?}"
                    );
                }
            }
        }
        futures_util::future::ready(Ok(()))
    }
}
struct Inner {
    validators: Arc<Validators>,
    peers_tx: PeersTx,
    resolver_worker_handle: AbortHandle,
}
impl Drop for Inner {
    fn drop(&mut self) {
        tracing::info!("stopping validators resolver");
        self.resolver_worker_handle.abort();
    }
}
#[derive(Clone)]
pub struct Validator {
    inner: Arc<ValidatorInner>,
}
struct ValidatorInner {
    handle: KnownPeerHandle,
}
impl Validator {
    pub fn peer_id(&self) -> PeerId {
        self.inner.handle.peer_info().id
    }
    pub fn is_expired(&self, now: u32) -> bool {
        const NEW_THRESHOLD: u32 = 1800;
        let peer_info = self.inner.handle.peer_info();
        let is_quite_old = peer_info.created_at + NEW_THRESHOLD < now;
        is_quite_old || peer_info.expires_at < now
    }
}
impl std::fmt::Debug for Validator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Validator").field("peer_id", &self.peer_id()).finish()
    }
}
struct Validators {
    config: ValidatorsConfig,
    resolved: ArcSwap<Vec<Validator>>,
    targets: ArcSwap<Vec<Validator>>,
    network: Network,
    overlay: PublicOverlay,
    current_epoch: parking_lot::Mutex<usize>,
    target_validators_gauge: metrics::Gauge,
    resolved_validators_gauge: metrics::Gauge,
}
impl Validators {
    #[tracing::instrument(name = "resolve_validators", skip_all)]
    async fn listen(
        self: &Arc<Self>,
        mut peers_rx: PeersRx,
        peer_resolver: PeerResolver,
    ) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(listen)),
            file!(),
            206u32,
        );
        let mut peers_rx = peers_rx;
        let peer_resolver = peer_resolver;
        tracing::info!("started");
        scopeguard::defer! {
            tracing::info!("finished");
        };
        let mut current_peers = None;
        let local_id = peer_resolver.dht_service().local_id();
        loop {
            __guard.checkpoint(214u32);
            {
                __guard.end_section(215u32);
                let __result = tokio::select! {
                    maybe_peers = peers_rx.recv() => { match maybe_peers { Some(peers) =>
                    { current_peers = Some(peers); } None => break, } } _ = async { let
                    Some(mut peers) = current_peers.take() else {
                    futures_util::future::pending(). await }; let epoch = self
                    .prepare_peers(& mut peers, local_id); let this = self.clone(); let
                    tracker_handle = JoinTask::new(async move { this
                    .track_resolved(epoch). await; }); self.resolve(peers, &
                    peer_resolver). await; tracker_handle. await; } => {}
                };
                __guard.start_section(215u32);
                __result
            }
        }
    }
    fn prepare_peers(
        &self,
        peers: &mut FastHashSet<PeerId>,
        local_id: &PeerId,
    ) -> usize {
        peers.remove(local_id);
        metrics::gauge!("tycho_core_overlay_client_validators_to_resolve")
            .set(peers.len() as f64);
        let epoch = {
            let mut current_epoch = self.current_epoch.lock();
            *current_epoch += 1;
            *current_epoch
        };
        tracing::debug!(epoch, ? peers, "preparing validators");
        {
            let targets = self.targets.load_full();
            let mut changed = false;
            let targets = targets
                .iter()
                .filter(|validator| {
                    let retain = peers.contains(&validator.inner.handle.peer_info().id);
                    tracing::debug!(
                        id = % validator.peer_id(), ? retain, "filtering validator"
                    );
                    changed |= !retain;
                    retain
                })
                .cloned()
                .collect::<Vec<_>>();
            let count = targets.len();
            if changed {
                self.targets.store(Arc::new(targets));
            }
            self.target_validators_gauge.set(count as f64);
        }
        tracing::debug!(epoch, "prepared validators");
        {
            let resolved = self.resolved.load_full();
            tracing::debug!(epoch, ? resolved, ? peers, "resolving validators");
            let mut changed = false;
            let resolved = resolved
                .iter()
                .filter(|validator| {
                    let retain = peers.remove(&validator.inner.handle.peer_info().id);
                    changed |= !retain;
                    retain
                })
                .cloned()
                .collect::<Vec<_>>();
            let count = resolved.len();
            tracing::debug!(epoch, ? resolved, count, "resolved validators");
            if changed {
                self.resolved.store(Arc::new(resolved));
            }
            self.resolved_validators_gauge.set(count as f64);
        }
        epoch
    }
    async fn resolve(&self, peers: FastHashSet<PeerId>, peer_resolver: &PeerResolver) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(resolve)),
            file!(),
            324u32,
        );
        let peers = peers;
        let peer_resolver = peer_resolver;
        tracing::debug!(? peers, "started resolving validators");
        let mut resolved = FuturesUnordered::new();
        for peer_id in peers {
            __guard.checkpoint(329u32);
            let peer = peer_resolver.insert(&peer_id, false);
            resolved
                .push(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        331u32,
                    );
                    {
                        __guard.end_section(331u32);
                        let __result = peer.wait_resolved().await;
                        __guard.start_section(331u32);
                        __result
                    }
                });
        }
        while let Some(handle) = {
            __guard.end_section(334u32);
            let __result = resolved.next().await;
            __guard.start_section(334u32);
            __result
        } {
            __guard.checkpoint(334u32);
            let peer_id = handle.peer_info().id;
            tracing::debug!(% peer_id, "resolved validator");
            let mut resolved = self.resolved.load_full();
            Arc::make_mut(&mut resolved)
                .push(Validator {
                    inner: Arc::new(ValidatorInner { handle }),
                });
            let count = resolved.len();
            self.resolved.store(resolved);
            self.resolved_validators_gauge.set(count as f64);
        }
        tracing::debug!("resolved all validators");
    }
    #[tracing::instrument(skip(self))]
    async fn track_resolved(&self, epoch: usize) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(track_resolved)),
            file!(),
            351u32,
        );
        let epoch = epoch;
        use futures_util::StreamExt;
        use rand::seq::SliceRandom;
        tracing::debug!(epoch, "started resolving peers");
        scopeguard::defer! {
            tracing::debug!(epoch, "finished monitoring resolved validators");
        }
        let request = Request::from_tl(overlay::Ping);
        let max_validators = self.config.keep;
        let mut interval = tokio::time::interval(self.config.ping_interval);
        loop {
            __guard.checkpoint(365u32);
            {
                __guard.end_section(366u32);
                let __result = interval.tick().await;
                __guard.start_section(366u32);
                __result
            };
            let mut resolved = Arc::unwrap_or_clone(self.resolved.load_full());
            let now = tycho_util::time::now_sec();
            resolved.retain(|validator| !validator.is_expired(now));
            resolved.shuffle(&mut rand::rng());
            let spawn_ping = |validator: Validator| {
                let network = self.network.clone();
                let overlay = self.overlay.clone();
                let request = request.clone();
                let ping_timeout = self.config.ping_timeout;
                JoinTask::new(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        384u32,
                    );
                    let _histogram = HistogramGuard::begin(
                        "tycho_core_overlay_client_validator_ping_time",
                    );
                    let peer_id = validator.peer_id();
                    let res = {
                        __guard.end_section(393u32);
                        let __result = tokio::time::timeout(
                                ping_timeout,
                                overlay.query(&network, &peer_id, request),
                            )
                            .await;
                        __guard.start_section(393u32);
                        __result
                    };
                    match res {
                        Ok(Ok(res)) => {
                            match res.parse_tl::<overlay::Pong>() {
                                Ok(_) => Some(validator),
                                Err(e) => {
                                    tracing::debug!(
                                        % peer_id, "received an invalid ping response: {e}"
                                    );
                                    None
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::debug!(% peer_id, "failed to ping validator: {e}");
                            None
                        }
                        Err(_) => {
                            tracing::debug!(
                                % peer_id, "failed to ping validator: timeout"
                            );
                            None
                        }
                    }
                })
            };
            let mut targets = Vec::with_capacity(max_validators);
            let mut resolved = resolved.into_iter();
            let mut futures = resolved
                .by_ref()
                .map(spawn_ping)
                .take(max_validators)
                .collect::<FuturesUnordered<_>>();
            while let Some(res) = {
                __guard.end_section(427u32);
                let __result = futures.next().await;
                __guard.start_section(427u32);
                __result
            } {
                __guard.checkpoint(427u32);
                match res {
                    Some(validator) => {
                        targets.push(validator);
                        if targets.len() >= max_validators {
                            {
                                __guard.end_section(433u32);
                                __guard.start_section(433u32);
                                break;
                            };
                        }
                    }
                    None => {
                        match resolved.next() {
                            Some(validator) => futures.push(spawn_ping(validator)),
                            None => {
                                __guard.end_section(440u32);
                                __guard.start_section(440u32);
                                break;
                            }
                        }
                    }
                }
            }
            let count = targets.len();
            {
                let current_epoch = self.current_epoch.lock();
                if *current_epoch == epoch {
                    self.targets.store(Arc::new(targets));
                } else {
                    {
                        __guard.end_section(454u32);
                        return;
                    };
                }
            }
            self.target_validators_gauge.set(count as f64);
            tracing::info!(epoch, "updated current validators list");
        }
    }
}
type PeersTx = mpsc::UnboundedSender<FastHashSet<PeerId>>;
type PeersRx = mpsc::UnboundedReceiver<FastHashSet<PeerId>>;
