use std::borrow::Borrow;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use everscale_types::models::ValidatorSet;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tycho_network::{KnownPeerHandle, PeerId, PeerResolver};
use tycho_util::FastHashSet;

use crate::block_strider::{BlockSubscriber, BlockSubscriberContext};

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

impl Drop for ValidatorsResolver {
    fn drop(&mut self) {
        self.inner.resolver_worker_handle.abort();
    }
}

impl ValidatorsResolver {
    pub fn new(peer_resolver: Option<PeerResolver>) -> Self {
        let (peers_tx, peers_rx) = tokio::sync::mpsc::unbounded_channel();

        let validators = Validators::default();

        let resolver_worker_handle = tokio::spawn({
            let validators = validators.clone();
            async move {
                if let Some(peer_resolver) = peer_resolver {
                    validators.listen(peers_rx, peer_resolver).await;
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

    pub fn choose_multiple(&self, n: usize) -> Vec<Validator> {
        use rand::seq::SliceRandom;

        let current = self.inner.validators.current.load();
        current
            .choose_multiple(&mut rand::thread_rng(), n)
            .cloned()
            .collect()
    }
}

impl BlockSubscriber for ValidatorsResolver {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
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

        let config = match cx.block.load_custom() {
            Ok(extra) => extra.config,
            Err(e) => {
                tracing::error!("failed to load mc_extra: {e:?}");
                None
            }
        };

        let config = config.context("No config in keyblock").unwrap();

        match config.get_current_validator_set() {
            Ok(vset) => self.update_validator_set(&vset),
            Err(e) => {
                tracing::error!("failed to get validator set from blockchain config: {e:?}");
            }
        }

        futures_util::future::ready(Ok(()))
    }
}

struct Inner {
    validators: Validators,
    peers_tx: PeersTx,

    resolver_worker_handle: AbortHandle,
}

impl Drop for Inner {
    fn drop(&mut self) {
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
}

#[derive(Clone, Default)]
struct Validators {
    current: Arc<ArcSwap<Vec<Validator>>>,
}

impl Validators {
    async fn listen(&self, mut peers_rx: PeersRx, peer_resolver: PeerResolver) {
        let mut current_peers = None;

        loop {
            tokio::select! {
                maybe_peers = peers_rx.recv() => {
                    match maybe_peers {
                        Some(peers) => {
                            current_peers = Some(peers);
                        }
                        None => break,
                    }
                }
                _ = async {
                    let Some(peers) = current_peers.take() else {
                        futures_util::future::pending().await
                    };
                    self.process(peers, &peer_resolver).await;
                } => {}
            }
        }
    }

    async fn process(&self, mut peers: FastHashSet<PeerId>, peer_resolver: &PeerResolver) {
        tracing::debug!(?peers, "started resolving validators");

        // Remove us from the list of validators
        peers.remove(peer_resolver.dht_service().local_id());

        // Remove old validators and skip existing ones
        {
            let current = self.current.load_full();
            let mut changed = false;
            let current = current
                .iter()
                .filter(|validator| {
                    let retain = peers.remove(&validator.inner.handle.peer_info().id);
                    changed |= !retain;
                    retain
                })
                .cloned()
                .collect::<Vec<_>>();

            if changed {
                self.current.store(Arc::new(current));
            }
        }

        // Resolve all remaining new peers
        let mut resolved = FuturesUnordered::new();
        for peer_id in peers {
            let peer = peer_resolver.insert(&peer_id, false);
            resolved.push(async move { peer.wait_resolved().await });
        }

        // TODO: Add metrics (gauge) for the number of currently resolved/unresolved validators

        while let Some(handle) = resolved.next().await {
            let peer_id = handle.peer_info().id;
            tracing::debug!(%peer_id, "resolved validator");

            let mut current = self.current.load_full();
            Arc::make_mut(&mut current).push(Validator {
                inner: Arc::new(ValidatorInner { handle }),
            });
            self.current.store(current);
        }

        tracing::debug!("resolved all validators");
    }
}

type PeersTx = mpsc::UnboundedSender<FastHashSet<PeerId>>;
type PeersRx = mpsc::UnboundedReceiver<FastHashSet<PeerId>>;
