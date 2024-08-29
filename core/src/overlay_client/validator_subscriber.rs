use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use everscale_types::models::ValidatorSet;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use indexmap::IndexMap;
use parking_lot::RwLock;
use rand::prelude::IteratorRandom;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::AbortHandle;
use tycho_network::{KnownPeerHandle, PeerId, PeerResolver};

use crate::block_strider::{BlockSubscriber, BlockSubscriberContext};

struct Inner {
    validators: Validators,
    task_sender: UnboundedSender<Vec<PeerId>>,

    resolver_worker_handle: AbortHandle,
}

#[derive(Clone)]
pub struct ValidatorSubscriber {
    inner: Arc<Inner>,
}

impl Drop for ValidatorSubscriber {
    fn drop(&mut self) {
        self.inner.resolver_worker_handle.abort();
    }
}

impl ValidatorSubscriber {
    pub fn new(peer_resolver: PeerResolver) -> Self {
        let (task_sender, task_receiver) = tokio::sync::mpsc::unbounded_channel();

        let validators = Validators::new();

        let resolver_worker_handle = tokio::spawn(Self::start_listening_for_new_peers(
            validators.clone(),
            task_receiver,
            peer_resolver.clone(),
        ));

        Self {
            inner: Arc::new(Inner {
                validators,
                task_sender,

                resolver_worker_handle: resolver_worker_handle.abort_handle(),
            }),
        }
    }

    pub fn send_validators(&self, peers: Vec<PeerId>) {
        if let Err(e) = self.inner.task_sender.send(peers) {
            tracing::error!("Failed to send validators {e:?}");
        }
    }

    async fn start_listening_for_new_peers(
        current: Validators,
        mut new_peers: UnboundedReceiver<Vec<PeerId>>,
        peer_resolver: PeerResolver,
    ) {
        let mut current_peers: Option<Vec<PeerId>> = None;

        loop {
            tokio::select! {
                maybe_peers = new_peers.recv() => {
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

                    Self::process(peers, current.clone(), peer_resolver.clone()).await;

                } => ()
            }
        }
    }

    async fn process(peers: Vec<PeerId>, current: Validators, peer_resolver: PeerResolver) {
        let mut resolved = FuturesUnordered::new();

        for p in peers {
            let pr_handle = peer_resolver.insert(&p, false);
            resolved.push(async move { pr_handle.wait_resolved().await });
        }

        while let Some(r) = resolved.next().await {
            let info = r.load_peer_info().id;
            let mut guard = current.current.write();
            guard.insert(info, Validator {
                inner: Arc::new(ValidatorInner { peer_info: r }),
            });
        }
    }

    pub fn get_random_validators(&self, amount: usize) -> Vec<Validator> {
        let mut candidates = Vec::new();

        let guard = self.inner.validators.current.read();
        let active = guard
            .values()
            .choose_multiple(&mut rand::thread_rng(), amount);

        for i in active {
            candidates.push(i.clone());
        }
        candidates
    }

    pub fn update_current_validator_set(&self, vset: ValidatorSet) {
        let mut guard = self.inner.validators.current.write();

        let new_peers = vset
            .list
            .iter()
            .map(|x| PeerId(x.public_key.0))
            .collect::<HashSet<_>>();

        guard.retain(|key, _| new_peers.contains(key));

        let mut peers_to_resolve = Vec::new();

        for peer in new_peers {
            peers_to_resolve.push(peer);
        }

        if let Err(e) = self.inner.task_sender.send(peers_to_resolve) {
            tracing::error!("failed to sent peer resolve task {e:?}");
        }
    }
}

impl BlockSubscriber for ValidatorSubscriber {
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
                tracing::error!("Failed to load mc_extra. {e:?}");
                None
            }
        };

        let config = config.context("No config in keyblock").unwrap();

        match config.get_current_validator_set() {
            Ok(vset) => self.update_current_validator_set(vset),
            Err(e) => {
                tracing::error!("Failed to get validator set from blockchain config. {e:?}");
            }
        }

        futures_util::future::ready(Ok(()))
    }
}

#[derive(Clone)]
pub struct Validator {
    inner: Arc<ValidatorInner>,
}

struct ValidatorInner {
    peer_info: KnownPeerHandle,
}

impl Validator {
    pub fn peer_id(&self) -> PeerId {
        self.inner.peer_info.peer_info().id
    }
}

#[derive(Clone)]
pub struct Validators {
    current: Arc<RwLock<IndexMap<PeerId, Validator>>>,
}

impl Validators {
    pub fn new() -> Self {
        Self {
            current: Arc::new(RwLock::new(IndexMap::new())),
        }
    }
}
