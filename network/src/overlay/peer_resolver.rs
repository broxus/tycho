use std::collections::{hash_map, BTreeMap};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use exponential_backoff::Backoff;
use futures_util::future::{select, Either};
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;
use tycho_util::futures::JoinTask;
use tycho_util::time::now_sec;
use tycho_util::{FastDashMap, FastHashMap};

use crate::dht::DhtService;
use crate::network::{KnownPeerHandle, Network, WeakKnownPeerHandle, WeakNetwork};
use crate::proto::dht;
use crate::types::{PeerId, PeerInfo};

pub struct PeerResolver {
    tasks: FastDashMap<PeerId, Weak<JoinTask<()>>>,
    shared: Arc<PeerResolverShared>,
}

impl PeerResolver {
    pub fn insert(&self, handle: &KnownPeerHandle) -> PeerResolverHandle {
        use dashmap::mapref::entry::Entry;

        match self.tasks.entry(handle.peer_info().id) {
            Entry::Vacant(entry) => {
                let inner = self.spawn(handle);
                entry.insert(Arc::downgrade(&inner));
                PeerResolverHandle { inner }
            }
            Entry::Occupied(mut entry) => match entry.get().upgrade() {
                Some(inner) => PeerResolverHandle { inner },
                None => {
                    let inner = self.spawn(handle);
                    entry.insert(Arc::downgrade(&inner));
                    PeerResolverHandle { inner }
                }
            },
        }
    }

    fn spawn(&self, handle: &KnownPeerHandle) -> Arc<JoinTask<()>> {
        let weak = handle.downgrade();
        let shared = self.shared.clone();
        Arc::new(JoinTask::new(async move {
            loop {
                let Some(weak) = weak.upgrade() else {
                    return;
                };

                let mut backoff =
                    Backoff::new(10, Duration::from_millis(100), Duration::from_secs(10));
                backoff.set_jitter(0.1);

                for duration in &backoff {
                    tokio::time::sleep(duration).await;
                }
            }
        }))
    }
}

struct PeerResolverShared {
    weak_network: WeakNetwork,
    dht_service: DhtService,
    fast_retry_count: u32,
    min_retry_interval: Duration,
    max_retry_interval: Duration,
    stale_retry_interval: Duration,
}

impl PeerResolverShared {
    async fn make_resolver_handle(self: &Arc<Self>, peer_id: &PeerId) -> PeerResolverHandle {
        let handle = match self.weak_network.upgrade() {
            Some(handle) => handle.known_peers().make_handle(peer_id, false),
            None => return PeerResolverHandle::noop(),
        };

        let mut next_update_at = handle.as_ref().map(|handle| handle.peer_info().expires_at);

        let data = Arc::new(PeerResolverHandleData {
            handle: Mutex::new(handle),
            is_stale: AtomicBool::new(false),
        });

        PeerResolverHandle(Arc::new(PeerResolverHandleInner {
            task: JoinTask::new({
                let peer_id = *peer_id;
                let shared = self.clone();
                let handle = handle.clone();
                let data = data.clone();
                async move {
                    if !is_resolved {
                        let peer_info = shared.resolve_peer(&peer_id, &data.is_stale).await;
                    }
                }
            }),
            data,
        }))
    }

    async fn resolve_peer(
        &self,
        peer_id: &PeerId,
        is_stale: &AtomicBool,
    ) -> Option<(Network, Arc<PeerInfo>)> {
        struct Iter<'a> {
            backoff: Option<exponential_backoff::Iter<'a>>,
            is_stale: &'a AtomicBool,
            stale_retry_interval: &'a Duration,
        }

        impl Iterator for Iter<'_> {
            type Item = Duration;

            fn next(&mut self) -> Option<Self::Item> {
                Some(loop {
                    match self.backoff.as_mut() {
                        // Get next duration from the backoff iterator.
                        Some(backoff) => match backoff.next() {
                            // Use it for the first attempts.
                            Some(duration) => break duration,
                            // Set `is_stale` flag on last attempt and continue wih only
                            // the `stale_retry_interval` for all subsequent iterations.
                            None => {
                                self.is_stale.store(true, Ordering::Release);
                                self.backoff = None;
                            }
                        },
                        // Use `stale_retry_interval` after the max retry count is reached.
                        None => break *self.stale_retry_interval,
                    }
                })
            }
        }

        let backoff = Backoff::new(
            self.fast_retry_count,
            self.min_retry_interval,
            Some(self.max_retry_interval),
        );
        let mut iter = Iter {
            backoff: Some(backoff.iter()),
            is_stale,
            stale_retry_interval: &self.stale_retry_interval,
        };

        // "Fast" path
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let is_stale = attempts > self.fast_retry_count as usize;

            // NOTE: Acquire network ref only during the operation.
            {
                let network = self.weak_network.upgrade()?;
                let dht_client = self.dht_service.make_client(network.clone());

                let res = dht_client
                    .entry(dht::PeerValueKeyName::NodeInfo)
                    .find_value::<PeerInfo>(peer_id)
                    .await;

                let now = now_sec();
                match res {
                    // TODO: Should we move signature check into the `spawn_blocking`?
                    Ok(peer_info) if peer_info.id == peer_id && peer_info.is_valid(now) => {
                        return Some((network, Arc::new(peer_info)));
                    }
                    Ok(_) => {
                        tracing::trace!(
                            %peer_id,
                            attempts,
                            is_stale,
                            "received an invalid peer info",
                        );
                    }
                    Err(e) => {
                        tracing::trace!(
                            %peer_id,
                            attempts,
                            is_stale,
                            "failed to resolve a peer info: {e:?}",
                        );
                    }
                }
            }

            let interval = iter.next().expect("retries iterator must be infinite");
            tokio::time::sleep(interval).await;
        }
    }
}

#[derive(Clone)]
pub struct PeerResolverHandle(Arc<PeerResolverHandleInner>);

impl PeerResolverHandle {
    fn noop() -> Self {
        static HANDLE: OnceLock<PeerResolverHandle> = OnceLock::new();
        HANDLE
            .get_or_init(|| {
                PeerResolverHandle(Arc::new(PeerResolverHandleInner {
                    task: JoinTask::new(std::future::ready(())),
                    data: Default::default(),
                }))
            })
            .clone()
    }

    pub fn is_resolved(&self) -> bool {
        self.0.data.handle.lock().unwrap().is_some()
    }

    pub fn load_handle(&self) -> Option<KnownPeerHandle> {
        self.0.data.handle.lock().unwrap().clone()
    }

    pub fn is_stale(&self) -> bool {
        self.0.data.is_stale.load(Ordering::Acquire)
    }
}

struct PeerResolverHandleInner {
    task: JoinTask<()>,
    data: Arc<PeerResolverHandleData>,
}

#[derive(Default)]
struct PeerResolverHandleData {
    handle: Mutex<Option<KnownPeerHandle>>,
    is_stale: AtomicBool,
}
