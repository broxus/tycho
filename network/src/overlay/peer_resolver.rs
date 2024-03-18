use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use exponential_backoff::Backoff;
use tycho_util::futures::JoinTask;
use tycho_util::time::now_sec;
use tycho_util::FastDashMap;

use crate::dht::DhtService;
use crate::network::{KnownPeerHandle, KnownPeersError, Network, PeerBannedError, WeakNetwork};
use crate::proto::dht;
use crate::types::{PeerId, PeerInfo};

pub struct PeerResolver {
    tasks: FastDashMap<PeerId, Weak<PeerResolverHandleInner>>,
    shared: Arc<PeerResolverShared>,
}

impl PeerResolver {
    pub fn insert(&self, peer_id: &PeerId) -> PeerResolverHandle {
        use dashmap::mapref::entry::Entry;

        match self.tasks.entry(*peer_id) {
            Entry::Vacant(entry) => {
                let handle = self.shared.make_resolver_handle(peer_id);
                entry.insert(Arc::downgrade(&handle.inner));
                handle
            }
            Entry::Occupied(mut entry) => match entry.get().upgrade() {
                Some(inner) => PeerResolverHandle { inner },
                None => {
                    let handle = self.shared.make_resolver_handle(peer_id);
                    entry.insert(Arc::downgrade(&handle.inner));
                    handle
                }
            },
        }
    }
}

struct PeerResolverShared {
    weak_network: WeakNetwork,
    dht_service: DhtService,
    min_ttl_sec: u32,
    update_before_sec: u32,
    fast_retry_count: u32,
    min_retry_interval: Duration,
    max_retry_interval: Duration,
    stale_retry_interval: Duration,
}

impl PeerResolverShared {
    fn make_resolver_handle(self: &Arc<Self>, peer_id: &PeerId) -> PeerResolverHandle {
        let handle = match self.weak_network.upgrade() {
            Some(handle) => handle.known_peers().make_handle(peer_id, false),
            None => return PeerResolverHandle::noop(peer_id),
        };

        let next_update_at = handle
            .as_ref()
            .map(|handle| self.compute_update_at(&handle.peer_info()));

        let data = Arc::new(PeerResolverHandleData {
            peer_id: *peer_id,
            handle: Mutex::new(handle),
            is_stale: AtomicBool::new(false),
        });

        PeerResolverHandle {
            inner: Arc::new(PeerResolverHandleInner {
                task: JoinTask::new(self.clone().run_task(data.clone(), next_update_at)),
                data,
            }),
        }
    }

    async fn run_task(
        self: Arc<Self>,
        data: Arc<PeerResolverHandleData>,
        mut next_update_at: Option<u32>,
    ) {
        tracing::trace!(peer_id = %data.peer_id, "peer resolver task started");

        // TODO: Select between the loop body and `KnownPeers` update event.
        loop {
            // Wait if needed.
            if let Some(update_at) = next_update_at {
                let update_at = std::time::UNIX_EPOCH + Duration::from_secs(update_at as u64);
                let now = std::time::SystemTime::now();
                if let Ok(remaining) = update_at.duration_since(now) {
                    tokio::time::sleep(remaining).await;
                }
            }

            // Start resolving peer.
            match self.resolve_peer(&data.peer_id, &data.is_stale).await {
                Some((network, peer_info)) => {
                    let mut handle = data.handle.lock().unwrap();

                    let peer_info_guard;
                    let peer_info = match &*handle {
                        // TODO: Force write into known peers to keep the handle in it?
                        Some(handle) => match handle.update_peer_info(&peer_info) {
                            Ok(()) => peer_info.as_ref(),
                            Err(KnownPeersError::OutdatedInfo) => {
                                peer_info_guard = handle.peer_info();
                                peer_info_guard.as_ref()
                            }
                            // TODO: Allow resuming task after ban?
                            Err(KnownPeersError::PeerBanned(PeerBannedError)) => break,
                        },
                        None => match network
                            .known_peers()
                            .insert_allow_outdated(peer_info, false)
                        {
                            Ok(new_handle) => {
                                peer_info_guard = handle.insert(new_handle).peer_info();
                                peer_info_guard.as_ref()
                            }
                            // TODO: Allow resuming task after ban?
                            Err(PeerBannedError) => break,
                        },
                    };

                    next_update_at = Some(self.compute_update_at(peer_info));
                }
                None => break,
            }
        }

        tracing::trace!(peer_id = %data.peer_id, "peer resolver task finished");
    }

    /// Returns a verified peer info with the strong reference to the network.
    /// Or `None` if network no longer exists.
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

    fn compute_update_at(&self, peer_info: &PeerInfo) -> u32 {
        let real_ttl = peer_info
            .expires_at
            .saturating_sub(self.update_before_sec)
            .saturating_sub(peer_info.created_at);

        let adjusted_ttl = std::cmp::max(real_ttl, self.min_ttl_sec);
        peer_info.created_at.saturating_add(adjusted_ttl)
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct PeerResolverHandle {
    inner: Arc<PeerResolverHandleInner>,
}

impl PeerResolverHandle {
    fn noop(peer_id: &PeerId) -> Self {
        PeerResolverHandle {
            inner: Arc::new(PeerResolverHandleInner {
                task: JoinTask::new(std::future::ready(())),
                data: Arc::new(PeerResolverHandleData {
                    peer_id: *peer_id,
                    handle: Mutex::new(None),
                    is_stale: AtomicBool::new(false),
                }),
            }),
        }
    }

    pub fn is_resolved(&self) -> bool {
        self.inner.data.handle.lock().unwrap().is_some()
    }

    pub fn load_handle(&self) -> Option<KnownPeerHandle> {
        self.inner.data.handle.lock().unwrap().clone()
    }

    pub fn is_stale(&self) -> bool {
        self.inner.data.is_stale.load(Ordering::Acquire)
    }
}

struct PeerResolverHandleInner {
    task: JoinTask<()>,
    data: Arc<PeerResolverHandleData>,
}

struct PeerResolverHandleData {
    peer_id: PeerId,
    handle: Mutex<Option<KnownPeerHandle>>,
    is_stale: AtomicBool,
}
