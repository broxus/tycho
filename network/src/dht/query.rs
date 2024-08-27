use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use ahash::{HashMapExt, HashSetExt};
use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use tokio::sync::Semaphore;
use tycho_util::futures::{JoinTask, Shared, WeakShared};
use tycho_util::sync::{rayon_run, yield_on_complex};
use tycho_util::time::now_sec;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use crate::dht::routing::{HandlesRoutingTable, SimpleRoutingTable};
use crate::network::Network;
use crate::proto::dht::{rpc, NodeResponse, Value, ValueRef, ValueResponse};
use crate::types::{PeerId, PeerInfo, Request};
use crate::util::NetworkExt;

pub struct QueryCache<R> {
    cache: FastDashMap<[u8; 32], WeakSpawnedFut<R>>,
}

impl<R> QueryCache<R> {
    pub async fn run<F, Fut>(&self, target_id: &[u8; 32], f: F) -> R
    where
        R: Clone + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = R> + Send + 'static,
    {
        use dashmap::mapref::entry::Entry;

        let fut = match self.cache.entry(*target_id) {
            Entry::Vacant(entry) => {
                let fut = Shared::new(JoinTask::new(f()));
                if let Some(weak) = fut.downgrade() {
                    entry.insert(weak);
                }
                fut
            }
            Entry::Occupied(mut entry) => {
                if let Some(fut) = entry.get().upgrade() {
                    fut
                } else {
                    let fut = Shared::new(JoinTask::new(f()));
                    match fut.downgrade() {
                        Some(weak) => entry.insert(weak),
                        None => entry.remove(),
                    };
                    fut
                }
            }
        };

        fn on_drop<R>(_key: &[u8; 32], value: &WeakSpawnedFut<R>) -> bool {
            value.strong_count() == 0
        }

        let (output, is_last) = {
            struct Guard<'a, R> {
                target_id: &'a [u8; 32],
                cache: &'a FastDashMap<[u8; 32], WeakSpawnedFut<R>>,
                fut: Option<Shared<JoinTask<R>>>,
            }

            impl<R> Drop for Guard<'_, R> {
                fn drop(&mut self) {
                    // Remove value from cache if we consumed the last future instance
                    if self.fut.take().map(Shared::consume).unwrap_or_default() {
                        self.cache.remove_if(self.target_id, on_drop);
                    }
                }
            }

            // Wrap future into guard to remove it from cache event it was cancelled
            let mut guard = Guard {
                target_id,
                cache: &self.cache,
                fut: None,
            };
            let fut = guard.fut.insert(fut);

            // Await future.
            // If `Shared` future is not polled to `Complete` state,
            // the guard will try to consume it and remove from cache
            // if it was the last instance.
            fut.await
        };

        // TODO: add ttl and force others to make a request for a fresh data
        if is_last {
            // Remove value from cache if we consumed the last future instance
            self.cache.remove_if(target_id, on_drop);
        }

        output
    }
}

impl<R> Default for QueryCache<R> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

type WeakSpawnedFut<T> = WeakShared<JoinTask<T>>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DhtQueryMode {
    #[default]
    Closest,
    Random,
}

pub struct Query {
    network: Network,
    candidates: SimpleRoutingTable,
    max_k: usize,
}

impl Query {
    pub fn new(
        network: Network,
        routing_table: &HandlesRoutingTable,
        target_id: &[u8; 32],
        max_k: usize,
        mode: DhtQueryMode,
    ) -> Self {
        let mut candidates = SimpleRoutingTable::new(PeerId(*target_id));

        let random_id;
        let target_id_for_full = match mode {
            DhtQueryMode::Closest => target_id,
            DhtQueryMode::Random => {
                random_id = rand::random();
                &random_id
            }
        };

        routing_table.visit_closest(target_id_for_full, max_k, |node| {
            candidates.add(node.load_peer_info(), max_k, &Duration::MAX, Some);
        });

        Self {
            network,
            candidates,
            max_k,
        }
    }

    fn local_id(&self) -> &[u8; 32] {
        self.candidates.local_id.as_bytes()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn find_value(mut self) -> Option<Box<Value>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(rpc::FindValue {
            key: *self.local_id(),
            k: self.max_k as u32,
        }));

        // Prepare request to initial candidates
        let semaphore = Semaphore::new(MAX_PARALLEL_REQUESTS);
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(self.local_id(), self.max_k, |node| {
                futures.push(Self::visit::<ValueResponse>(
                    self.network.clone(),
                    node.clone(),
                    request_body.clone(),
                    &semaphore,
                ));
            });

        // Process responses and refill futures until the value is found or all peers are traversed
        let mut visited = FastHashSet::new();
        while let Some((node, res)) = futures.next().await {
            match res {
                // Return the value if found
                Some(Ok(ValueResponse::Found(value))) => {
                    let mut signature_checked = false;
                    let is_valid =
                        value.verify_ext(now_sec(), self.local_id(), &mut signature_checked);
                    tracing::debug!(peer_id = %node.id, is_valid, "found value");

                    yield_on_complex(signature_checked).await;

                    if !is_valid {
                        // Ignore invalid values
                        continue;
                    }

                    return Some(value);
                }
                // Refill futures from the nodes response
                Some(Ok(ValueResponse::NotFound(nodes))) => {
                    let node_count = nodes.len();
                    let has_new = self
                        .update_candidates(now_sec(), self.max_k, nodes, &mut visited)
                        .await;
                    tracing::debug!(peer_id = %node.id, count = node_count, has_new, "received nodes");

                    if !has_new {
                        // Do nothing if candidates were not changed
                        continue;
                    }

                    // Add new nodes from the closest range
                    self.candidates
                        .visit_closest(self.local_id(), self.max_k, |node| {
                            if visited.contains(&node.id) {
                                // Skip already visited nodes
                                return;
                            }
                            futures.push(Self::visit::<ValueResponse>(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
                                &semaphore,
                            ));
                        });
                }
                // Do nothing on error
                Some(Err(e)) => {
                    tracing::warn!(peer_id = %node.id, "failed to query nodes: {e}");
                }
                // Do nothing on timeout
                None => {
                    tracing::warn!(peer_id = %node.id, "failed to query nodes: timeout");
                }
            }
        }

        // Done
        None
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn find_peers(mut self, depth: Option<usize>) -> FastHashMap<PeerId, Arc<PeerInfo>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(rpc::FindNode {
            key: *self.local_id(),
            k: self.max_k as u32,
        }));

        // Prepare request to initial candidates
        let semaphore = Semaphore::new(MAX_PARALLEL_REQUESTS);
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(self.local_id(), self.max_k, |node| {
                futures.push(Self::visit::<NodeResponse>(
                    self.network.clone(),
                    node.clone(),
                    request_body.clone(),
                    &semaphore,
                ));
            });

        // Process responses and refill futures until all peers are traversed
        let mut current_depth = 0;
        let max_depth = depth.unwrap_or(usize::MAX);
        let mut result = FastHashMap::<PeerId, Arc<PeerInfo>>::new();
        while let Some((node, res)) = futures.next().await {
            match res {
                // Refill futures from the nodes response
                Some(Ok(NodeResponse { nodes })) => {
                    tracing::debug!(peer_id = %node.id, count = nodes.len(), "received nodes");
                    if !self
                        .update_candidates_full(now_sec(), self.max_k, nodes, &mut result)
                        .await
                    {
                        // Do nothing if candidates were not changed
                        continue;
                    }

                    current_depth += 1;
                    if current_depth >= max_depth {
                        // Stop on max depth
                        break;
                    }

                    // Add new nodes from the closest range
                    self.candidates
                        .visit_closest(self.local_id(), self.max_k, |node| {
                            if result.contains_key(&node.id) {
                                // Skip already visited nodes
                                return;
                            }
                            futures.push(Self::visit::<NodeResponse>(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
                                &semaphore,
                            ));
                        });
                }
                // Do nothing on error
                Some(Err(e)) => {
                    tracing::warn!(peer_id = %node.id, "failed to query nodes: {e}");
                }
                // Do nothing on timeout
                None => {
                    tracing::warn!(peer_id = %node.id, "failed to query nodes: timeout");
                }
            }
        }

        // Done
        result
    }

    async fn update_candidates(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<Arc<PeerInfo>>,
        visited: &mut FastHashSet<PeerId>,
    ) -> bool {
        let mut has_new = false;
        process_only_valid(now, nodes, |node| {
            // Insert a new entry
            if visited.insert(node.id) {
                self.candidates.add(node, max_k, &Duration::MAX, Some);
                has_new = true;
            }
        })
        .await;

        has_new
    }

    async fn update_candidates_full(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<Arc<PeerInfo>>,
        visited: &mut FastHashMap<PeerId, Arc<PeerInfo>>,
    ) -> bool {
        let mut has_new = false;
        process_only_valid(now, nodes, |node| {
            match visited.entry(node.id) {
                // Insert a new entry
                hash_map::Entry::Vacant(entry) => {
                    let node = entry.insert(node).clone();
                    self.candidates.add(node, max_k, &Duration::MAX, Some);
                    has_new = true;
                }
                // Try to replace an old entry
                hash_map::Entry::Occupied(mut entry) => {
                    if entry.get().created_at < node.created_at {
                        *entry.get_mut() = node;
                    }
                }
            }
        })
        .await;

        has_new
    }

    async fn visit<T>(
        network: Network,
        node: Arc<PeerInfo>,
        request_body: Bytes,
        semaphore: &Semaphore,
    ) -> (Arc<PeerInfo>, Option<Result<T>>)
    where
        for<'a> T: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        let Ok(_permit) = semaphore.acquire().await else {
            return (node, None);
        };

        let req = network.query(&node.id, Request {
            version: Default::default(),
            body: request_body.clone(),
        });

        let res = match tokio::time::timeout(REQUEST_TIMEOUT, req).await {
            Ok(res) => {
                Some(res.and_then(|res| tl_proto::deserialize::<T>(&res.body).map_err(Into::into)))
            }
            Err(_) => None,
        };

        (node, res)
    }
}

pub struct StoreValue<F = ()> {
    futures: FuturesUnordered<F>,
}

impl StoreValue<()> {
    pub fn new(
        network: Network,
        routing_table: &HandlesRoutingTable,
        value: &ValueRef<'_>,
        max_k: usize,
        local_peer_info: Option<&PeerInfo>,
    ) -> StoreValue<impl Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send> {
        let key_hash = match value {
            ValueRef::Peer(value) => tl_proto::hash(&value.key),
            ValueRef::Merged(value) => tl_proto::hash(&value.key),
        };

        let request_body = Bytes::from(match local_peer_info {
            Some(peer_info) => tl_proto::serialize((
                rpc::WithPeerInfo::wrap(peer_info),
                rpc::StoreRef::wrap(value),
            )),
            None => tl_proto::serialize(rpc::StoreRef::wrap(value)),
        });

        let semaphore = Arc::new(Semaphore::new(10));
        let futures = futures_util::stream::FuturesUnordered::new();
        routing_table.visit_closest(&key_hash, max_k, |node| {
            futures.push(Self::visit(
                network.clone(),
                node.load_peer_info(),
                request_body.clone(),
                semaphore.clone(),
            ));
        });

        StoreValue { futures }
    }

    async fn visit(
        network: Network,
        node: Arc<PeerInfo>,
        request_body: Bytes,
        semaphore: Arc<Semaphore>,
    ) -> (Arc<PeerInfo>, Option<Result<()>>) {
        let Ok(_permit) = semaphore.acquire().await else {
            return (node, None);
        };

        let req = network.send(&node.id, Request {
            version: Default::default(),
            body: request_body.clone(),
        });

        let res = match tokio::time::timeout(REQUEST_TIMEOUT, req).await {
            Ok(res) => Some(res),
            Err(_) => None,
        };

        (node, res)
    }
}

impl<T: Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send> StoreValue<T> {
    #[tracing::instrument(level = "debug", skip_all, name = "store_value")]
    pub async fn run(mut self) {
        while let Some((node, res)) = self.futures.next().await {
            match res {
                Some(Ok(())) => {
                    tracing::debug!(peer_id = %node.id, "value stored");
                }
                Some(Err(e)) => {
                    tracing::warn!(peer_id = %node.id, "failed to store value: {e}");
                }
                // Do nothing on timeout
                None => {
                    tracing::warn!(peer_id = %node.id, "failed to store value: timeout");
                }
            }
        }
    }
}

async fn process_only_valid<F>(now: u32, mut nodes: Vec<Arc<PeerInfo>>, mut handle_valid_node: F)
where
    F: FnMut(Arc<PeerInfo>) + Send,
{
    const SPAWN_THRESHOLD: usize = 4;

    // NOTE: Ensure that we don't block the thread for too long
    if nodes.len() > SPAWN_THRESHOLD {
        let nodes = rayon_run(move || {
            nodes.retain(|node| node.verify(now));
            nodes
        })
        .await;

        for node in nodes {
            handle_valid_node(node);
        }
    } else {
        for node in nodes {
            let mut signature_checked = false;
            let is_valid = node.verify_ext(now, &mut signature_checked);
            yield_on_complex(signature_checked).await;

            if is_valid {
                handle_valid_node(node);
            }
        }
    };
}

const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_PARALLEL_REQUESTS: usize = 10;
