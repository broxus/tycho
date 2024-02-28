use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use ahash::{HashMapExt, HashSetExt};
use anyhow::Result;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use tokio::sync::Semaphore;
use tycho_util::futures::{Shared, WeakShared};
use tycho_util::time::now_sec;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use crate::dht::routing::{RoutingTable, RoutingTableSource};
use crate::network::Network;
use crate::proto::dht::{rpc, NodeResponse, Value, ValueRef, ValueResponse};
use crate::types::{PeerId, PeerInfo, Request};
use crate::util::NetworkExt;

pub struct QueryCache<R> {
    cache: FastDashMap<[u8; 32], WeakSharedBoxedFut<R>>,
}

impl<R> QueryCache<R> {
    pub async fn run<F>(&self, target_id: &[u8; 32], f: F) -> R
    where
        R: Clone,
        F: FnOnce() -> BoxFuture<'static, R>,
    {
        use dashmap::mapref::entry::Entry;

        let fut = match self.cache.entry(*target_id) {
            Entry::Vacant(entry) => {
                let fut = Shared::new(f());
                if let Some(weak) = fut.downgrade() {
                    entry.insert(weak);
                }
                fut
            }
            Entry::Occupied(mut entry) => {
                if let Some(fut) = entry.get().upgrade() {
                    fut
                } else {
                    let fut = Shared::new(f());
                    match fut.downgrade() {
                        Some(weak) => entry.insert(weak),
                        None => entry.remove(),
                    };
                    fut
                }
            }
        };

        fn on_drop<R>(_key: &[u8; 32], value: &WeakSharedBoxedFut<R>) -> bool {
            value.strong_count() == 0
        }

        let (output, is_last) = {
            struct Guard<'a, R> {
                target_id: &'a [u8; 32],
                cache: &'a FastDashMap<[u8; 32], WeakSharedBoxedFut<R>>,
                fut: Option<Shared<BoxFuture<'static, R>>>,
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

            // Await future
            let res = fut.await;

            // Reset the guard if the future was successfully awaited
            guard.fut = None;

            res
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

type WeakSharedBoxedFut<T> = WeakShared<BoxFuture<'static, T>>;

pub struct Query {
    network: Network,
    candidates: RoutingTable,
    max_k: usize,
}

impl Query {
    pub fn new(
        network: Network,
        routing_table: &RoutingTable,
        target_id: &[u8; 32],
        max_k: usize,
    ) -> Self {
        let mut candidates = RoutingTable::new(PeerId(*target_id));
        routing_table.visit_closest(target_id, max_k, |node| {
            candidates.add(
                node.clone(),
                max_k,
                &Duration::MAX,
                RoutingTableSource::Trusted,
            );
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
                    let is_valid = value.is_valid(now_sec(), self.local_id());
                    tracing::debug!(peer_id = %node.id, is_valid, "found value");

                    if !is_valid {
                        // Ignore invalid values
                        continue;
                    }

                    return Some(value);
                }
                // Refill futures from the nodes response
                Some(Ok(ValueResponse::NotFound(nodes))) => {
                    let node_count = nodes.len();
                    let has_new =
                        self.update_candidates(now_sec(), self.max_k, nodes, &mut visited);
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
                    tracing::warn!(peer_id = %node.id, "failed to query nodes: {e:?}");
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
                    if !self.update_candidates_full(now_sec(), self.max_k, nodes, &mut result) {
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
                    tracing::warn!(peer_id = %node.id, "failed to query nodes: {e:?}");
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

    fn update_candidates(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<Arc<PeerInfo>>,
        visited: &mut FastHashSet<PeerId>,
    ) -> bool {
        let mut has_new = false;
        for node in nodes {
            // Skip invalid entries
            if !node.is_valid(now) {
                continue;
            }

            // Insert a new entry
            if visited.insert(node.id) {
                self.candidates
                    .add(node, max_k, &Duration::MAX, RoutingTableSource::Trusted);
                has_new = true;
            }
        }

        has_new
    }

    fn update_candidates_full(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<Arc<PeerInfo>>,
        visited: &mut FastHashMap<PeerId, Arc<PeerInfo>>,
    ) -> bool {
        let mut has_new = false;
        for node in nodes {
            // Skip invalid entries
            if !node.is_valid(now) {
                continue;
            }

            match visited.entry(node.id) {
                // Insert a new entry
                hash_map::Entry::Vacant(entry) => {
                    let node = entry.insert(node).clone();
                    self.candidates
                        .add(node, max_k, &Duration::MAX, RoutingTableSource::Trusted);
                    has_new = true;
                }
                // Try to replace an old entry
                hash_map::Entry::Occupied(mut entry) => {
                    if entry.get().created_at < node.created_at {
                        *entry.get_mut() = node;
                    }
                }
            }
        }

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

        let req = network.query(
            &node.id,
            Request {
                version: Default::default(),
                body: request_body.clone(),
            },
        );

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
        routing_table: &RoutingTable,
        value: ValueRef<'_>,
        max_k: usize,
        local_peer_info: Option<&PeerInfo>,
    ) -> StoreValue<impl Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send> {
        let key_hash = match &value {
            ValueRef::Peer(value) => tl_proto::hash(&value.key),
            ValueRef::Overlay(value) => tl_proto::hash(&value.key),
        };

        let request_body = Bytes::from(match local_peer_info {
            Some(peer_info) => {
                tl_proto::serialize((rpc::WithPeerInfoRef { peer_info }, rpc::StoreRef { value }))
            }
            None => tl_proto::serialize(rpc::StoreRef { value }),
        });

        let semaphore = Arc::new(Semaphore::new(10));
        let futures = futures_util::stream::FuturesUnordered::new();
        routing_table.visit_closest(&key_hash, max_k, |node| {
            futures.push(Self::visit(
                network.clone(),
                node.clone(),
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

        let req = network.send(
            &node.id,
            Request {
                version: Default::default(),
                body: request_body.clone(),
            },
        );

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
                    tracing::warn!(peer_id = %node.id, "failed to store value: {e:?}");
                }
                // Do nothing on timeout
                None => {
                    tracing::warn!(peer_id = %node.id, "failed to store value: timeout");
                }
            }
        }
    }
}

const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_PARALLEL_REQUESTS: usize = 10;
