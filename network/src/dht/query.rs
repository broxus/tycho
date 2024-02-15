use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use ahash::{HashMapExt, HashSetExt};
use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use tokio::sync::Semaphore;
use tycho_util::time::now_sec;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dht::routing::RoutingTable;
use crate::network::Network;
use crate::proto::dht::{rpc, NodeResponse, Value, ValueRef, ValueResponse};
use crate::types::{PeerId, PeerInfo, Request};
use crate::util::NetworkExt;

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
            candidates.add(node.clone(), max_k, &Duration::MAX);
        });

        Self {
            network,
            candidates,
            max_k,
        }
    }

    fn local_id(&self) -> &[u8; 32] {
        self.candidates.local_id().as_bytes()
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
    pub async fn find_peers(mut self) -> impl Iterator<Item = Arc<PeerInfo>> {
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
        result.into_values()
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
                self.candidates.add(node, max_k, &Duration::MAX);
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
                    self.candidates.add(node, max_k, &Duration::MAX);
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
    ) -> StoreValue<impl Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send> {
        let key_hash = match &value {
            ValueRef::Peer(value) => tl_proto::hash(&value.key),
            ValueRef::Overlay(value) => tl_proto::hash(&value.key),
        };

        let request_body = Bytes::from(tl_proto::serialize(rpc::StoreRef { value }));

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
