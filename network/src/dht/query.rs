use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use ahash::{HashMapExt, HashSetExt};
use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tycho_util::time::now_sec;
use tycho_util::{FastHashMap, FastHashSet};

use crate::dht::routing::RoutingTable;
use crate::network::Network;
use crate::proto::dht;
use crate::types::{PeerId, Request};
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

    pub async fn find_value(mut self) -> Option<Result<Box<dht::Value>>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(dht::rpc::FindValue {
            key: *self.local_id(),
            k: self.max_k as u32,
        }));

        // Prepare request to initial candidates
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(self.local_id(), self.max_k, |node| {
                futures.push(Self::visit::<dht::ValueResponse>(
                    self.network.clone(),
                    node.clone(),
                    request_body.clone(),
                ));
            });

        // Process responses and refill futures until the value is found or all peers are traversed
        let mut visited = FastHashSet::new();
        while let Some((node, res)) = futures.next().await {
            match res {
                // Return the value if found
                Some(Ok(dht::ValueResponse::Found(value))) => {
                    if !validate_value(now_sec(), self.local_id(), &value) {
                        // Ignore invalid values
                        continue;
                    }

                    return Some(Ok(value));
                }
                // Refill futures from the nodes response
                Some(Ok(dht::ValueResponse::NotFound(nodes))) => {
                    tracing::debug!(peer_id = %node.id, count = nodes.len(), "received nodes");
                    if !self.update_candidates(now_sec(), self.max_k, nodes, &mut visited) {
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
                            futures.push(Self::visit::<dht::ValueResponse>(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
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

    pub async fn find_peers(mut self) -> impl Iterator<Item = Arc<dht::NodeInfo>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(dht::rpc::FindNode {
            key: *self.local_id(),
            k: self.max_k as u32,
        }));

        // Prepare request to initial candidates
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(self.local_id(), self.max_k, |node| {
                futures.push(Self::visit::<dht::NodeResponse>(
                    self.network.clone(),
                    node.clone(),
                    request_body.clone(),
                ));
            });

        // Process responses and refill futures until all peers are traversed
        let mut result = FastHashMap::<PeerId, Arc<dht::NodeInfo>>::new();
        while let Some((node, res)) = futures.next().await {
            match res {
                // Refill futures from the nodes response
                Some(Ok(dht::NodeResponse { nodes })) => {
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
                            futures.push(Self::visit::<dht::NodeResponse>(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
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
        nodes: Vec<dht::NodeInfo>,
        visited: &mut FastHashSet<PeerId>,
    ) -> bool {
        let mut has_new = false;
        for node in nodes {
            // Skip invalid entries
            if !validate_node_info(now, &node) {
                continue;
            }

            // Insert a new entry
            if visited.insert(node.id) {
                self.candidates.add(Arc::new(node), max_k, &Duration::MAX);
                has_new = true;
            }
        }

        has_new
    }

    fn update_candidates_full(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<dht::NodeInfo>,
        visited: &mut FastHashMap<PeerId, Arc<dht::NodeInfo>>,
    ) -> bool {
        let mut has_new = false;
        for node in nodes {
            // Skip invalid entries
            if !validate_node_info(now, &node) {
                continue;
            }

            match visited.entry(node.id) {
                // Insert a new entry
                hash_map::Entry::Vacant(entry) => {
                    let node = entry.insert(Arc::new(node)).clone();
                    self.candidates.add(node, max_k, &Duration::MAX);
                    has_new = true;
                }
                // Try to replace an old entry
                hash_map::Entry::Occupied(mut entry) => {
                    if entry.get().created_at < node.created_at {
                        *entry.get_mut() = Arc::new(node);
                    }
                }
            }
        }

        has_new
    }

    async fn visit<T>(
        network: Network,
        node: Arc<dht::NodeInfo>,
        request_body: Bytes,
    ) -> (Arc<dht::NodeInfo>, Option<Result<T>>)
    where
        for<'a> T: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
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

fn validate_node_info(now: u32, info: &dht::NodeInfo) -> bool {
    info.created_at <= now + CLOCK_THRESHOLD
        && info.address_list.created_at <= now + CLOCK_THRESHOLD
        && info.address_list.expires_at < now
        && !info.address_list.items.is_empty()
        && validate_signature(&info.id, &info.signature, info)
}

fn validate_value(now: u32, key: &[u8; 32], value: &dht::Value) -> bool {
    match value {
        dht::Value::Signed(value) => {
            value.expires_at < now
                && key == &tl_proto::hash(&value.key)
                && validate_signature(&value.key.peer_id, &value.signature, value)
        }
        dht::Value::Overlay(value) => value.expires_at < now && key == &tl_proto::hash(&value.key),
    }
}

fn validate_signature<T>(peed_id: &PeerId, signature: &Bytes, data: &T) -> bool
where
    T: tl_proto::TlWrite,
{
    let Some(pubkey) = peed_id.as_public_key() else {
        return false;
    };
    let Ok::<&[u8; 64], _>(signature) = signature.as_ref().try_into() else {
        return false;
    };
    pubkey.verify(data, signature)
}

const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const CLOCK_THRESHOLD: u32 = 1;
