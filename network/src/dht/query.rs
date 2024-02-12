use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use ahash::HashMapExt;
use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tycho_util::time::now_sec;
use tycho_util::FastHashMap;

use crate::dht::routing::RoutingTable;
use crate::network::Network;
use crate::proto::dht;
use crate::types::{PeerId, Request};
use crate::util::NetworkExt;

pub struct FindNodesQuery {
    network: Network,
    candidates: RoutingTable,
    max_k: usize,
}

impl FindNodesQuery {
    pub fn new(
        network: Network,
        target: &PeerId,
        nodes: Vec<Arc<dht::NodeInfo>>,
        max_k: usize,
    ) -> Self {
        let mut candidates = RoutingTable::new(*target);
        for node in nodes {
            candidates.add(node, max_k, &Duration::MAX);
        }

        Self {
            network,
            candidates,
            max_k,
        }
    }

    pub async fn run(mut self) -> FastHashMap<PeerId, Arc<dht::NodeInfo>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(dht::rpc::FindNode {
            k: self.max_k as u32,
            key: self.candidates.local_id().to_bytes(),
        }));

        // Prepare request to initial candidates
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(self.candidates.local_id().as_bytes(), self.max_k, |node| {
                futures.push(Self::visit(
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
                Some(Ok(nodes)) => {
                    tracing::debug!(peer_id = %node.id, count = nodes.len(), "received nodes");
                    if !self.update_candidates(now_sec(), self.max_k, nodes, &mut result) {
                        // Do nothing if candidates were not changed
                        continue;
                    }

                    // Add new nodes from the closest range
                    self.candidates.visit_closest(
                        self.candidates.local_id().as_bytes(),
                        self.max_k,
                        |node| {
                            if result.contains_key(&node.id) {
                                // Skip already visited nodes
                                return;
                            }
                            futures.push(Self::visit(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
                            ));
                        },
                    );
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
        nodes: Vec<dht::NodeInfo>,
        result: &mut FastHashMap<PeerId, Arc<dht::NodeInfo>>,
    ) -> bool {
        let mut has_new = false;
        for node in nodes {
            // Skip invalid entries
            if !validate_node_info(now, &node) {
                continue;
            }

            match result.entry(node.id) {
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

    async fn visit(
        network: Network,
        node: Arc<dht::NodeInfo>,
        request_body: Bytes,
    ) -> (Arc<dht::NodeInfo>, Option<Result<Nodes>>) {
        let req = network.query(
            &node.id,
            Request {
                version: Default::default(),
                body: request_body.clone(),
            },
        );

        let res = match tokio::time::timeout(REQUEST_TIMEOUT, req).await {
            Ok(res) => Some(res.and_then(|res| {
                tl_proto::deserialize::<dht::NodeResponse>(&res.body)
                    .map_err(Into::into)
                    .map(|res| res.nodes)
            })),
            Err(_) => None,
        };

        (node, res)
    }
}

fn validate_node_info(now: u32, info: &dht::NodeInfo) -> bool {
    const CLOCK_THRESHOLD: u32 = 1;
    if info.created_at > now + CLOCK_THRESHOLD
        || info.address_list.created_at > now + CLOCK_THRESHOLD
        || info.address_list.expires_at >= now
        || info.address_list.items.is_empty()
    {
        return false;
    }

    let Some(pubkey) = info.id.as_public_key() else {
        return false;
    };

    let Ok::<&[u8; 64], _>(signature) = info.signature.as_ref().try_into() else {
        return false;
    };

    pubkey.verify(info, signature)
}

const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

type Nodes = Vec<dht::NodeInfo>;
