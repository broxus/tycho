use std::collections::hash_map;
use std::sync::Arc;
use std::time::Duration;

use ahash::{HashMapExt, HashSetExt};
use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use tokio::sync::Semaphore;
use tycho_util::futures::{JoinTask, Shared, WeakSharedHandle};
use tycho_util::sync::{rayon_run, yield_on_complex};
use tycho_util::time::now_sec;
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use crate::dht::config::DhtConfig;
use crate::dht::routing::{HandlesRoutingTable, SimpleRoutingTable};
use crate::network::Network;
use crate::proto::dht::{NodeResponse, Value, ValueRef, ValueResponse, rpc};
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

type WeakSpawnedFut<T> = WeakSharedHandle<JoinTask<T>>;

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
    timeout: Duration,
}

impl Query {
    pub fn new(
        network: Network,
        routing_table: &HandlesRoutingTable,
        target_id: &[u8; 32],
        config: &DhtConfig,
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

        let max_k = config.max_k;
        let timeout = config.request_timeout;

        routing_table.visit_closest(target_id_for_full, max_k, |node| {
            candidates.add(node.load_peer_info(), max_k, &Duration::MAX, Some);
        });

        Self {
            network,
            candidates,
            max_k,
            timeout,
        }
    }

    fn local_id(&self) -> &[u8; 32] {
        self.candidates.local_id.as_bytes()
    }

    #[tracing::instrument(skip_all)]
    pub async fn find_value(mut self) -> Option<Box<Value>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(rpc::FindValue {
            key: *self.local_id(),
            k: self.max_k as u32,
        }));

        // Prepare request to initial candidates
        let mut scheduled = FastHashSet::new();
        let semaphore = Semaphore::new(MAX_PARALLEL_REQUESTS);
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(self.local_id(), self.max_k, |node| {
                if scheduled.insert(node.id) {
                    futures.push(Self::visit::<ValueResponse>(
                        self.network.clone(),
                        node.clone(),
                        request_body.clone(),
                        &semaphore,
                        self.timeout,
                    ));
                }
            });

        // Process responses and refill futures until the value is found or all peers are traversed
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

                    // Update candidates.
                    let mut has_new = false;
                    process_only_valid(now_sec(), nodes, |node| {
                        has_new = self.candidates.add(node, self.max_k, &Duration::MAX, Some);
                    })
                    .await;
                    tracing::debug!(peer_id = %node.id, count = node_count, has_new, "received nodes");

                    // Add new nodes from the closest range
                    self.candidates
                        .visit_closest(self.local_id(), self.max_k, |node| {
                            if scheduled.insert(node.id) {
                                futures.push(Self::visit::<ValueResponse>(
                                    self.network.clone(),
                                    node.clone(),
                                    request_body.clone(),
                                    &semaphore,
                                    self.timeout,
                                ));
                            }
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

    #[tracing::instrument(skip_all)]
    pub async fn find_peers(mut self, depth: Option<usize>) -> FastHashMap<PeerId, Arc<PeerInfo>> {
        // Prepare shared request
        let request_body = Bytes::from(tl_proto::serialize(rpc::FindNode {
            key: *self.local_id(),
            k: self.max_k as u32,
        }));

        // Prepare request to initial candidates
        let mut scheduled = FastHashSet::new();
        let mut candidate_depths = FastHashMap::new();
        let semaphore = Semaphore::new(MAX_PARALLEL_REQUESTS);
        let mut futures = FuturesUnordered::new();

        let visit = |this: &Query, node: &Arc<PeerInfo>, depth: usize| {
            use futures_util::FutureExt;

            Self::visit::<NodeResponse>(
                this.network.clone(),
                node.clone(),
                request_body.clone(),
                &semaphore,
                this.timeout,
            )
            .map(move |res| (res, depth))
        };

        self.candidates
            .visit_closest(self.local_id(), self.max_k, |node| {
                if scheduled.insert(node.id) {
                    candidate_depths.insert(node.id, 0);
                    futures.push(visit(&self, node, 0));
                }
            });

        // Process responses and refill futures until all peers are traversed
        let max_depth = depth.unwrap_or(usize::MAX);
        let mut result = FastHashMap::<PeerId, Arc<PeerInfo>>::new();
        while let Some(((node, res), query_depth)) = futures.next().await {
            match res {
                // Refill futures from the nodes response
                Some(Ok(NodeResponse { nodes })) => {
                    tracing::debug!(peer_id = %node.id, count = nodes.len(), "received nodes");

                    // Update candidates.
                    process_only_valid(now_sec(), nodes, |node| {
                        let discovered_depth = query_depth.saturating_add(1);
                        candidate_depths
                            .entry(node.id)
                            .and_modify(|depth| *depth = (*depth).min(discovered_depth))
                            .or_insert(discovered_depth);

                        match result.entry(node.id) {
                            // Insert a new entry
                            hash_map::Entry::Vacant(entry) => {
                                let node = entry.insert(node).clone();
                                self.candidates.add(node, self.max_k, &Duration::MAX, Some);
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

                    // Add new nodes from the closest range
                    self.candidates
                        .visit_closest(self.local_id(), self.max_k, |node| {
                            let Some(&candidate_depth) = candidate_depths.get(&node.id) else {
                                return;
                            };

                            if candidate_depth <= max_depth && scheduled.insert(node.id) {
                                futures.push(visit(&self, node, candidate_depth));
                            }
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

    async fn visit<T>(
        network: Network,
        node: Arc<PeerInfo>,
        request_body: Bytes,
        semaphore: &Semaphore,
        timeout: Duration,
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

        let res = match tokio::time::timeout(timeout, req).await {
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
        config: &DhtConfig,
        local_peer_info: Option<&PeerInfo>,
    ) -> StoreValue<impl Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send + use<>> {
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
        routing_table.visit_closest(&key_hash, config.max_k, |node| {
            futures.push(Self::visit(
                network.clone(),
                node.load_peer_info(),
                request_body.clone(),
                semaphore.clone(),
                config.request_timeout,
            ));
        });

        StoreValue { futures }
    }

    async fn visit(
        network: Network,
        node: Arc<PeerInfo>,
        request_body: Bytes,
        semaphore: Arc<Semaphore>,
        timeout: Duration,
    ) -> (Arc<PeerInfo>, Option<Result<()>>) {
        let Ok(_permit) = semaphore.acquire().await else {
            return (node, None);
        };

        let req = network.send(&node.id, Request {
            version: Default::default(),
            body: request_body.clone(),
        });

        let res = (tokio::time::timeout(timeout, req).await).ok();

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

const MAX_PARALLEL_REQUESTS: usize = 10;

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use tycho_crypto::ed25519;

    use super::*;
    use crate::dht::{DhtClient, DhtService};
    use crate::util::Router;

    struct Node {
        network: Network,
        dht: DhtClient,
    }

    impl Node {
        fn new() -> Self {
            let key = rand::random::<ed25519::SecretKey>();
            let local_id = ed25519::PublicKey::from(&key).into();

            let (_, dht_service) = DhtService::builder(local_id).build();
            let router = Router::builder().route(dht_service.clone()).build();
            let network = Network::builder()
                .with_private_key(key.to_bytes())
                .build((Ipv4Addr::LOCALHOST, 0), router)
                .unwrap();
            let dht = dht_service.make_client(&network);

            Self { network, dht }
        }

        fn add_peer(&self, peer: &Self) {
            self.dht.add_peer(peer.peer_info()).unwrap();
        }

        fn peer_info(&self) -> Arc<PeerInfo> {
            Arc::new(self.network.sign_peer_info(now_sec(), 60))
        }

        fn make_query(&self, target_id: &[u8; 32]) -> Query {
            let routing_table = self.dht.inner.routing_table.lock().unwrap();
            Query::new(
                self.network.clone(),
                &routing_table,
                target_id,
                &DhtConfig::default(),
                DhtQueryMode::Closest,
            )
        }
    }

    #[tokio::test]
    async fn find_peers_limits_queries_by_hop_depth() {
        let [a, b, c, d, e] = std::array::from_fn(|_| Node::new());

        a.add_peer(&b);
        b.add_peer(&c);
        c.add_peer(&d);
        d.add_peer(&e);

        // All peers except `b` can be accessed from `a`.
        let _known_peers = [&c, &d, &e].map(|peer| {
            a.network
                .known_peers()
                .insert(peer.peer_info(), false)
                .unwrap()
        });

        let target_id = rand::random();

        let result = a.make_query(&target_id).find_peers(Some(0)).await;
        assert!(!result.contains_key(a.network.peer_id()));
        assert!(!result.contains_key(b.network.peer_id()));
        assert!(result.contains_key(c.network.peer_id()));
        assert!(!result.contains_key(d.network.peer_id()));

        let result = a.make_query(&target_id).find_peers(Some(1)).await;
        assert!(result.contains_key(c.network.peer_id()));
        assert!(result.contains_key(d.network.peer_id()));
        assert!(!result.contains_key(e.network.peer_id()));

        let result = a.make_query(&target_id).find_peers(Some(2)).await;
        assert!(result.contains_key(c.network.peer_id()));
        assert!(result.contains_key(d.network.peer_id()));
        assert!(result.contains_key(e.network.peer_id()));
    }
}
