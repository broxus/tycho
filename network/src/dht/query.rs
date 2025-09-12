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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run)),
            file!(),
            line!(),
        );
        let target_id = target_id;
        let f = f;
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
                    if self.fut.take().map(Shared::consume).unwrap_or_default() {
                        self.cache.remove_if(self.target_id, on_drop);
                    }
                }
            }
            let mut guard = Guard {
                target_id,
                cache: &self.cache,
                fut: None,
            };
            let fut = guard.fut.insert(fut);
            {
                __guard.end_section(line!());
                let __result = fut.await;
                __guard.start_section(line!());
                __result
            }
        };
        if is_last {
            self.cache.remove_if(target_id, on_drop);
        }
        output
    }
}
impl<R> Default for QueryCache<R> {
    fn default() -> Self {
        Self { cache: Default::default() }
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
        routing_table
            .visit_closest(
                target_id_for_full,
                max_k,
                |node| {
                    candidates.add(node.load_peer_info(), max_k, &Duration::MAX, Some);
                },
            );
        Self { network, candidates, max_k }
    }
    fn local_id(&self) -> &[u8; 32] {
        self.candidates.local_id.as_bytes()
    }
    #[tracing::instrument(skip_all)]
    pub async fn find_value(mut self) -> Option<Box<Value>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(find_value)),
            file!(),
            line!(),
        );
        let request_body = Bytes::from(
            tl_proto::serialize(rpc::FindValue {
                key: *self.local_id(),
                k: self.max_k as u32,
            }),
        );
        let semaphore = Semaphore::new(MAX_PARALLEL_REQUESTS);
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(
                self.local_id(),
                self.max_k,
                |node| {
                    futures
                        .push(
                            Self::visit::<
                                ValueResponse,
                            >(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
                                &semaphore,
                            ),
                        );
                },
            );
        let mut visited = FastHashSet::new();
        while let Some((node, res)) = {
            __guard.end_section(line!());
            let __result = futures.next().await;
            __guard.start_section(line!());
            __result
        } {
            match res {
                Some(Ok(ValueResponse::Found(value))) => {
                    let mut signature_checked = false;
                    let is_valid = value
                        .verify_ext(now_sec(), self.local_id(), &mut signature_checked);
                    tracing::debug!(peer_id = % node.id, is_valid, "found value");
                    {
                        __guard.end_section(line!());
                        let __result = yield_on_complex(signature_checked).await;
                        __guard.start_section(line!());
                        __result
                    };
                    if !is_valid {
                        continue;
                    }
                    return Some(value);
                }
                Some(Ok(ValueResponse::NotFound(nodes))) => {
                    let node_count = nodes.len();
                    let has_new = {
                        __guard.end_section(line!());
                        let __result = self
                            .update_candidates(
                                now_sec(),
                                self.max_k,
                                nodes,
                                &mut visited,
                            )
                            .await;
                        __guard.start_section(line!());
                        __result
                    };
                    tracing::debug!(
                        peer_id = % node.id, count = node_count, has_new,
                        "received nodes"
                    );
                    if !has_new {
                        continue;
                    }
                    self.candidates
                        .visit_closest(
                            self.local_id(),
                            self.max_k,
                            |node| {
                                if visited.contains(&node.id) {
                                    return;
                                }
                                futures
                                    .push(
                                        Self::visit::<
                                            ValueResponse,
                                        >(
                                            self.network.clone(),
                                            node.clone(),
                                            request_body.clone(),
                                            &semaphore,
                                        ),
                                    );
                            },
                        );
                }
                Some(Err(e)) => {
                    tracing::warn!(peer_id = % node.id, "failed to query nodes: {e}");
                }
                None => {
                    tracing::warn!(
                        peer_id = % node.id, "failed to query nodes: timeout"
                    );
                }
            }
        }
        None
    }
    #[tracing::instrument(skip_all)]
    pub async fn find_peers(
        mut self,
        depth: Option<usize>,
    ) -> FastHashMap<PeerId, Arc<PeerInfo>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(find_peers)),
            file!(),
            line!(),
        );
        let depth = depth;
        let request_body = Bytes::from(
            tl_proto::serialize(rpc::FindNode {
                key: *self.local_id(),
                k: self.max_k as u32,
            }),
        );
        let semaphore = Semaphore::new(MAX_PARALLEL_REQUESTS);
        let mut futures = FuturesUnordered::new();
        self.candidates
            .visit_closest(
                self.local_id(),
                self.max_k,
                |node| {
                    futures
                        .push(
                            Self::visit::<
                                NodeResponse,
                            >(
                                self.network.clone(),
                                node.clone(),
                                request_body.clone(),
                                &semaphore,
                            ),
                        );
                },
            );
        let mut current_depth = 0;
        let max_depth = depth.unwrap_or(usize::MAX);
        let mut result = FastHashMap::<PeerId, Arc<PeerInfo>>::new();
        while let Some((node, res)) = {
            __guard.end_section(line!());
            let __result = futures.next().await;
            __guard.start_section(line!());
            __result
        } {
            match res {
                Some(Ok(NodeResponse { nodes })) => {
                    tracing::debug!(
                        peer_id = % node.id, count = nodes.len(), "received nodes"
                    );
                    if !{
                        __guard.end_section(line!());
                        let __result = self
                            .update_candidates_full(
                                now_sec(),
                                self.max_k,
                                nodes,
                                &mut result,
                            )
                            .await;
                        __guard.start_section(line!());
                        __result
                    } {
                        continue;
                    }
                    current_depth += 1;
                    if current_depth >= max_depth {
                        break;
                    }
                    self.candidates
                        .visit_closest(
                            self.local_id(),
                            self.max_k,
                            |node| {
                                if result.contains_key(&node.id) {
                                    return;
                                }
                                futures
                                    .push(
                                        Self::visit::<
                                            NodeResponse,
                                        >(
                                            self.network.clone(),
                                            node.clone(),
                                            request_body.clone(),
                                            &semaphore,
                                        ),
                                    );
                            },
                        );
                }
                Some(Err(e)) => {
                    tracing::warn!(peer_id = % node.id, "failed to query nodes: {e}");
                }
                None => {
                    tracing::warn!(
                        peer_id = % node.id, "failed to query nodes: timeout"
                    );
                }
            }
        }
        result
    }
    async fn update_candidates(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<Arc<PeerInfo>>,
        visited: &mut FastHashSet<PeerId>,
    ) -> bool {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(update_candidates)),
            file!(),
            line!(),
        );
        let now = now;
        let max_k = max_k;
        let nodes = nodes;
        let visited = visited;
        let mut has_new = false;
        {
            __guard.end_section(line!());
            let __result = process_only_valid(
                    now,
                    nodes,
                    |node| {
                        if visited.insert(node.id) {
                            self.candidates.add(node, max_k, &Duration::MAX, Some);
                            has_new = true;
                        }
                    },
                )
                .await;
            __guard.start_section(line!());
            __result
        };
        has_new
    }
    async fn update_candidates_full(
        &mut self,
        now: u32,
        max_k: usize,
        nodes: Vec<Arc<PeerInfo>>,
        visited: &mut FastHashMap<PeerId, Arc<PeerInfo>>,
    ) -> bool {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(update_candidates_full)),
            file!(),
            line!(),
        );
        let now = now;
        let max_k = max_k;
        let nodes = nodes;
        let visited = visited;
        let mut has_new = false;
        {
            __guard.end_section(line!());
            let __result = process_only_valid(
                    now,
                    nodes,
                    |node| {
                        match visited.entry(node.id) {
                            hash_map::Entry::Vacant(entry) => {
                                let node = entry.insert(node).clone();
                                self.candidates.add(node, max_k, &Duration::MAX, Some);
                                has_new = true;
                            }
                            hash_map::Entry::Occupied(mut entry) => {
                                if entry.get().created_at < node.created_at {
                                    *entry.get_mut() = node;
                                }
                            }
                        }
                    },
                )
                .await;
            __guard.start_section(line!());
            __result
        };
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(visit)),
            file!(),
            line!(),
        );
        let network = network;
        let node = node;
        let request_body = request_body;
        let semaphore = semaphore;
        let Ok(_permit) = ({
            __guard.end_section(line!());
            let __result = semaphore.acquire().await;
            __guard.start_section(line!());
            __result
        }) else {
            return (node, None);
        };
        let req = network
            .query(
                &node.id,
                Request {
                    version: Default::default(),
                    body: request_body.clone(),
                },
            );
        let res = match {
            __guard.end_section(line!());
            let __result = tokio::time::timeout(REQUEST_TIMEOUT, req).await;
            __guard.start_section(line!());
            __result
        } {
            Ok(res) => {
                Some(
                    res
                        .and_then(|res| {
                            tl_proto::deserialize::<T>(&res.body).map_err(Into::into)
                        }),
                )
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
    ) -> StoreValue<
        impl Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send + use<>,
    > {
        let key_hash = match value {
            ValueRef::Peer(value) => tl_proto::hash(&value.key),
            ValueRef::Merged(value) => tl_proto::hash(&value.key),
        };
        let request_body = Bytes::from(
            match local_peer_info {
                Some(peer_info) => {
                    tl_proto::serialize((
                        rpc::WithPeerInfo::wrap(peer_info),
                        rpc::StoreRef::wrap(value),
                    ))
                }
                None => tl_proto::serialize(rpc::StoreRef::wrap(value)),
            },
        );
        let semaphore = Arc::new(Semaphore::new(10));
        let futures = futures_util::stream::FuturesUnordered::new();
        routing_table
            .visit_closest(
                &key_hash,
                max_k,
                |node| {
                    futures
                        .push(
                            Self::visit(
                                network.clone(),
                                node.load_peer_info(),
                                request_body.clone(),
                                semaphore.clone(),
                            ),
                        );
                },
            );
        StoreValue { futures }
    }
    async fn visit(
        network: Network,
        node: Arc<PeerInfo>,
        request_body: Bytes,
        semaphore: Arc<Semaphore>,
    ) -> (Arc<PeerInfo>, Option<Result<()>>) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(visit)),
            file!(),
            line!(),
        );
        let network = network;
        let node = node;
        let request_body = request_body;
        let semaphore = semaphore;
        let Ok(_permit) = ({
            __guard.end_section(line!());
            let __result = semaphore.acquire().await;
            __guard.start_section(line!());
            __result
        }) else {
            return (node, None);
        };
        let req = network
            .send(
                &node.id,
                Request {
                    version: Default::default(),
                    body: request_body.clone(),
                },
            );
        let res = ({
            __guard.end_section(line!());
            let __result = tokio::time::timeout(REQUEST_TIMEOUT, req).await;
            __guard.start_section(line!());
            __result
        })
            .ok();
        (node, res)
    }
}
impl<T: Future<Output = (Arc<PeerInfo>, Option<Result<()>>)> + Send> StoreValue<T> {
    #[tracing::instrument(level = "debug", skip_all, name = "store_value")]
    pub async fn run(mut self) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run)),
            file!(),
            line!(),
        );
        while let Some((node, res)) = {
            __guard.end_section(line!());
            let __result = self.futures.next().await;
            __guard.start_section(line!());
            __result
        } {
            match res {
                Some(Ok(())) => {
                    tracing::debug!(peer_id = % node.id, "value stored");
                }
                Some(Err(e)) => {
                    tracing::warn!(peer_id = % node.id, "failed to store value: {e}");
                }
                None => {
                    tracing::warn!(
                        peer_id = % node.id, "failed to store value: timeout"
                    );
                }
            }
        }
    }
}
async fn process_only_valid<F>(
    now: u32,
    mut nodes: Vec<Arc<PeerInfo>>,
    mut handle_valid_node: F,
)
where
    F: FnMut(Arc<PeerInfo>) + Send,
{
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(process_only_valid)),
        file!(),
        line!(),
    );
    let now = now;
    let mut nodes = nodes;
    let mut handle_valid_node = handle_valid_node;
    const SPAWN_THRESHOLD: usize = 4;
    if nodes.len() > SPAWN_THRESHOLD {
        let nodes = {
            __guard.end_section(line!());
            let __result = rayon_run(move || {
                    nodes.retain(|node| node.verify(now));
                    nodes
                })
                .await;
            __guard.start_section(line!());
            __result
        };
        for node in nodes {
            handle_valid_node(node);
        }
    } else {
        for node in nodes {
            let mut signature_checked = false;
            let is_valid = node.verify_ext(now, &mut signature_checked);
            {
                __guard.end_section(line!());
                let __result = yield_on_complex(signature_checked).await;
                __guard.start_section(line!());
                __result
            };
            if is_valid {
                handle_valid_node(node);
            }
        }
    };
}
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_PARALLEL_REQUESTS: usize = 10;
