use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ahash::HashMapExt;
use anyhow::Result;
use bytes::{Buf, Bytes};
use rand::RngCore;
use tl_proto::TlRead;
use tokio::sync::{Notify, broadcast};
use tycho_util::time::now_sec;
use tycho_util::{FastHashMap, realloc_box_enum};

pub use self::config::DhtConfig;
pub use self::peer_resolver::{
    PeerResolver, PeerResolverBuilder, PeerResolverConfig, PeerResolverHandle,
};
pub use self::query::DhtQueryMode;
use self::query::{Query, QueryCache, StoreValue};
use self::routing::HandlesRoutingTable;
use self::storage::Storage;
pub use self::storage::{DhtValueMerger, DhtValueSource, StorageError};
use crate::network::Network;
use crate::proto::dht::{
    NodeInfoResponse, NodeResponse, PeerValue, PeerValueKey, PeerValueKeyName, PeerValueKeyRef,
    PeerValueRef, Value, ValueRef, ValueResponseRaw, rpc,
};
use crate::types::{PeerId, PeerInfo, Request, Response, Service, ServiceRequest};
use crate::util::{NetworkExt, Routable};

mod background_tasks;
mod config;
mod peer_resolver;
mod query;
mod routing;
mod storage;

// Counters
const METRIC_IN_REQ_TOTAL: &str = "tycho_net_dht_in_req_total";
const METRIC_IN_REQ_FAIL_TOTAL: &str = "tycho_net_dht_in_req_fail_total";

const METRIC_IN_REQ_WITH_PEER_INFO_TOTAL: &str = "tycho_net_dht_in_req_with_peer_info_total";
const METRIC_IN_REQ_FIND_NODE_TOTAL: &str = "tycho_net_dht_in_req_find_node_total";
const METRIC_IN_REQ_FIND_VALUE_TOTAL: &str = "tycho_net_dht_in_req_find_value_total";
const METRIC_IN_REQ_GET_NODE_INFO_TOTAL: &str = "tycho_net_dht_in_req_get_node_info_total";
const METRIC_IN_REQ_STORE_TOTAL: &str = "tycho_net_dht_in_req_store_value_total";

#[derive(Clone)]
pub struct DhtClient {
    inner: Arc<DhtInner>,
    network: Network,
}

impl DhtClient {
    #[inline]
    pub fn network(&self) -> &Network {
        &self.network
    }

    #[inline]
    pub fn service(&self) -> &DhtService {
        DhtService::wrap(&self.inner)
    }

    pub fn add_peer(&self, peer: Arc<PeerInfo>) -> Result<bool> {
        anyhow::ensure!(peer.verify(now_sec()), "invalid peer info");
        let added = self.inner.add_peer_info(&self.network, peer);
        Ok(added)
    }

    pub fn add_allow_outdated_peer(&self, peer: Arc<PeerInfo>) -> Result<bool> {
        anyhow::ensure!(peer.verify(now_sec()), "invalid peer info");
        let added = self.inner.add_allow_outdated_peer_info(&self.network, peer);
        Ok(added)
    }

    pub async fn get_node_info(&self, peer_id: &PeerId) -> Result<PeerInfo> {
        let res = self
            .network
            .query(peer_id, Request::from_tl(rpc::GetNodeInfo))
            .await?;
        let NodeInfoResponse { info } = res.parse_tl()?;
        Ok(info)
    }

    pub fn entry(&self, name: PeerValueKeyName) -> DhtQueryBuilder<'_> {
        DhtQueryBuilder {
            inner: &self.inner,
            network: &self.network,
            name,
            idx: 0,
        }
    }

    /// Find a value by its key hash.
    ///
    /// This is quite a low-level method, so it is recommended to use [`DhtClient::entry`].
    pub async fn find_value(&self, key_hash: &[u8; 32], mode: DhtQueryMode) -> Option<Box<Value>> {
        self.inner.find_value(&self.network, key_hash, mode).await
    }
}

#[derive(Clone, Copy)]
pub struct DhtQueryBuilder<'a> {
    inner: &'a DhtInner,
    network: &'a Network,
    name: PeerValueKeyName,
    idx: u32,
}

impl<'a> DhtQueryBuilder<'a> {
    #[inline]
    pub fn with_idx(&mut self, idx: u32) -> &mut Self {
        self.idx = idx;
        self
    }

    pub async fn find_value<T>(&self, peer_id: &PeerId) -> Result<T, FindValueError>
    where
        for<'tl> T: tl_proto::TlRead<'tl>,
    {
        let key_hash = tl_proto::hash(PeerValueKeyRef {
            name: self.name,
            peer_id,
        });

        match self
            .inner
            .find_value(self.network, &key_hash, DhtQueryMode::Closest)
            .await
        {
            Some(value) => match value.as_ref() {
                Value::Peer(value) => {
                    tl_proto::deserialize(&value.data).map_err(FindValueError::InvalidData)
                }
                Value::Merged(_) => Err(FindValueError::InvalidData(
                    tl_proto::TlError::UnknownConstructor,
                )),
            },
            None => Err(FindValueError::NotFound),
        }
    }

    pub async fn find_peer_value_raw(
        &self,
        peer_id: &PeerId,
    ) -> Result<Box<PeerValue>, FindValueError> {
        let key_hash = tl_proto::hash(PeerValueKeyRef {
            name: self.name,
            peer_id,
        });

        match self
            .inner
            .find_value(self.network, &key_hash, DhtQueryMode::Closest)
            .await
        {
            Some(value) => {
                realloc_box_enum!(value, {
                    Value::Peer(value) => Box::new(value) => Ok(value),
                    Value::Merged(_) => Err(FindValueError::InvalidData(
                        tl_proto::TlError::UnknownConstructor,
                    )),
                })
            }
            None => Err(FindValueError::NotFound),
        }
    }

    pub fn with_data<T>(&self, data: T) -> DhtQueryWithDataBuilder<'a>
    where
        T: tl_proto::TlWrite,
    {
        DhtQueryWithDataBuilder {
            inner: *self,
            data: tl_proto::serialize(&data),
            at: None,
            ttl: self.inner.config.max_stored_value_ttl.as_secs() as _,
            with_peer_info: false,
        }
    }
}

pub struct DhtQueryWithDataBuilder<'a> {
    inner: DhtQueryBuilder<'a>,
    data: Vec<u8>,
    at: Option<u32>,
    ttl: u32,
    with_peer_info: bool,
}

impl DhtQueryWithDataBuilder<'_> {
    pub fn with_time(&mut self, at: u32) -> &mut Self {
        self.at = Some(at);
        self
    }

    pub fn with_ttl(&mut self, ttl: u32) -> &mut Self {
        self.ttl = ttl;
        self
    }

    pub fn with_peer_info(&mut self, with_peer_info: bool) -> &mut Self {
        self.with_peer_info = with_peer_info;
        self
    }

    pub async fn store(&self) -> Result<()> {
        let dht = self.inner.inner;
        let network = self.inner.network;

        let mut value = self.make_unsigned_value_ref();
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        dht.store_value(network, &ValueRef::Peer(value), self.with_peer_info)
            .await
    }

    pub fn store_locally(&self) -> Result<bool, StorageError> {
        let dht = self.inner.inner;
        let network = self.inner.network;

        let mut value = self.make_unsigned_value_ref();
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        dht.store_value_locally(&ValueRef::Peer(value))
    }

    pub fn into_signed_value(self) -> PeerValue {
        let dht = self.inner.inner;
        let network = self.inner.network;

        let mut value = PeerValue {
            key: PeerValueKey {
                name: self.name,
                peer_id: dht.local_id,
            },
            data: self.data.into_boxed_slice(),
            expires_at: self.at.unwrap_or_else(now_sec) + self.ttl,
            signature: Box::new([0; 64]),
        };
        *value.signature = network.sign_tl(&value);
        value
    }

    fn make_unsigned_value_ref(&self) -> PeerValueRef<'_> {
        PeerValueRef {
            key: PeerValueKeyRef {
                name: self.inner.name,
                peer_id: &self.inner.inner.local_id,
            },
            data: &self.data,
            expires_at: self.at.unwrap_or_else(now_sec) + self.ttl,
            signature: &[0; 64],
        }
    }
}

impl<'a> std::ops::Deref for DhtQueryWithDataBuilder<'a> {
    type Target = DhtQueryBuilder<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for DhtQueryWithDataBuilder<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct DhtServiceBackgroundTasks {
    inner: Arc<DhtInner>,
}

impl DhtServiceBackgroundTasks {
    pub fn spawn_without_bootstrap(self, network: &Network) {
        self.inner
            .start_background_tasks(Network::downgrade(network), Vec::new());
    }

    pub fn spawn<I>(self, network: &Network, bootstrap_peers: I) -> Result<usize>
    where
        I: IntoIterator<Item: std::ops::Deref<Target = PeerInfo>>,
    {
        let mut peers = Vec::new();
        for peer in bootstrap_peers {
            let peer = &*peer;

            anyhow::ensure!(
                peer.verify(now_sec()),
                "invalid peer info for id {}",
                peer.id
            );
            let peer = Arc::new(peer.clone());
            if self.inner.add_peer_info(network, peer.clone()) {
                peers.push(peer);
            }
        }

        let count = peers.len();
        self.inner
            .start_background_tasks(Network::downgrade(network), peers);
        Ok(count)
    }
}

pub struct DhtServiceBuilder {
    local_id: PeerId,
    config: Option<DhtConfig>,
}

impl DhtServiceBuilder {
    pub fn with_config(mut self, config: DhtConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn build(self) -> (DhtServiceBackgroundTasks, DhtService) {
        let config = self.config.unwrap_or_default();

        let storage = {
            let mut builder = Storage::builder()
                .with_max_capacity(config.max_storage_capacity)
                .with_max_ttl(config.max_stored_value_ttl);

            if let Some(time_to_idle) = config.storage_item_time_to_idle {
                builder = builder.with_max_idle(time_to_idle);
            }

            builder.build()
        };

        let (announced_peers, _) = broadcast::channel(config.announced_peers_channel_capacity);

        let inner = Arc::new(DhtInner {
            local_id: self.local_id,
            routing_table: Mutex::new(HandlesRoutingTable::new(self.local_id)),
            storage,
            local_peer_info: Mutex::new(None),
            config,
            announced_peers,
            find_value_queries: Default::default(),
            peer_added: Arc::new(Default::default()),

            local_info_pre_announced: AtomicBool::new(false),
            local_info_announced_notify: Arc::new(Default::default()),
        });

        let background_tasks = DhtServiceBackgroundTasks {
            inner: inner.clone(),
        };

        (background_tasks, DhtService(inner))
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct DhtService(Arc<DhtInner>);

impl DhtService {
    #[inline]
    fn wrap(inner: &Arc<DhtInner>) -> &Self {
        // SAFETY: `DhtService` has the same memory layout as `Arc<DhtInner>`.
        unsafe { &*(inner as *const Arc<DhtInner>).cast::<Self>() }
    }

    pub fn builder(local_id: PeerId) -> DhtServiceBuilder {
        DhtServiceBuilder {
            local_id,
            config: None,
        }
    }

    pub fn local_id(&self) -> &PeerId {
        &self.0.local_id
    }

    pub fn make_client(&self, network: &Network) -> DhtClient {
        DhtClient {
            inner: self.0.clone(),
            network: network.clone(),
        }
    }

    pub fn make_peer_resolver(&self) -> PeerResolverBuilder {
        PeerResolver::builder(self.clone())
    }

    pub fn has_peer(&self, peer_id: &PeerId) -> bool {
        self.0.routing_table.lock().unwrap().contains(peer_id)
    }

    pub fn find_local_closest(&self, key: &[u8; 32], count: usize) -> Vec<Arc<PeerInfo>> {
        self.0.routing_table.lock().unwrap().closest(key, count)
    }

    pub fn store_value_locally(&self, value: &ValueRef<'_>) -> Result<bool, StorageError> {
        self.0.store_value_locally(value)
    }

    pub fn insert_merger(
        &self,
        group_id: &[u8; 32],
        merger: Arc<dyn DhtValueMerger>,
    ) -> Option<Arc<dyn DhtValueMerger>> {
        self.0.storage.insert_merger(group_id, merger)
    }

    pub fn remove_merger(&self, group_id: &[u8; 32]) -> Option<Arc<dyn DhtValueMerger>> {
        self.0.storage.remove_merger(group_id)
    }

    pub fn peer_added(&self) -> &Arc<Notify> {
        &self.0.peer_added
    }

    pub async fn wait_for_pre_announce(&self) {
        self.0.wait_for_local_info_announced().await;
    }
}

impl Service<ServiceRequest> for DhtService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_dht_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        metrics::counter!(METRIC_IN_REQ_TOTAL).increment(1);

        let (constructor, body) = match self.0.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);
                return futures_util::future::ready(None);
            }
        };

        let response = crate::match_tl_request!(body, tag = constructor, {
            rpc::FindNode as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_node");
                metrics::counter!(METRIC_IN_REQ_FIND_NODE_TOTAL).increment(1);

                let res = self.0.handle_find_node(r);
                Some(tl_proto::serialize(res))
            },
            rpc::FindValue as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_value");
                metrics::counter!(METRIC_IN_REQ_FIND_VALUE_TOTAL).increment(1);

                let res = self.0.handle_find_value(r);
                Some(tl_proto::serialize(res))
            },
            rpc::GetNodeInfo as _ => {
                tracing::debug!("get_node_info");
                metrics::counter!(METRIC_IN_REQ_GET_NODE_INFO_TOTAL).increment(1);

                self.0.handle_get_node_info().map(tl_proto::serialize)
            },
        }, e => {
            tracing::debug!("failed to deserialize query: {e}");
            None
        });

        if response.is_none() {
            metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);
        }

        futures_util::future::ready(response.map(|body| Response {
            version: Default::default(),
            body: Bytes::from(body),
        }))
    }

    #[tracing::instrument(
        level = "debug",
        name = "on_dht_message",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_message(&self, req: ServiceRequest) -> Self::OnMessageFuture {
        metrics::counter!(METRIC_IN_REQ_TOTAL).increment(1);

        let (constructor, body) = match self.0.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize message: {e}");
                metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);
                return futures_util::future::ready(());
            }
        };

        let mut has_error = false;
        crate::match_tl_request!(body, tag = constructor, {
            rpc::StoreRef<'_> as ref r => {
                tracing::debug!("store");
                metrics::counter!(METRIC_IN_REQ_STORE_TOTAL).increment(1);

                if let Err(e) = self.0.handle_store(r) {
                    tracing::debug!("failed to store value: {e}");
                    has_error = true;
                }
            }
        }, e => {
            tracing::debug!("failed to deserialize message: {e}");
            has_error = true;
        });

        if has_error {
            metrics::counter!(METRIC_IN_REQ_FAIL_TOTAL).increment(1);
        }

        futures_util::future::ready(())
    }
}

impl Routable for DhtService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [
            rpc::WithPeerInfo::TL_ID,
            rpc::FindNode::TL_ID,
            rpc::FindValue::TL_ID,
            rpc::GetNodeInfo::TL_ID,
        ]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::WithPeerInfo::TL_ID, rpc::Store::TL_ID]
    }
}

struct DhtInner {
    local_id: PeerId,
    routing_table: Mutex<HandlesRoutingTable>,
    storage: Storage,
    local_peer_info: Mutex<Option<PeerInfo>>,
    config: DhtConfig,
    announced_peers: broadcast::Sender<Arc<PeerInfo>>,
    find_value_queries: QueryCache<Option<Box<Value>>>,
    peer_added: Arc<Notify>,

    local_info_pre_announced: AtomicBool,
    local_info_announced_notify: Arc<Notify>,
}

impl DhtInner {
    async fn wait_for_local_info_announced(&self) {
        let notified = self.local_info_announced_notify.notified();
        if self.local_info_pre_announced.load(Ordering::Acquire) {
            return;
        }

        notified.await;
    }

    async fn find_value(
        &self,
        network: &Network,
        key_hash: &[u8; 32],
        mode: DhtQueryMode,
    ) -> Option<Box<Value>> {
        self.find_value_queries
            .run(key_hash, || {
                let query = Query::new(
                    network.clone(),
                    &self.routing_table.lock().unwrap(),
                    key_hash,
                    self.config.max_k,
                    mode,
                );

                // NOTE: expression is intentionally split to drop the routing table guard
                Box::pin(query.find_value())
            })
            .await
    }

    async fn store_value(
        &self,
        network: &Network,
        value: &ValueRef<'_>,
        with_peer_info: bool,
    ) -> Result<()> {
        self.storage.insert(DhtValueSource::Local, value)?;

        let local_info = if with_peer_info {
            let mut info = self.local_peer_info.lock().unwrap();
            Some(
                info.get_or_insert_with(|| self.make_local_peer_info(network, now_sec()))
                    .clone(),
            )
        } else {
            None
        };

        let key_hash = match value {
            ValueRef::Peer(value) => tl_proto::hash(&value.key),
            ValueRef::Merged(value) => tl_proto::hash(&value.key),
        };

        let local_peers = {
            let table = self.routing_table.lock().unwrap();
            table.closest(&key_hash, self.config.max_k * 2)
        };

        let query = {
            let table = self.routing_table.lock().unwrap();
            Query::new(
                network.clone(),
                &table,
                &key_hash,
                self.config.max_k,
                DhtQueryMode::Closest,
            )
        };
        let lookup_peers = query.find_peers(Some(3)).await;

        let mut candidates = FastHashMap::new();
        for peer in local_peers {
            candidates.insert(peer.id, peer);
        }
        for (_, peer) in lookup_peers {
            // Ensure the peer is known so store requests can connect.
            let _ = network.known_peers().insert(peer.clone(), false);
            candidates.insert(peer.id, peer);
        }

        let mut chosen = candidates.into_values().collect::<Vec<_>>();
        chosen.sort_by_key(|peer| xor_distance(&peer.id, PeerId::wrap(&key_hash)));
        chosen.truncate(self.config.max_k);

        StoreValue::new_with_peers(network.clone(), chosen, value, local_info.as_ref())
            .run()
            .await;

        Ok(())
    }

    fn store_value_locally(&self, value: &ValueRef<'_>) -> Result<bool, StorageError> {
        self.storage.insert(DhtValueSource::Local, value)
    }

    // NOTE: Requires the incoming peer info to be valid.
    fn add_peer_info(&self, network: &Network, peer_info: Arc<PeerInfo>) -> bool {
        if peer_info.id == self.local_id {
            return false;
        }

        let mut routing_table = self.routing_table.lock().unwrap();
        let added = routing_table.add(
            peer_info.clone(),
            self.config.max_k,
            &self.config.max_peer_info_ttl,
            |peer_info| network.known_peers().insert(peer_info, false).ok(),
        );
        drop(routing_table);

        if added {
            self.peer_added.notify_waiters();
        }

        added
    }

    fn add_allow_outdated_peer_info(&self, network: &Network, peer_info: Arc<PeerInfo>) -> bool {
        if peer_info.id == self.local_id {
            return false;
        }

        let mut added = false;

        let mut routing_table = self.routing_table.lock().unwrap();
        if !routing_table.contains(&peer_info.id) {
            added = routing_table.add(
                peer_info.clone(),
                self.config.max_k,
                &self.config.max_peer_info_ttl,
                |peer_info| {
                    network
                        .known_peers()
                        .insert_allow_outdated(peer_info, false)
                        .ok()
                },
            );
        }
        drop(routing_table);

        if added {
            self.peer_added.notify_waiters();
        }

        added
    }

    fn make_unsigned_peer_value<'a>(
        &'a self,
        name: PeerValueKeyName,
        data: &'a [u8],
        expires_at: u32,
    ) -> PeerValueRef<'a> {
        PeerValueRef {
            key: PeerValueKeyRef {
                name,
                peer_id: &self.local_id,
            },
            data,
            expires_at,
            signature: &[0; 64],
        }
    }

    fn make_local_peer_info(&self, network: &Network, now: u32) -> PeerInfo {
        network.sign_peer_info(now, self.config.max_peer_info_ttl.as_secs() as u32)
    }

    fn try_handle_prefix<'a>(&self, req: &'a ServiceRequest) -> Result<(u32, &'a [u8])> {
        let mut body = req.as_ref();
        anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

        // NOTE: read constructor without advancing the body
        let mut constructor = std::convert::identity(body).get_u32_le();

        if constructor == rpc::WithPeerInfo::TL_ID {
            metrics::counter!(METRIC_IN_REQ_WITH_PEER_INFO_TOTAL).increment(1);

            let peer_info = rpc::WithPeerInfo::read_from(&mut body)?.peer_info;
            anyhow::ensure!(
                peer_info.id == req.metadata.peer_id,
                "suggested peer ID does not belong to the sender"
            );

            anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);
            self.announced_peers.send(Arc::new(peer_info)).ok();

            // NOTE: read constructor without advancing the body
            constructor = std::convert::identity(body).get_u32_le();
        }

        Ok((constructor, body))
    }

    fn handle_store(&self, req: &rpc::StoreRef<'_>) -> Result<bool, StorageError> {
        self.storage.insert(DhtValueSource::Remote, &req.value)
    }

    fn handle_find_node(&self, req: &rpc::FindNode) -> NodeResponse {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, (req.k as usize).min(self.config.max_k));

        NodeResponse { nodes }
    }

    fn handle_find_value(&self, req: &rpc::FindValue) -> ValueResponseRaw {
        if let Some(value) = self.storage.get(&req.key) {
            ValueResponseRaw::Found(value)
        } else {
            let nodes = self
                .routing_table
                .lock()
                .unwrap()
                .closest(&req.key, (req.k as usize).min(self.config.max_k));

            ValueResponseRaw::NotFound(nodes)
        }
    }

    fn handle_get_node_info(&self) -> Option<NodeInfoResponse> {
        self.local_peer_info
            .lock()
            .unwrap()
            .clone()
            .map(|info| NodeInfoResponse { info })
    }
}

fn random_key_at_distance(from: &PeerId, distance: usize, rng: &mut impl RngCore) -> PeerId {
    let distance = MAX_XOR_DISTANCE - distance;

    let mut result = *from;

    let byte_offset = distance / 8;
    rng.fill_bytes(&mut result.0[byte_offset..]);

    let bit_offset = distance % 8;
    if bit_offset != 0 {
        let mask = 0xff >> bit_offset;
        result.0[byte_offset] ^= (result.0[byte_offset] ^ from.0[byte_offset]) & !mask;
    }

    result
}

pub fn xor_distance(left: &PeerId, right: &PeerId) -> usize {
    for (i, (left, right)) in std::iter::zip(left.0.chunks(8), right.0.chunks(8)).enumerate() {
        let left = u64::from_be_bytes(left.try_into().unwrap());
        let right = u64::from_be_bytes(right.try_into().unwrap());
        let diff = left ^ right;
        if diff != 0 {
            return MAX_XOR_DISTANCE - (i * 64 + diff.leading_zeros() as usize);
        }
    }

    0
}

const MAX_XOR_DISTANCE: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum FindValueError {
    #[error("failed to deserialize value: {0}")]
    InvalidData(#[from] tl_proto::TlError),
    #[error("value not found")]
    NotFound,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proper_random_keys() {
        let peer_id = rand::random();
        let random_id = random_key_at_distance(&peer_id, 20, &mut rand::rng());
        println!("{peer_id}");
        println!("{random_id}");

        let distance = xor_distance(&peer_id, &random_id);
        println!("{distance}");
        assert!(distance <= 23);
    }
}
