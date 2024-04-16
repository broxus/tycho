use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::{Buf, Bytes};
use rand::RngCore;
use tl_proto::TlRead;
use tokio::sync::broadcast;
use tycho_util::realloc_box_enum;
use tycho_util::time::now_sec;

use self::query::{Query, QueryCache, StoreValue};
use self::routing::HandlesRoutingTable;
use self::storage::Storage;
use crate::network::Network;
use crate::proto::dht::{
    rpc, NodeInfoResponse, NodeResponse, PeerValue, PeerValueKey, PeerValueKeyName,
    PeerValueKeyRef, PeerValueRef, Value, ValueRef, ValueResponseRaw,
};
use crate::types::{PeerId, PeerInfo, Request, Response, Service, ServiceRequest};
use crate::util::{NetworkExt, Routable};

pub use self::config::DhtConfig;
pub use self::peer_resolver::{PeerResolver, PeerResolverBuilder, PeerResolverHandle};
pub use self::query::DhtQueryMode;
pub use self::storage::{DhtValueMerger, DhtValueSource, StorageError};

mod background_tasks;
mod config;
mod peer_resolver;
mod query;
mod routing;
mod storage;

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
        self.inner.add_peer_info(&self.network, peer)
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

impl<'a> std::ops::DerefMut for DhtQueryWithDataBuilder<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct DhtServiceBackgroundTasks {
    inner: Arc<DhtInner>,
}

impl DhtServiceBackgroundTasks {
    pub fn spawn(self, network: &Network) {
        self.inner
            .start_background_tasks(Network::downgrade(network));
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
}

impl Service<ServiceRequest> for DhtService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_dht_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match self.0.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                return futures_util::future::ready(None);
            }
        };

        let response = crate::match_tl_request!(body, tag = constructor, {
            rpc::FindNode as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_node");

                let res = self.0.handle_find_node(r);
                Some(tl_proto::serialize(res))
            },
            rpc::FindValue as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_value");

                let res = self.0.handle_find_value(r);
                Some(tl_proto::serialize(res))
            },
            rpc::GetNodeInfo as _ => {
                tracing::debug!("get_node_info");

                self.0.handle_get_node_info().map(tl_proto::serialize)
            },
        }, e => {
            tracing::debug!("failed to deserialize query: {e}");
            None
        });

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
        let (constructor, body) = match self.0.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize message: {e}");
                return futures_util::future::ready(());
            }
        };

        crate::match_tl_request!(body, tag = constructor, {
            rpc::StoreRef<'_> as ref r => {
                tracing::debug!("store");

                if let Err(e) = self.0.handle_store(r) {
                    tracing::debug!("failed to store value: {e}");
                }
            }
        }, e => {
            tracing::debug!("failed to deserialize message: {e}");
        });

        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
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
}

impl DhtInner {
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

        let local_peer_info = if with_peer_info {
            let mut node_info = self.local_peer_info.lock().unwrap();
            Some(
                node_info
                    .get_or_insert_with(|| self.make_local_peer_info(network, now_sec()))
                    .clone(),
            )
        } else {
            None
        };

        let query = StoreValue::new(
            network.clone(),
            &self.routing_table.lock().unwrap(),
            value,
            self.config.max_k,
            local_peer_info.as_ref(),
        );

        // NOTE: expression is intentionally split to drop the routing table guard
        query.run().await;
        Ok(())
    }

    fn store_value_locally(&self, value: &ValueRef<'_>) -> Result<bool, StorageError> {
        self.storage.insert(DhtValueSource::Local, value)
    }

    fn add_peer_info(&self, network: &Network, peer_info: Arc<PeerInfo>) -> Result<bool> {
        anyhow::ensure!(peer_info.is_valid(now_sec()), "invalid peer info");

        if peer_info.id == self.local_id {
            return Ok(false);
        }

        let mut routing_table = self.routing_table.lock().unwrap();
        Ok(routing_table.add(
            peer_info.clone(),
            self.config.max_k,
            &self.config.max_peer_info_ttl,
            |peer_info| network.known_peers().insert(peer_info, false).ok(),
        ))
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
        let mut offset = 0;

        if constructor == rpc::WithPeerInfo::TL_ID {
            let peer_info = rpc::WithPeerInfo::read_from(body, &mut offset)?.peer_info;
            anyhow::ensure!(
                peer_info.id == req.metadata.peer_id,
                "suggested peer ID does not belong to the sender"
            );
            self.announced_peers.send(Arc::new(peer_info)).ok();

            body = &body[offset..];
            anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

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
        let random_id = random_key_at_distance(&peer_id, 20, &mut rand::thread_rng());
        println!("{peer_id}");
        println!("{random_id}");

        let distance = xor_distance(&peer_id, &random_id);
        println!("{distance}");
        assert!(distance <= 23);
    }
}
