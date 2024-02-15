use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::{Buf, Bytes};
use tycho_util::realloc_box_enum;
use tycho_util::time::{now_sec, shifted_interval};

use self::query::{Query, StoreValue};
use self::routing::RoutingTable;
use self::storage::Storage;
use crate::network::{Network, WeakNetwork};
use crate::proto::dht::{
    rpc, NodeInfoResponse, NodeResponse, PeerValue, PeerValueKey, PeerValueKeyName,
    PeerValueKeyRef, PeerValueRef, Value, ValueRef, ValueResponseRaw,
};
use crate::types::{
    Address, PeerAffinity, PeerId, PeerInfo, Request, Response, Service, ServiceRequest,
};
use crate::util::{NetworkExt, Routable};

pub use self::config::DhtConfig;
pub use self::storage::{OverlayValueMerger, StorageError};

mod config;
mod query;
mod routing;
mod storage;

pub struct DhtClientBuilder {
    inner: Arc<DhtInner>,
    disable_background_tasks: bool,
}

impl DhtClientBuilder {
    pub fn disable_background_tasks(mut self) -> Self {
        self.disable_background_tasks = true;
        self
    }

    pub fn build(self, network: Network) -> DhtClient {
        if !self.disable_background_tasks {
            self.inner
                .start_background_tasks(Network::downgrade(&network));
        }

        DhtClient {
            inner: self.inner,
            network,
        }
    }
}

#[derive(Clone)]
pub struct DhtClient {
    inner: Arc<DhtInner>,
    network: Network,
}

impl DhtClient {
    pub fn network(&self) -> &Network {
        &self.network
    }

    pub fn add_peer(&self, peer: Arc<PeerInfo>) -> Result<bool> {
        self.inner.add_node_info(&self.network, peer)
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

        match self.inner.find_value(self.network, &key_hash).await {
            Some(value) => match value.as_ref() {
                Value::Peer(value) => {
                    tl_proto::deserialize(&value.data).map_err(FindValueError::InvalidData)
                }
                Value::Overlay(_) => Err(FindValueError::InvalidData(
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

        match self.inner.find_value(self.network, &key_hash).await {
            Some(value) => {
                realloc_box_enum!(value, {
                    Value::Peer(value) => Box::new(value) => Ok(value),
                    Value::Overlay(_) => Err(FindValueError::InvalidData(
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
            ttl: DEFAULT_TTL,
        }
    }
}

pub struct DhtQueryWithDataBuilder<'a> {
    inner: DhtQueryBuilder<'a>,
    data: Vec<u8>,
    at: Option<u32>,
    ttl: u32,
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

    pub async fn store(&self) -> Result<()> {
        let dht = self.inner.inner;
        let network = self.inner.network;

        let mut value = PeerValueRef {
            key: PeerValueKeyRef {
                name: self.inner.name,
                peer_id: &dht.local_id,
            },
            data: &self.data,
            expires_at: self.at.unwrap_or_else(now_sec) + self.ttl,
            signature: &[0; 64],
        };
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        dht.store_value(network, ValueRef::Peer(value)).await
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

pub struct DhtServiceBuilder {
    local_id: PeerId,
    config: Option<DhtConfig>,
    overlay_merger: Option<Arc<dyn OverlayValueMerger>>,
}

impl DhtServiceBuilder {
    pub fn with_config(mut self, config: DhtConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_overlay_value_merger<T: OverlayValueMerger>(mut self, merger: Arc<T>) -> Self {
        self.overlay_merger = Some(merger);
        self
    }

    pub fn build(self) -> (DhtClientBuilder, DhtService) {
        let config = self.config.unwrap_or_default();

        let storage = {
            let mut builder = Storage::builder()
                .with_max_capacity(config.max_storage_capacity)
                .with_max_ttl(config.max_stored_value_ttl);

            if let Some(time_to_idle) = config.storage_item_time_to_idle {
                builder = builder.with_max_idle(time_to_idle);
            }

            if let Some(ref merger) = self.overlay_merger {
                builder = builder.with_overlay_value_merger(merger);
            }

            builder.build()
        };

        let inner = Arc::new(DhtInner {
            local_id: self.local_id,
            routing_table: Mutex::new(RoutingTable::new(self.local_id)),
            storage,
            node_info: Mutex::new(None),
            max_k: config.max_k,
            node_ttl: config.max_node_info_ttl,
        });

        let client_builder = DhtClientBuilder {
            inner: inner.clone(),
            disable_background_tasks: false,
        };

        (client_builder, DhtService(inner))
    }
}

#[derive(Clone)]
pub struct DhtService(Arc<DhtInner>);

impl DhtService {
    pub fn builder(local_id: PeerId) -> DhtServiceBuilder {
        DhtServiceBuilder {
            local_id,
            config: None,
            overlay_merger: None,
        }
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
        let response = crate::match_tl_request!(req.body, {
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
            tracing::debug!("failed to deserialize query from: {e:?}");
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
        crate::match_tl_request!(req.body, {
            rpc::StoreRef<'_> as ref r => {
                tracing::debug!("store");

                if let Err(e) = self.0.handle_store(r) {
                    tracing::debug!("failed to store value: {e:?}");
                }
            }
        }, e => {
            tracing::debug!("failed to deserialize message from: {e:?}");
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
            rpc::FindNode::TL_ID,
            rpc::FindValue::TL_ID,
            rpc::GetNodeInfo::TL_ID,
        ]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::Store::TL_ID]
    }
}

struct DhtInner {
    local_id: PeerId,
    routing_table: Mutex<RoutingTable>,
    storage: Storage,
    node_info: Mutex<Option<PeerInfo>>,
    max_k: usize,
    node_ttl: Duration,
}

impl DhtInner {
    fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork) {
        const INFO_UPDATE_PERIOD: Duration = Duration::from_secs(60);

        const ANNOUNCE_PERIOD: Duration = Duration::from_secs(600);
        const ANNOUNCE_SHIFT: Duration = Duration::from_secs(60);
        const POPULATE_PERIOD: Duration = Duration::from_secs(60);
        const POPULATE_SHIFT: Duration = Duration::from_secs(10);

        enum Action {
            Refresh,
            Announce,
            Populate,
        }

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background DHT loop started");
            let mut refresh_interval = tokio::time::interval(INFO_UPDATE_PERIOD);
            let mut announce_interval = shifted_interval(ANNOUNCE_PERIOD, ANNOUNCE_SHIFT);
            let mut populate_interval = shifted_interval(POPULATE_PERIOD, POPULATE_SHIFT);

            loop {
                let action = tokio::select! {
                    _ = refresh_interval.tick() => Action::Refresh,
                    _ = announce_interval.tick() => Action::Announce,
                    _ = populate_interval.tick() => Action::Populate,
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::Refresh => {
                        this.refresh_local_node_info(&network, DEFAULT_TTL);
                    }
                    Action::Announce => {
                        // Always refresh node info before announcing
                        this.refresh_local_node_info(&network, DEFAULT_TTL);
                        refresh_interval.reset();

                        if let Err(e) = this.announce_local_node_info(&network, DEFAULT_TTL).await {
                            tracing::error!("failed to announce local DHT node info: {e:?}");
                        }
                    }
                    Action::Populate => {
                        // TODO: spawn and await in the background?
                        this.find_more_dht_nodes(&network).await;
                    }
                }
            }
            tracing::debug!("background DHT loop finished");
        });
    }

    fn refresh_local_node_info(&self, network: &Network, ttl: u32) {
        let now = now_sec();
        let mut node_info = PeerInfo {
            id: self.local_id,
            address_list: vec![network.local_addr().into()].into_boxed_slice(),
            created_at: now,
            expires_at: now + ttl,
            signature: Box::new([0; 64]),
        };
        *node_info.signature = network.sign_tl(&node_info);

        *self.node_info.lock().unwrap() = Some(node_info);
    }

    async fn announce_local_node_info(&self, network: &Network, ttl: u32) -> Result<()> {
        let data = tl_proto::serialize(&[network.local_addr().into()] as &[Address]);

        let mut value =
            self.make_unsigned_peer_value(PeerValueKeyName::NodeInfo, &data, now_sec() + ttl);
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        self.store_value(network, ValueRef::Peer(value)).await
    }

    async fn find_more_dht_nodes(&self, network: &Network) {
        // TODO: deduplicate shared futures
        let query = Query::new(
            network.clone(),
            &self.routing_table.lock().unwrap(),
            self.local_id.as_bytes(),
            self.max_k,
        );

        // NOTE: expression is intentionally split to drop the routing table guard
        let peers = query.find_peers().await;

        let mut routing_table = self.routing_table.lock().unwrap();
        for peer in peers {
            routing_table.add(peer, self.max_k, &self.node_ttl);
        }
    }

    async fn find_value(&self, network: &Network, key_hash: &[u8; 32]) -> Option<Box<Value>> {
        // TODO: deduplicate shared futures
        let query = Query::new(
            network.clone(),
            &self.routing_table.lock().unwrap(),
            key_hash,
            self.max_k,
        );

        // NOTE: expression is intentionally split to drop the routing table guard
        query.find_value().await
    }

    async fn store_value(&self, network: &Network, value: ValueRef<'_>) -> Result<()> {
        self.storage.insert(&value)?;

        let query = StoreValue::new(
            network.clone(),
            &self.routing_table.lock().unwrap(),
            value,
            self.max_k,
        );

        // NOTE: expression is intentionally split to drop the routing table guard
        query.run().await;
        Ok(())
    }

    fn add_node_info(&self, network: &Network, node: Arc<PeerInfo>) -> Result<bool> {
        anyhow::ensure!(node.is_valid(now_sec()), "invalid peer node info");

        if node.id == self.local_id {
            return Ok(false);
        }

        let mut routing_table = self.routing_table.lock().unwrap();
        let is_new = routing_table.add(node.clone(), self.max_k, &self.node_ttl);
        if is_new {
            network.known_peers().insert(node, PeerAffinity::Allowed);
        }
        Ok(is_new)
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

    fn handle_store(&self, req: &rpc::StoreRef<'_>) -> Result<bool, StorageError> {
        self.storage.insert(&req.value)
    }

    fn handle_find_node(&self, req: &rpc::FindNode) -> NodeResponse {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, (req.k as usize).min(self.max_k));

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
                .closest(&req.key, (req.k as usize).min(self.max_k));

            ValueResponseRaw::NotFound(nodes)
        }
    }

    fn handle_get_node_info(&self) -> Option<NodeInfoResponse> {
        self.node_info
            .lock()
            .unwrap()
            .clone()
            .map(|info| NodeInfoResponse { info })
    }
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

const DEFAULT_TTL: u32 = 3600;
