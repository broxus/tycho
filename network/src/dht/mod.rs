use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::{Buf, Bytes};
use tl_proto::TlWrite;
use tycho_util::time::{now_sec, shifted_interval};

use self::query::{Query, StoreValue};
use self::routing::RoutingTable;
use self::storage::Storage;
use crate::network::{Network, WeakNetwork};
use crate::proto::dht;
use crate::types::{
    AddressList, PeerAffinity, PeerId, PeerInfo, Request, Response, Service, ServiceRequest,
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

    pub fn add_peer(&self, peer: Arc<dht::NodeInfo>) -> Result<bool> {
        self.inner.add_node_info(&self.network, peer)
    }

    pub async fn get_node_info(&self, peer_id: &PeerId) -> Result<dht::NodeInfo> {
        let res = self
            .network
            .query(peer_id, Request::from_tl(dht::rpc::GetNodeInfo))
            .await?;
        let dht::NodeInfoResponse { info } = res.parse_tl()?;
        Ok(info)
    }

    pub async fn find_value<T>(&self, key: &T) -> Result<T::Value<'static>, FindValueError>
    where
        T: dht::WithValue,
    {
        match self
            .inner
            .find_value(&self.network, &tl_proto::hash(key))
            .await
        {
            Some(value) => T::parse_value(value).map_err(FindValueError::InvalidData),
            None => Err(FindValueError::NotFound),
        }
    }

    pub async fn store_value(&self, value: Box<dht::Value>) -> Result<()> {
        self.inner.store_value(&self.network, value).await
    }

    pub fn make_signed_value<T>(&self, name: &str, expires_at: u32, data: T) -> Box<dht::Value>
    where
        T: TlWrite + 'static,
    {
        self.inner
            .make_signed_value(&self.network, name, expires_at, data)
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
                .with_max_key_name_len(config.max_stored_key_name_len)
                .with_max_key_index(config.max_stored_key_index)
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
            dht::rpc::FindNode as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_node");

                let res = self.0.handle_find_node(r);
                Some(tl_proto::serialize(res))
            },
            dht::rpc::FindValue as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_value");

                let res = self.0.handle_find_value(r);
                Some(tl_proto::serialize(res))
            },
            dht::rpc::GetNodeInfo as _ => {
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
            dht::rpc::Store as ref r => {
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
            dht::rpc::FindNode::TL_ID,
            dht::rpc::FindValue::TL_ID,
            dht::rpc::GetNodeInfo::TL_ID,
        ]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [dht::rpc::Store::TL_ID]
    }
}

struct DhtInner {
    local_id: PeerId,
    routing_table: Mutex<RoutingTable>,
    storage: Storage,
    node_info: Mutex<Option<dht::NodeInfo>>,
    max_k: usize,
    node_ttl: Duration,
}

impl DhtInner {
    fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork) {
        const INFO_TTL: u32 = 3600;
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
                        this.refresh_local_node_info(&network, INFO_TTL);
                    }
                    Action::Announce => {
                        // Always refresh node info before announcing
                        this.refresh_local_node_info(&network, INFO_TTL);
                        refresh_interval.reset();

                        if let Err(e) = this.announce_local_node_info(&network, INFO_TTL).await {
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
        let mut node_info = dht::NodeInfo {
            id: self.local_id,
            address_list: AddressList {
                items: vec![network.local_addr().into()],
                created_at: now,
                expires_at: now + ttl,
            },
            created_at: now,
            signature: Bytes::new(),
        };
        node_info.signature = network.sign_tl(&node_info).to_vec().into();

        *self.node_info.lock().unwrap() = Some(node_info);
    }

    async fn announce_local_node_info(&self, network: &Network, ttl: u32) -> Result<()> {
        let value = {
            let created_at = now_sec();
            let expires_at = created_at + ttl;

            self.make_signed_value(
                network,
                "addr",
                expires_at,
                AddressList {
                    items: vec![network.local_addr().into()],
                    created_at,
                    expires_at,
                },
            )
        };

        self.store_value(network, value).await
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

    async fn find_value(&self, network: &Network, key_hash: &[u8; 32]) -> Option<Box<dht::Value>> {
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

    async fn store_value(&self, network: &Network, value: Box<dht::Value>) -> Result<()> {
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

    fn add_node_info(&self, network: &Network, node: Arc<dht::NodeInfo>) -> Result<bool> {
        anyhow::ensure!(node.is_valid(now_sec()), "invalid peer node info");

        // TODO: add support for multiple addresses
        let peer_info = match node.address_list.items.first() {
            Some(address) if node.id != self.local_id => PeerInfo {
                peer_id: node.id,
                affinity: PeerAffinity::Allowed,
                address: address.clone(),
            },
            _ => return Ok(false),
        };

        let mut routing_table = self.routing_table.lock().unwrap();
        let is_new = routing_table.add(node, self.max_k, &self.node_ttl);
        if is_new {
            network.known_peers().insert(peer_info);
        }
        Ok(is_new)
    }

    fn make_signed_value<T>(
        &self,
        network: &Network,
        name: &str,
        expires_at: u32,
        data: T,
    ) -> Box<dht::Value>
    where
        T: TlWrite + 'static,
    {
        let mut value = dht::SignedValue {
            key: dht::SignedKey {
                name: name.to_owned().into(),
                idx: 0,
                peer_id: self.local_id,
            },
            data: match castaway::cast!(data, Bytes) {
                Ok(data) => data,
                Err(data) => tl_proto::serialize(data).into(),
            },
            expires_at,
            signature: Default::default(),
        };
        value.signature = network.sign_tl(&value).to_vec().into();

        Box::new(dht::Value::Signed(value))
    }

    fn handle_store(&self, req: &dht::rpc::Store) -> Result<bool, StorageError> {
        self.storage.insert(&req.value)
    }

    fn handle_find_node(&self, req: &dht::rpc::FindNode) -> NodeResponseRaw {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, (req.k as usize).min(self.max_k));

        NodeResponseRaw { nodes }
    }

    fn handle_find_value(&self, req: &dht::rpc::FindValue) -> ValueResponseRaw {
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

    fn handle_get_node_info(&self) -> Option<dht::NodeInfoResponse> {
        self.node_info
            .lock()
            .unwrap()
            .clone()
            .map(|info| dht::NodeInfoResponse { info })
    }
}

enum ValueResponseRaw {
    Found(Bytes),
    NotFound(Vec<Arc<dht::NodeInfo>>),
}

impl TlWrite for ValueResponseRaw {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + match self {
            Self::Found(value) => value.max_size_hint(),
            Self::NotFound(nodes) => nodes.max_size_hint(),
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        const FOUND_TL_ID: u32 = tl_proto::id!("dht.valueFound", scheme = "proto.tl");
        const NOT_FOUND_TL_ID: u32 = tl_proto::id!("dht.valueNotFound", scheme = "proto.tl");

        match self {
            Self::Found(value) => {
                packet.write_u32(FOUND_TL_ID);
                packet.write_raw_slice(&value);
            }
            Self::NotFound(nodes) => {
                packet.write_u32(NOT_FOUND_TL_ID);
                nodes.write_to(packet);
            }
        }
    }
}

#[derive(TlWrite)]
#[tl(boxed, id = "dht.nodesFound", scheme = "proto.tl")]
struct NodeResponseRaw {
    nodes: Vec<Arc<dht::NodeInfo>>,
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
