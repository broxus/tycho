use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::{Buf, Bytes};
use tl_proto::TlWrite;
use tycho_util::time::{now_sec, shifted_interval};

use self::query::{Query, StoreValue};
use self::routing::RoutingTable;
use self::storage::{Storage, StorageError};
use crate::network::{Network, WeakNetwork};
use crate::proto::dht;
use crate::types::{
    AddressList, InboundServiceRequest, PeerAffinity, PeerId, PeerInfo, Request, Response, Service,
};
use crate::util::{validate_signature, NetworkExt, Routable};

pub use self::storage::StorageBuilder;

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
        self.inner.add_peer(&self.network, peer)
    }

    pub async fn get_node_info(&self, peer_id: &PeerId) -> Result<dht::NodeInfo> {
        let res = self
            .network
            .query(peer_id, Request::from_tl(dht::rpc::GetNodeInfo))
            .await?;
        let dht::NodeInfoResponse { info } = res.parse_tl()?;
        Ok(info)
    }

    pub async fn find_value<T>(&self, key: &T) -> Option<Result<T::Value<'static>>>
    where
        T: dht::WithValue,
    {
        let res = self
            .inner
            .find_value(&self.network, &tl_proto::hash(key))
            .await?;
        Some(res.and_then(|value| T::parse_value(value).map_err(Into::into)))
    }

    pub async fn store_value(&self, value: Box<dht::Value>) -> Result<()> {
        self.inner.store_value(&self.network, value).await
    }
}

pub struct DhtServiceBuilder<MandatoryFields = Storage> {
    mandatory_fields: MandatoryFields,
    local_id: PeerId,
    node_ttl: Duration,
    max_k: usize,
}

impl DhtServiceBuilder {
    pub fn build(self) -> (DhtClientBuilder, DhtService) {
        let storage = self.mandatory_fields;

        let inner = Arc::new(DhtInner {
            local_id: self.local_id,
            routing_table: Mutex::new(RoutingTable::new(self.local_id)),
            storage,
            node_info: Mutex::new(None),
            max_k: self.max_k,
            node_ttl: self.node_ttl,
        });

        let client_builder = DhtClientBuilder {
            inner: inner.clone(),
            disable_background_tasks: false,
        };

        (client_builder, DhtService(inner))
    }
}

impl DhtServiceBuilder<()> {
    pub fn with_storage<F>(self, f: F) -> DhtServiceBuilder<Storage>
    where
        F: FnOnce(StorageBuilder) -> StorageBuilder,
    {
        let storage = f(Storage::builder()).build();
        DhtServiceBuilder {
            mandatory_fields: storage,
            local_id: self.local_id,
            node_ttl: self.node_ttl,
            max_k: self.max_k,
        }
    }
}

impl<T> DhtServiceBuilder<T> {
    pub fn with_max_k(mut self, max_k: usize) -> Self {
        self.max_k = max_k;
        self
    }

    pub fn with_node_ttl(mut self, ttl: Duration) -> Self {
        self.node_ttl = ttl;
        self
    }
}

#[derive(Clone)]
pub struct DhtService(Arc<DhtInner>);

impl DhtService {
    pub fn builder(local_id: PeerId) -> DhtServiceBuilder<()> {
        DhtServiceBuilder {
            mandatory_fields: (),
            local_id,
            node_ttl: Duration::from_secs(15 * 60),
            max_k: 20,
        }
    }
}

impl Service<InboundServiceRequest<Bytes>> for DhtService {
    type QueryResponse = Response<Bytes>;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: InboundServiceRequest<Bytes>) -> Self::OnQueryFuture {
        tracing::debug!(
            peer_id = %req.metadata.peer_id,
            addr = %req.metadata.remote_address,
            byte_len = req.body.len(),
            "processing DHT query",
        );

        let response = crate::match_tl_request!(req.body, {
            dht::rpc::FindNode as ref r => {
                let res = self.0.handle_find_node(r);
                Some(tl_proto::serialize(res))
            },
            dht::rpc::FindValue as ref r => {
                let res = self.0.handle_find_value(r);
                Some(tl_proto::serialize(res))
            },
            dht::rpc::GetNodeInfo as _ => {
                self.0.handle_get_node_info().map(tl_proto::serialize)
            },
        }, e => {
            tracing::debug!(
                peer_id = %req.metadata.peer_id,
                addr = %req.metadata.remote_address,
                "failed to deserialize query from: {e:?}"
            );
            None
        });

        futures_util::future::ready(response.map(|body| Response {
            version: Default::default(),
            body: Bytes::from(body),
        }))
    }

    #[inline]
    fn on_message(&self, req: InboundServiceRequest<Bytes>) -> Self::OnMessageFuture {
        tracing::debug!(
            peer_id = %req.metadata.peer_id,
            addr = %req.metadata.remote_address,
            byte_len = req.body.len(),
            "processing DHT message",
        );

        crate::match_tl_request!(req.body, {
            dht::rpc::Store as ref r => match self.0.handle_store(r) {
                Ok(_) => {},
                Err(e) => {
                    tracing::debug!(
                        peer_id = %req.metadata.peer_id,
                        addr = %req.metadata.remote_address,
                        "failed to store value: {e:?}"
                    );
                }
            }
        }, e => {
            tracing::debug!(
                peer_id = %req.metadata.peer_id,
                addr = %req.metadata.remote_address,
                "failed to deserialize message from: {e:?}"
            );
        });

        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: InboundServiceRequest<Bytes>) -> Self::OnDatagramFuture {
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

    async fn announce_local_node_info(self: &Arc<Self>, network: &Network, ttl: u32) -> Result<()> {
        let value = {
            let now = now_sec();

            let mut value = dht::SignedValue {
                key: dht::SignedKey {
                    name: "addr".to_owned().into(),
                    idx: 0,
                    peer_id: self.local_id,
                },
                data: Bytes::from(tl_proto::serialize(AddressList {
                    items: vec![network.local_addr().into()],
                    created_at: now,
                    expires_at: now + ttl,
                })),
                expires_at: now_sec() + ttl,
                signature: Bytes::new(),
            };
            value.signature = network.sign_tl(&value).to_vec().into();
            Box::new(dht::Value::Signed(value))
        };

        self.store_value(network, value).await
    }

    async fn find_more_dht_nodes(self: &Arc<Self>, network: &Network) {
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

    async fn find_value(
        &self,
        network: &Network,
        key_hash: &[u8; 32],
    ) -> Option<Result<Box<dht::Value>>> {
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

    fn add_peer(&self, network: &Network, peer: Arc<dht::NodeInfo>) -> Result<bool> {
        anyhow::ensure!(
            validate_node_info(now_sec(), &peer),
            "invalid peer node info"
        );

        // TODO: add support for multiple addresses
        let peer_info = match peer.address_list.items.first() {
            Some(address) if peer.id != self.local_id => PeerInfo {
                peer_id: peer.id,
                affinity: PeerAffinity::Allowed,
                address: address.clone(),
            },
            _ => return Ok(false),
        };

        let mut routing_table = self.routing_table.lock().unwrap();
        let is_new = routing_table.add(peer, self.max_k, &self.node_ttl);
        if is_new {
            network.known_peers().insert(peer_info);
        }
        Ok(is_new)
    }

    fn handle_store(&self, req: &dht::rpc::Store) -> Result<bool, StorageError> {
        self.storage.insert(&req.value)
    }

    fn handle_find_node(&self, req: &dht::rpc::FindNode) -> NodeResponseRaw {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, req.k as usize);

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
                .closest(&req.key, req.k as usize);

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

fn validate_node_info(now: u32, info: &dht::NodeInfo) -> bool {
    info.created_at <= now + CLOCK_THRESHOLD
        && info.address_list.created_at <= now + CLOCK_THRESHOLD
        && info.address_list.expires_at >= now
        && !info.address_list.items.is_empty()
        && validate_signature(&info.id, &info.signature, info)
}

fn validate_value(now: u32, key: &[u8; 32], value: &dht::Value) -> bool {
    match value {
        dht::Value::Signed(value) => {
            value.expires_at >= now
                && key == &tl_proto::hash(&value.key)
                && validate_signature(&value.key.peer_id, &value.signature, value)
        }
        dht::Value::Overlay(value) => value.expires_at >= now && key == &tl_proto::hash(&value.key),
    }
}

#[derive(TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
enum ValueResponseRaw {
    #[tl(id = "dht.valueFound")]
    Found(Bytes),
    #[tl(id = "dht.valueNotFound")]
    NotFound(Vec<Arc<dht::NodeInfo>>),
}

#[derive(TlWrite)]
#[tl(boxed, id = "dht.nodesFound", scheme = "proto.tl")]
struct NodeResponseRaw {
    nodes: Vec<Arc<dht::NodeInfo>>,
}

const CLOCK_THRESHOLD: u32 = 1;
