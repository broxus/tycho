use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::{Buf, Bytes};
use tl_proto::TlWrite;
use tycho_util::time::now_sec;

use self::query::Query;
use self::routing::RoutingTable;
use self::storage::{Storage, StorageError};
use crate::network::{Network, WeakNetwork};
use crate::proto::dht;
use crate::types::{AddressList, InboundServiceRequest, PeerId, Request, Response, Service};
use crate::util::{NetworkExt, Routable};

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

    pub fn add_peer(&self, peer: Arc<dht::NodeInfo>) {
        self.inner
            .routing_table
            .lock()
            .unwrap()
            .add(peer, self.inner.max_k, &self.inner.node_ttl);
    }

    pub async fn get_node_info(&self, peer_id: &PeerId) -> Result<dht::NodeInfo> {
        self.network
            .query(peer_id, Request::from_tl(dht::rpc::GetNodeInfo))
            .await?
            .parse_tl()
            .map_err(Into::into)
    }

    pub async fn find_peers(&self, target_id: &[u8; 32]) -> Result<Vec<Arc<dht::NodeInfo>>> {
        let max_k = self.inner.max_k;
        let closest_nodes = {
            let routing_table = self.inner.routing_table.lock().unwrap();
            routing_table.closest(target_id, max_k)
        };
        // TODO: deduplicate shared futures
        let nodes = Query::new(self.network.clone(), target_id, closest_nodes, max_k)
            .find_peers()
            .await;
        Ok(nodes.into_values().collect())
    }

    pub async fn find_value<T>(&self, key: &T) -> Option<Result<T::Value<'static>>>
    where
        T: dht::WithValue,
    {
        let res = self.find_value_impl(&tl_proto::hash(key)).await?;
        Some(res.and_then(|value| T::parse_value(value).map_err(Into::into)))
    }

    async fn find_value_impl(&self, hash: &[u8; 32]) -> Option<Result<Box<dht::Value>>> {
        let max_k = self.inner.max_k;
        let closest_nodes = {
            let routing_table = self.inner.routing_table.lock().unwrap();
            routing_table.closest(hash, max_k)
        };
        // TODO: deduplicate shared futures
        Query::new(self.network.clone(), hash, closest_nodes, max_k)
            .find_value()
            .await
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

    pub fn with_max_k(mut self, max_k: usize) -> Self {
        self.max_k = max_k;
        self
    }

    pub fn with_node_ttl(mut self, ttl: Duration) -> Self {
        self.node_ttl = ttl;
        self
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
        const INFO_UPDATE_INTERVAL: Duration = Duration::from_secs(60);
        const ANNOUNCE_EVERY_N_STEPS: usize = 10;

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background DHT loop started");
            let mut interval = tokio::time::interval(INFO_UPDATE_INTERVAL);
            let mut step = 0;
            loop {
                interval.tick().await;
                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                this.refresh_local_node_info(&network, INFO_TTL);

                step = (step + 1) % ANNOUNCE_EVERY_N_STEPS;
                if step == 0 {
                    if let Err(e) = this.announce_local_node_info(&network).await {
                        tracing::error!("failed to announce local DHT node info: {e:?}");
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
        let signature = network.sign_tl(&node_info);
        node_info.signature = signature.to_vec().into();

        *self.node_info.lock().unwrap() = Some(node_info);
    }

    async fn announce_local_node_info(self: &Arc<Self>, _network: &Network) -> Result<()> {
        // TODO: store node info in the DHT
        todo!()
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
