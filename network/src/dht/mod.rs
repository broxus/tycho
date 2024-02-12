use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::{Buf, Bytes};
use tl_proto::TlWrite;
use tycho_util::time::now_sec;

use self::routing::RoutingTable;
use self::storage::{Storage, StorageError};
use crate::proto::dht;
use crate::types::{PeerId, Response, Service};
use crate::util::Routable;
use crate::{AddressList, InboundServiceRequest, Network, WeakNetwork};

pub use self::routing::RoutingTableBuilder;
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
        self.inner.routing_table.lock().unwrap().add(peer);
    }

    pub async fn find_peers(&self, key: &PeerId) -> Result<Vec<PeerId>> {
        todo!()
    }

    pub async fn find_value<T>(&self, key: T) -> Result<T::Value<'static>>
    where
        T: dht::WithValue,
    {
        todo!()
    }
}

pub struct DhtServiceBuilder<MandatoryFields = (RoutingTable, Storage)> {
    mandatory_fields: MandatoryFields,
    local_id: PeerId,
}

impl DhtServiceBuilder {
    pub fn build(self) -> (DhtClientBuilder, DhtService) {
        let (routing_table, storage) = self.mandatory_fields;

        let inner = Arc::new(DhtInner {
            local_id: self.local_id,
            routing_table: Mutex::new(routing_table),
            storage,
            node_info: Mutex::new(None),
        });

        let client_builder = DhtClientBuilder {
            inner: inner.clone(),
            disable_background_tasks: false,
        };

        (client_builder, DhtService(inner))
    }
}

impl<T1> DhtServiceBuilder<(T1, ())> {
    pub fn with_storage<F>(self, f: F) -> DhtServiceBuilder<(T1, Storage)>
    where
        F: FnOnce(StorageBuilder) -> StorageBuilder,
    {
        let (routing_table, _) = self.mandatory_fields;
        let storage = f(Storage::builder()).build();
        DhtServiceBuilder {
            mandatory_fields: (routing_table, storage),
            local_id: self.local_id,
        }
    }
}

impl<T2> DhtServiceBuilder<((), T2)> {
    pub fn with_routing_table<F>(self, f: F) -> DhtServiceBuilder<(RoutingTable, T2)>
    where
        F: FnOnce(RoutingTableBuilder) -> RoutingTableBuilder,
    {
        let routing_table = f(RoutingTable::builder(self.local_id)).build();
        let (_, storage) = self.mandatory_fields;
        DhtServiceBuilder {
            mandatory_fields: (routing_table, storage),
            local_id: self.local_id,
        }
    }
}

#[derive(Clone)]
pub struct DhtService(Arc<DhtInner>);

impl DhtService {
    pub fn builder(local_id: PeerId) -> DhtServiceBuilder<((), ())> {
        DhtServiceBuilder {
            mandatory_fields: ((), ()),
            local_id,
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
            dht::rpc::FindNode as r => {
                let res = self.0.handle_find_node(r);
                Some(tl_proto::serialize(res))
            },
            dht::rpc::FindValue as r => {
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
            dht::rpc::Store as r => match self.0.handle_store(r) {
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

    fn handle_store(&self, req: dht::rpc::Store) -> Result<bool, StorageError> {
        self.storage.insert(&req.value)
    }

    fn handle_find_node(&self, req: dht::rpc::FindNode) -> NodeResponseRaw {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, req.k as usize);

        NodeResponseRaw { nodes }
    }

    fn handle_find_value(&self, req: dht::rpc::FindValue) -> ValueResponseRaw {
        match self.storage.get(&req.key) {
            Some(value) => ValueResponseRaw::Found(value),
            None => {
                let nodes = self
                    .routing_table
                    .lock()
                    .unwrap()
                    .closest(&req.key, req.k as usize);

                ValueResponseRaw::NotFound(nodes.into_iter().map(|node| node.into()).collect())
            }
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
