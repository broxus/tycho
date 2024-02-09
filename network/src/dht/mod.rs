use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use bytes::{Buf, Bytes};
use futures_util::Future;
use tl_proto::TlWrite;

use self::routing::{RoutingTable, RoutingTableBuilder};
use self::storage::{Storage, StorageBuilder};
use crate::types::{PeerId, Response, Service};
use crate::util::{BoxFutureOrNoop, Routable};
use crate::{proto, InboundServiceRequest, Network};

mod routing;
mod storage;

pub struct DhtBuilder<MandatoryFields = (RoutingTable, Storage)> {
    mandatory_fields: MandatoryFields,
    local_id: PeerId,
}

impl DhtBuilder {
    pub fn build(self) -> (DhtClientBuilder, DhtService) {
        let (routing_table, storage) = self.mandatory_fields;

        let inner = Arc::new(DhtInner {
            local_id: self.local_id,
            routing_table: Mutex::new(routing_table),
            last_table_refresh: Instant::now(),
            storage,
            node_info: Mutex::new(None),
        });

        (DhtClientBuilder(inner.clone()), DhtService(inner))
    }
}

impl<T1> DhtBuilder<(T1, ())> {
    pub fn with_storage<F>(self, f: F) -> DhtBuilder<(T1, Storage)>
    where
        F: FnOnce(StorageBuilder) -> StorageBuilder,
    {
        let (routing_table, _) = self.mandatory_fields;
        let storage = f(Storage::builder()).build();
        DhtBuilder {
            mandatory_fields: (routing_table, storage),
            local_id: self.local_id,
        }
    }
}

impl<T2> DhtBuilder<((), T2)> {
    pub fn with_routing_table<F>(self, f: F) -> DhtBuilder<(RoutingTable, T2)>
    where
        F: FnOnce(RoutingTableBuilder) -> RoutingTableBuilder,
    {
        let routing_table = f(RoutingTable::builder(self.local_id)).build();
        let (_, storage) = self.mandatory_fields;
        DhtBuilder {
            mandatory_fields: (routing_table, storage),
            local_id: self.local_id,
        }
    }
}

pub struct DhtClientBuilder(Arc<DhtInner>);

impl DhtClientBuilder {
    pub fn build(self, network: Network) -> DhtClient {
        // TODO: spawn background tasks here:
        // - refresh routing table
        // - update and broadcast node info

        DhtClient {
            inner: self.0,
            network,
        }
    }
}

pub struct DhtService(Arc<DhtInner>);

impl DhtService {
    pub fn builder(local_id: PeerId) -> DhtBuilder<((), ())> {
        DhtBuilder {
            mandatory_fields: ((), ()),
            local_id,
        }
    }

    pub fn make_client(&self, network: Network) -> DhtClient {
        DhtClient {
            inner: self.0.clone(),
            network,
        }
    }
}

macro_rules! match_req {
    ($req:ident, {
        $($ty:path as $pat:pat => $expr:expr),*$(,)?
    }) => {{
        let e = if $req.body.len() >= 4 {
            match $req.body.as_ref().get_u32_le() {
                $(
                    <$ty>::TL_ID => match tl_proto::deserialize::<$ty>(&$req.body) {
                        Ok($pat) => return ($expr).boxed_or_noop(),
                        Err(e) => e,
                    }
                )*
                _ => tl_proto::TlError::UnknownConstructor,
            }
        } else {
            tl_proto::TlError::UnexpectedEof
        };
        tracing::debug!("failed to deserialize request: {e:?}");
        BoxFutureOrNoop::Noop
    }};
}

impl Service<InboundServiceRequest<Bytes>> for DhtService {
    type QueryResponse = Response<Bytes>;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: InboundServiceRequest<Bytes>) -> Self::OnQueryFuture {
        match_req!(req, {
            proto::dht::rpc::Store as req => self.0.clone().store(req),
            proto::dht::rpc::FindNode as req => self.0.clone().find_node(req),
            proto::dht::rpc::FindValue as req => self.0.clone().find_value(req),
            proto::dht::rpc::GetNodeInfo as _ => self.0.clone().get_node_info(),
        })
    }

    #[inline]
    fn on_message(&self, _req: InboundServiceRequest<Bytes>) -> Self::OnMessageFuture {
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
            proto::dht::rpc::Store::TL_ID,
            proto::dht::rpc::FindNode::TL_ID,
            proto::dht::rpc::FindValue::TL_ID,
            proto::dht::rpc::GetNodeInfo::TL_ID,
        ]
    }
}

pub struct DhtClient {
    inner: Arc<DhtInner>,
    network: Network,
}

impl DhtClient {
    pub async fn find_peers(&self, key: &PeerId) -> Result<Vec<PeerId>> {
        todo!()
    }

    pub async fn find_value<T>(&self, key: T) -> Result<T::Value<'static>>
    where
        T: proto::dht::WithValue,
    {
        todo!()
    }
}

struct DhtInner {
    local_id: PeerId,
    routing_table: Mutex<RoutingTable>,
    last_table_refresh: Instant,
    storage: Storage,
    node_info: Mutex<Option<proto::dht::NodeInfo>>,
}

impl DhtInner {
    async fn store(self: Arc<Self>, req: proto::dht::rpc::Store) -> Result<proto::dht::Stored> {
        self.storage.insert(&req.value)?;
        Ok(proto::dht::Stored)
    }

    async fn find_node(self: Arc<Self>, req: proto::dht::rpc::FindNode) -> Result<NodeResponseRaw> {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, req.k as usize);

        Ok(NodeResponseRaw { nodes })
    }

    async fn find_value(
        self: Arc<Self>,
        req: proto::dht::rpc::FindValue,
    ) -> Result<ValueResponseRaw> {
        match self.storage.get(&req.key) {
            Some(value) => Ok(ValueResponseRaw::Found(value)),
            None => {
                let nodes = self
                    .routing_table
                    .lock()
                    .unwrap()
                    .closest(&req.key, req.k as usize);

                Ok(ValueResponseRaw::NotFound(
                    nodes.into_iter().map(|node| node.into()).collect(),
                ))
            }
        }
    }

    async fn get_node_info(self: Arc<Self>) -> Result<proto::dht::NodeInfoResponse> {
        match self.node_info.lock().unwrap().as_ref() {
            Some(info) => Ok(proto::dht::NodeInfoResponse { info: info.clone() }),
            None => Err(anyhow::anyhow!("node info not available")),
        }
    }
}

trait HandlerFuture<T> {
    fn boxed_or_noop(self) -> BoxFutureOrNoop<T>;
}

impl<F, T, E> HandlerFuture<Option<Response<Bytes>>> for F
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    E: std::fmt::Debug,
{
    fn boxed_or_noop(self) -> BoxFutureOrNoop<Option<Response<Bytes>>> {
        BoxFutureOrNoop::Boxed(Box::pin(async move {
            match self.await {
                Ok(res) => Some(Response {
                    version: Default::default(),
                    body: Bytes::from(tl_proto::serialize(&res)),
                }),
                Err(e) => {
                    tracing::debug!("failed to handle request: {e:?}");
                    // TODO: return error response here?
                    None
                }
            }
        }))
    }
}

#[derive(Debug, Clone, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
enum ValueResponseRaw {
    #[tl(id = "dht.valueFound")]
    Found(Bytes),
    #[tl(id = "dht.valueNotFound")]
    NotFound(Vec<Arc<proto::dht::NodeInfo>>),
}

#[derive(Debug, Clone, TlWrite)]
#[tl(boxed, id = "dht.nodesFound", scheme = "proto.tl")]
pub struct NodeResponseRaw {
    /// List of nodes closest to the key.
    pub nodes: Vec<Arc<proto::dht::NodeInfo>>,
}
