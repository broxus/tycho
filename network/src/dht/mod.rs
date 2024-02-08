use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use futures_util::future::BoxFuture;

use self::routing::RoutingTable;
use self::storage::Storage;
use crate::network::WeakNetwork;
use crate::types::{PeerId, Response, Service};
use crate::{proto, InboundServiceRequest};

mod routing;
mod storage;

pub struct Dht(Arc<DhtInner>);

impl Dht {
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
    network: WeakNetwork,
}

impl Service<InboundServiceRequest<Bytes>> for Dht {
    type QueryResponse = Response<Bytes>;
    type OnQueryFuture = BoxFuture<'static, Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&mut self, req: InboundServiceRequest<Bytes>) -> Self::OnQueryFuture {
        // TODO: parse query and dispatch to appropriate method

        todo!()
    }

    #[inline]
    fn on_message(&mut self, req: InboundServiceRequest<Bytes>) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&mut self, req: InboundServiceRequest<Bytes>) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}
