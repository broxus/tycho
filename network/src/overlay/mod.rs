use std::sync::Arc;

use crate::proto::overlay::rpc;
use crate::types::{PeerId, Response, Service, ServiceRequest};
use crate::util::Routable;

pub struct OverlayServiceBuilder {
    local_id: PeerId,
}

#[derive(Clone)]
pub struct OverlayService(Arc<OverlayInner>);

impl OverlayService {
    pub fn builder(local_id: PeerId) -> OverlayServiceBuilder {
        OverlayServiceBuilder { local_id }
    }
}

impl Service<ServiceRequest> for OverlayService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        todo!()
    }

    fn on_message(&self, req: ServiceRequest) -> Self::OnMessageFuture {
        todo!()
    }

    fn on_datagram(&self, req: ServiceRequest) -> Self::OnDatagramFuture {
        todo!()
    }
}

impl Routable for OverlayService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::ExchangeRandomPublicEntries::TL_ID, rpc::Prefix::TL_ID]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::Prefix::TL_ID]
    }
}

struct OverlayInner {
    local_id: PeerId,
}

impl OverlayInner {
    fn start_background_tasks(&self) {
        // TODO
    }
}
