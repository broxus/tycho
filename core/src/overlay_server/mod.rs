use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use tokio::sync::broadcast;
use tycho_network::proto::dht::{rpc, Value};
use tycho_network::{Response, Service, ServiceRequest};
use tycho_storage::Storage;

pub struct OverlayServer(OverlayServerInner);

impl OverlayServer {
    pub fn new(storage: Arc<Storage>) -> Arc<Self> {
        Arc::new(Self(OverlayServerInner { storage }))
    }
}

struct OverlayServerInner {
    storage: Arc<Storage>,
}

impl OverlayServerInner {
    fn try_handle_prefix<'a>(&self, req: &'a ServiceRequest) -> anyhow::Result<(u32, &'a [u8])> {
        let mut body = req.as_ref();
        anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

        let mut constructor = std::convert::identity(body).get_u32_le();

        Ok((constructor, body))
    }
}

impl Service<ServiceRequest> for OverlayServer {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
    level = "debug",
    name = "on_overlay_server_query",
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

        let response: Option<Vec<u8>> = tycho_network::match_tl_request!(body, tag = constructor, {
            // TODO
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
    name = "on_overlay_server_message",
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

        tycho_network::match_tl_request!(body, tag = constructor, {
            // TODO
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
