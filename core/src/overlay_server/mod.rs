use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use tokio::sync::broadcast;
use tycho_network::proto::dht::{rpc, NodeResponse, Value, ValueResponseRaw};
use tycho_network::{Response, Service, ServiceRequest};
use tycho_storage::{KeyBlocksDirection, Storage};
use tycho_util::futures::BoxFutureOrNoop;

use crate::proto;

pub struct OverlayServer(Arc<OverlayServerInner>);

impl OverlayServer {
    pub fn new(storage: Arc<Storage>) -> Arc<Self> {
        Arc::new(Self(Arc::new(OverlayServerInner { storage })))
    }
}

impl Service<ServiceRequest> for OverlayServer {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
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
                return BoxFutureOrNoop::Noop;
            }
        };

        tycho_network::match_tl_request!(body, tag = constructor, {
            proto::overlay::GetNextKeyBlockIds as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(blockId = %req.block, max_size = req.max_size, "keyBlocksRequest");

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_next_key_block_ids(&req);
                        Some(Response::from_tl(res))

                        /*tokio::task::spawn_blocking(move || {
                            let res = inner.handle_get_next_key_block_ids(&req);
                            Some(Response::from_tl(res))
                        }).await.unwrap_or_else(|_| None)*/
                    }
                })
            },
        }, e => {
            tracing::debug!("failed to deserialize query: {e}");
            BoxFutureOrNoop::Noop
        })
    }

    #[inline]
    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

struct OverlayServerInner {
    storage: Arc<Storage>,
}

impl OverlayServerInner {
    fn storage(&self) -> &Storage {
        self.storage.as_ref()
    }

    fn try_handle_prefix<'a>(&self, req: &'a ServiceRequest) -> anyhow::Result<(u32, &'a [u8])> {
        let mut body = req.as_ref();
        anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

        let mut constructor = std::convert::identity(body).get_u32_le();

        Ok((constructor, body))
    }

    fn handle_get_next_key_block_ids(
        &self,
        req: &proto::overlay::GetNextKeyBlockIds,
    ) -> proto::overlay::Response<proto::overlay::KeyBlockIdsResponse> {
        const NEXT_KEY_BLOCKS_LIMIT: usize = 8;

        let block_handle_storage = self.storage().block_handle_storage();

        let limit = std::cmp::min(req.max_size as usize, NEXT_KEY_BLOCKS_LIMIT);

        let get_next_key_block_ids = || {
            let start_block_id = &req.block;
            if !start_block_id.shard.is_masterchain() {
                return Err(OverlayServerError::BlockNotFromMasterChain.into());
            }

            let mut iterator = block_handle_storage
                .key_blocks_iterator(KeyBlocksDirection::ForwardFrom(start_block_id.seqno))
                .take(limit)
                .peekable();

            if let Some(Ok(id)) = iterator.peek() {
                if id.root_hash != start_block_id.root_hash {
                    return Err(OverlayServerError::InvalidRootHash.into());
                }
                if id.file_hash != start_block_id.file_hash {
                    return Err(OverlayServerError::InvalidFileHash.into());
                }
            }

            let mut ids = Vec::with_capacity(limit);
            while let Some(id) = iterator.next().transpose()? {
                ids.push(id);
                if ids.len() >= limit {
                    break;
                }
            }

            Ok::<_, anyhow::Error>(ids)
        };

        match get_next_key_block_ids() {
            Ok(ids) => {
                let incomplete = ids.len() < limit;
                proto::overlay::Response::Ok(proto::overlay::KeyBlockIdsResponse {
                    blocks: ids,
                    incomplete,
                })
            }
            Err(e) => {
                tracing::warn!("get_next_key_block_ids failed: {e:?}");
                proto::overlay::Response::Err
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum OverlayServerError {
    #[error("Block is not from masterchain")]
    BlockNotFromMasterChain,
    #[error("Invalid root hash")]
    InvalidRootHash,
    #[error("Invalid file hash")]
    InvalidFileHash,
}
