use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use tokio::sync::broadcast;
use tycho_network::proto::dht::{rpc, NodeResponse, Value, ValueResponseRaw};
use tycho_network::{Response, Service, ServiceRequest};
use tycho_storage::{BlockConnection, KeyBlocksDirection, Storage};
use tycho_util::futures::BoxFutureOrNoop;

use crate::proto;

pub struct OverlayServer(Arc<OverlayServerInner>);

impl OverlayServer {
    pub fn new(storage: Arc<Storage>, support_persistent_states: bool) -> Self {
        Self(Arc::new(OverlayServerInner {
            storage,
            support_persistent_states,
        }))
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
            proto::overlay::rpc::GetNextKeyBlockIds as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(blockId = %req.block, max_size = req.max_size, "getNextKeyBlockIds");

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_next_key_block_ids(req);
                        Some(Response::from_tl(res))
                    }
                })
            },
            proto::overlay::rpc::GetBlockFull as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(blockId = %req.block, "getBlockFull");

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_block_full(req).await;
                        Some(Response::from_tl(res))
                    }
                })
            },
            proto::overlay::rpc::GetNextBlockFull as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(prevBlockId = %req.prev_block, "getNextBlockFull");

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_next_block_full(req).await;
                        Some(Response::from_tl(res))
                    }
                })
            },
            proto::overlay::rpc::GetPersistentStatePart as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(
                        block = %req.block,
                        mc_block = %req.mc_block,
                        offset = %req.offset,
                        max_size = %req.max_size,
                        "пetPersistentStatePart"
                    );

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_persistent_state_part(req).await;
                        Some(Response::from_tl(res))
                    }
                })
            },
            proto::overlay::rpc::GetArchiveInfo as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(mc_seqno = %req.mc_seqno, "getArchiveInfo");

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_archive_info(req).await;
                        Some(Response::from_tl(res))
                    }
                })
            },
            proto::overlay::rpc::GetArchiveSlice as req => {
                BoxFutureOrNoop::future({
                    tracing::debug!(
                        archive_id = %req.archive_id,
                        offset = %req.offset,
                        max_size = %req.max_size,
                        "getArchiveSlice"
                    );

                    let inner = self.0.clone();

                    async move {
                        let res = inner.handle_get_archive_slice(req).await;
                        Some(Response::from_tl(res))
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
    support_persistent_states: bool,
}

impl OverlayServerInner {
    fn storage(&self) -> &Storage {
        self.storage.as_ref()
    }

    fn supports_persistent_state_handling(&self) -> bool {
        self.support_persistent_states
    }

    fn try_handle_prefix<'a>(&self, req: &'a ServiceRequest) -> anyhow::Result<(u32, &'a [u8])> {
        let mut body = req.as_ref();
        anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

        let mut constructor = std::convert::identity(body).get_u32_le();

        Ok((constructor, body))
    }

    fn handle_get_next_key_block_ids(
        &self,
        req: proto::overlay::rpc::GetNextKeyBlockIds,
    ) -> proto::overlay::Response<proto::overlay::KeyBlockIds> {
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
                proto::overlay::Response::Ok(proto::overlay::KeyBlockIds {
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

    async fn handle_get_block_full(
        &self,
        req: proto::overlay::rpc::GetBlockFull,
    ) -> proto::overlay::Response<proto::overlay::BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let get_block_full = || async {
            let mut is_link = false;
            let block = match block_handle_storage.load_handle(&req.block)? {
                Some(handle)
                    if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) =>
                {
                    let block = block_storage.load_block_data_raw(&handle).await?;
                    let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                    proto::overlay::BlockFull::Found {
                        block_id: req.block,
                        proof: proof.into(),
                        block: block.into(),
                        is_link,
                    }
                }
                _ => proto::overlay::BlockFull::Empty,
            };

            Ok::<_, anyhow::Error>(block)
        };

        match get_block_full().await {
            Ok(block_full) => proto::overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_block_full failed: {e:?}");
                proto::overlay::Response::Err
            }
        }
    }

    async fn handle_get_next_block_full(
        &self,
        req: proto::overlay::rpc::GetNextBlockFull,
    ) -> proto::overlay::Response<proto::overlay::BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_connection_storage = self.storage().block_connection_storage();
        let block_storage = self.storage().block_storage();

        let get_next_block_full = || async {
            let next_block_id = match block_handle_storage.load_handle(&req.prev_block)? {
                Some(handle) if handle.meta().has_next1() => block_connection_storage
                    .load_connection(&req.prev_block, BlockConnection::Next1)?,
                _ => return Ok(proto::overlay::BlockFull::Empty),
            };

            let mut is_link = false;
            let block = match block_handle_storage.load_handle(&next_block_id)? {
                Some(handle)
                    if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) =>
                {
                    let block = block_storage.load_block_data_raw(&handle).await?;
                    let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                    proto::overlay::BlockFull::Found {
                        block_id: next_block_id,
                        proof: proof.into(),
                        block: block.into(),
                        is_link,
                    }
                }
                _ => proto::overlay::BlockFull::Empty,
            };

            Ok::<_, anyhow::Error>(block)
        };

        match get_next_block_full().await {
            Ok(block_full) => proto::overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_next_block_full failed: {e:?}");
                proto::overlay::Response::Err
            }
        }
    }

    async fn handle_get_persistent_state_part(
        &self,
        req: proto::overlay::rpc::GetPersistentStatePart,
    ) -> proto::overlay::Response<proto::overlay::PersistentStatePart> {
        const PART_MAX_SIZE: u64 = 1 << 21;

        let persistent_state_request_validation = || {
            anyhow::ensure!(
                self.supports_persistent_state_handling(),
                "Get persistent state not supported"
            );

            anyhow::ensure!(req.max_size <= PART_MAX_SIZE, "Unsupported max size");

            Ok::<_, anyhow::Error>(())
        };

        if let Err(e) = persistent_state_request_validation() {
            tracing::warn!("persistent_state_request_validation failed: {e:?}");
            return proto::overlay::Response::Err;
        }

        let persistent_state_storage = self.storage().persistent_state_storage();
        if !persistent_state_storage.state_exists(&req.mc_block, &req.block) {
            return proto::overlay::Response::Ok(proto::overlay::PersistentStatePart::NotFound);
        }

        let persistent_state_storage = self.storage.persistent_state_storage();
        match persistent_state_storage
            .read_state_part(&req.mc_block, &req.block, req.offset, req.max_size)
            .await
        {
            Some(data) => {
                proto::overlay::Response::Ok(proto::overlay::PersistentStatePart::Found { data })
            }
            None => proto::overlay::Response::Ok(proto::overlay::PersistentStatePart::NotFound),
        }
    }

    async fn handle_get_archive_info(
        &self,
        req: proto::overlay::rpc::GetArchiveInfo,
    ) -> proto::overlay::Response<proto::overlay::ArchiveInfo> {
        let mc_seqno = req.mc_seqno;
        let node_state = self.storage.node_state();

        let get_archive_id = || {
            let last_applied_mc_block = node_state.load_last_mc_block_id()?;
            let shards_client_mc_block_id = node_state.load_shards_client_mc_block_id()?;

            Ok::<_, anyhow::Error>((last_applied_mc_block, shards_client_mc_block_id))
        };

        match get_archive_id() {
            Ok((last_applied_mc_block, shards_client_mc_block_id)) => {
                if mc_seqno > last_applied_mc_block.seqno {
                    return proto::overlay::Response::Ok(proto::overlay::ArchiveInfo::NotFound);
                }

                if mc_seqno > shards_client_mc_block_id.seqno {
                    return proto::overlay::Response::Ok(proto::overlay::ArchiveInfo::NotFound);
                }

                let block_storage = self.storage().block_storage();

                let res = match block_storage.get_archive_id(mc_seqno) {
                    Some(id) => proto::overlay::ArchiveInfo::Found { id: id as u64 },
                    None => proto::overlay::ArchiveInfo::NotFound,
                };

                proto::overlay::Response::Ok(res)
            }
            Err(e) => {
                tracing::warn!("get_archive_id failed: {e:?}");
                proto::overlay::Response::Err
            }
        }
    }

    async fn handle_get_archive_slice(
        &self,
        req: proto::overlay::rpc::GetArchiveSlice,
    ) -> proto::overlay::Response<proto::overlay::Data> {
        let block_storage = self.storage.block_storage();

        let get_archive_slice = || {
            let archive_slice = block_storage
                .get_archive_slice(
                    req.archive_id as u32,
                    req.offset as usize,
                    req.max_size as usize,
                )?
                .ok_or(OverlayServerError::ArchiveNotFound)?;

            Ok::<_, anyhow::Error>(archive_slice)
        };

        match get_archive_slice() {
            Ok(data) => proto::overlay::Response::Ok(proto::overlay::Data { data: data.into() }),
            Err(e) => {
                tracing::warn!("get_archive_slice failed: {e:?}");
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
    #[error("Archive not found")]
    ArchiveNotFound,
}
