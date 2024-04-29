use std::sync::Arc;

use bytes::Buf;
use serde::{Deserialize, Serialize};
use tycho_network::{Response, Service, ServiceRequest};
use tycho_storage::{BlockConnection, KeyBlocksDirection, Storage};
use tycho_util::futures::BoxFutureOrNoop;

use crate::blockchain_rpc::INTERNAL_ERROR_CODE;
use crate::proto::blockchain::*;
use crate::proto::overlay;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct BlockchainRpcServiceConfig {
    /// The maximum number of key blocks in the response.
    ///
    /// Default: 8.
    pub max_key_blocks_list_len: usize,

    /// Whether to serve persistent states.
    ///
    /// Default: yes.
    pub serve_persistent_states: bool,
}

impl Default for BlockchainRpcServiceConfig {
    fn default() -> Self {
        Self {
            max_key_blocks_list_len: 8,
            serve_persistent_states: true,
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockchainRpcService {
    inner: Arc<Inner>,
}

impl BlockchainRpcService {
    pub fn new(storage: Arc<Storage>, config: BlockchainRpcServiceConfig) -> Self {
        Self {
            inner: Arc::new(Inner { storage, config }),
        }
    }
}

impl Service<ServiceRequest> for BlockchainRpcService {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_blockchain_server_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match self.inner.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                return BoxFutureOrNoop::Noop;
            }
        };

        tycho_network::match_tl_request!(body, tag = constructor, {
            rpc::GetNextKeyBlockIds as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    max_size = req.max_size,
                    "getNextKeyBlockIds",
                );

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_next_key_block_ids(&req);
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetBlockFull as req => {
                tracing::debug!(block_id = %req.block_id, "getBlockFull");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_block_full(&req).await;
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetNextBlockFull as req => {
                tracing::debug!(prev_block_id = %req.prev_block_id, "getNextBlockFull");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_next_block_full(&req).await;
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetPersistentStatePart as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    mc_block_id = %req.mc_block_id,
                    offset = %req.offset,
                    max_size = %req.max_size,
                    "getPersistentStatePart"
                );

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_persistent_state_part(&req).await;
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetArchiveInfo as req => {
                tracing::debug!(mc_seqno = %req.mc_seqno, "getArchiveInfo");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_archive_info(&req).await;
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetArchiveSlice as req => {
                tracing::debug!(
                    archive_id = %req.archive_id,
                    offset = %req.offset,
                    max_size = %req.max_size,
                    "getArchiveSlice"
                );

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_archive_slice(&req).await;
                    Some(Response::from_tl(res))
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

struct Inner {
    storage: Arc<Storage>,
    config: BlockchainRpcServiceConfig,
}

impl Inner {
    fn storage(&self) -> &Storage {
        self.storage.as_ref()
    }

    fn try_handle_prefix<'a>(
        &self,
        req: &'a ServiceRequest,
    ) -> Result<(u32, &'a [u8]), tl_proto::TlError> {
        let body = req.as_ref();
        if body.len() < 4 {
            return Err(tl_proto::TlError::UnexpectedEof);
        }

        let constructor = std::convert::identity(body).get_u32_le();
        Ok((constructor, body))
    }

    fn handle_get_next_key_block_ids(
        &self,
        req: &rpc::GetNextKeyBlockIds,
    ) -> overlay::Response<KeyBlockIds> {
        let block_handle_storage = self.storage().block_handle_storage();

        let limit = std::cmp::min(req.max_size as usize, self.config.max_key_blocks_list_len);

        let get_next_key_block_ids = || {
            if !req.block_id.shard.is_masterchain() {
                anyhow::bail!("first block id is not from masterchain");
            }

            let mut iterator = block_handle_storage
                .key_blocks_iterator(KeyBlocksDirection::ForwardFrom(req.block_id.seqno))
                .take(limit + 1);

            if let Some(id) = iterator.next().transpose()? {
                anyhow::ensure!(
                    id.root_hash == req.block_id.root_hash,
                    "first block root hash mismatch"
                );
                anyhow::ensure!(
                    id.file_hash == req.block_id.file_hash,
                    "first block file hash mismatch"
                );
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
                overlay::Response::Ok(KeyBlockIds {
                    block_ids: ids,
                    incomplete,
                })
            }
            Err(e) => {
                tracing::warn!("get_next_key_block_ids failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_block_full(&self, req: &rpc::GetBlockFull) -> overlay::Response<BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let get_block_full = async {
            let mut is_link = false;
            let block = match block_handle_storage.load_handle(&req.block_id)? {
                Some(handle)
                    if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) =>
                {
                    let block = block_storage.load_block_data_raw(&handle).await?;
                    let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                    BlockFull::Found {
                        block_id: req.block_id,
                        proof: proof.into(),
                        block: block.into(),
                        is_link,
                    }
                }
                _ => BlockFull::Empty,
            };

            Ok::<_, anyhow::Error>(block)
        };

        match get_block_full.await {
            Ok(block_full) => overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_block_full failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_next_block_full(
        &self,
        req: &rpc::GetNextBlockFull,
    ) -> overlay::Response<BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_connection_storage = self.storage().block_connection_storage();
        let block_storage = self.storage().block_storage();

        let get_next_block_full = async {
            let next_block_id = match block_handle_storage.load_handle(&req.prev_block_id)? {
                Some(handle) if handle.meta().has_next1() => block_connection_storage
                    .load_connection(&req.prev_block_id, BlockConnection::Next1)?,
                _ => return Ok(BlockFull::Empty),
            };

            let mut is_link = false;
            let block = match block_handle_storage.load_handle(&next_block_id)? {
                Some(handle)
                    if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) =>
                {
                    let block = block_storage.load_block_data_raw(&handle).await?;
                    let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                    BlockFull::Found {
                        block_id: next_block_id,
                        proof: proof.into(),
                        block: block.into(),
                        is_link,
                    }
                }
                _ => BlockFull::Empty,
            };

            Ok::<_, anyhow::Error>(block)
        };

        match get_next_block_full.await {
            Ok(block_full) => overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_next_block_full failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_persistent_state_part(
        &self,
        req: &rpc::GetPersistentStatePart,
    ) -> overlay::Response<PersistentStatePart> {
        const PART_MAX_SIZE: u64 = 1 << 21;

        let persistent_state_storage = self.storage().persistent_state_storage();

        let persistent_state_request_validation = || {
            anyhow::ensure!(
                self.config.serve_persistent_states,
                "persistent states are disabled"
            );
            anyhow::ensure!(req.max_size <= PART_MAX_SIZE, "too large max_size");
            Ok::<_, anyhow::Error>(())
        };

        if let Err(e) = persistent_state_request_validation() {
            tracing::warn!("persistent_state_request_validation failed: {e:?}");
            return overlay::Response::Err(INTERNAL_ERROR_CODE);
        }

        if !persistent_state_storage.state_exists(&req.mc_block_id, &req.block_id) {
            return overlay::Response::Ok(PersistentStatePart::NotFound);
        }

        match persistent_state_storage
            .read_state_part(&req.mc_block_id, &req.block_id, req.offset, req.max_size)
            .await
        {
            Some(data) => overlay::Response::Ok(PersistentStatePart::Found { data }),
            None => overlay::Response::Ok(PersistentStatePart::NotFound),
        }
    }

    async fn handle_get_archive_info(
        &self,
        req: &rpc::GetArchiveInfo,
    ) -> overlay::Response<ArchiveInfo> {
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
                    return overlay::Response::Ok(ArchiveInfo::NotFound);
                }

                if mc_seqno > shards_client_mc_block_id.seqno {
                    return overlay::Response::Ok(ArchiveInfo::NotFound);
                }

                let block_storage = self.storage().block_storage();

                overlay::Response::Ok(match block_storage.get_archive_id(mc_seqno) {
                    Some(id) => ArchiveInfo::Found { id: id as u64 },
                    None => ArchiveInfo::NotFound,
                })
            }
            Err(e) => {
                tracing::warn!("get_archive_id failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_archive_slice(
        &self,
        req: &rpc::GetArchiveSlice,
    ) -> overlay::Response<Data> {
        let block_storage = self.storage.block_storage();

        let get_archive_slice = || {
            let Some(archive_slice) = block_storage.get_archive_slice(
                req.archive_id as u32,
                req.offset as usize,
                req.max_size as usize,
            )?
            else {
                anyhow::bail!("archive not found");
            };

            Ok::<_, anyhow::Error>(archive_slice)
        };

        match get_archive_slice() {
            Ok(data) => overlay::Response::Ok(Data { data: data.into() }),
            Err(e) => {
                tracing::warn!("get_archive_slice failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }
}
