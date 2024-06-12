use std::sync::Arc;

use anyhow::Context;
use bytes::{Buf, Bytes};
use bytesize::ByteSize;
use futures_util::Future;
use serde::{Deserialize, Serialize};
use tycho_network::{InboundRequestMeta, Response, Service, ServiceRequest};
use tycho_storage::{BlockConnection, KeyBlocksDirection, Storage};
use tycho_util::futures::BoxFutureOrNoop;

use crate::blockchain_rpc::{BAD_REQUEST_ERROR_CODE, INTERNAL_ERROR_CODE, NOT_FOUND_ERROR_CODE};
use crate::proto::blockchain::*;
use crate::proto::overlay;

pub trait BroadcastListener: Send + Sync + 'static {
    type HandleMessageFut<'a>: Future<Output = ()> + Send + 'a;

    fn handle_message(
        &self,
        meta: Arc<InboundRequestMeta>,
        message: Bytes,
    ) -> Self::HandleMessageFut<'_>;
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct NoopBroadcastListener;

impl BroadcastListener for NoopBroadcastListener {
    type HandleMessageFut<'a> = futures_util::future::Ready<()>;

    #[inline]
    fn handle_message(&self, _: Arc<InboundRequestMeta>, _: Bytes) -> Self::HandleMessageFut<'_> {
        futures_util::future::ready(())
    }
}

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

pub struct BlockchainRpcServiceBuilder<MandatoryFields> {
    config: BlockchainRpcServiceConfig,
    mandatory_fields: MandatoryFields,
}

impl<B> BlockchainRpcServiceBuilder<(B, Storage)>
where
    B: BroadcastListener,
{
    pub fn build(self) -> BlockchainRpcService<B> {
        let (broadcast_listener, storage) = self.mandatory_fields;

        BlockchainRpcService {
            inner: Arc::new(Inner {
                storage,
                config: self.config,
                broadcast_listener,
            }),
        }
    }
}

impl<T1> BlockchainRpcServiceBuilder<(T1, ())> {
    pub fn with_storage(self, storage: Storage) -> BlockchainRpcServiceBuilder<(T1, Storage)> {
        let (broadcast_listener, _) = self.mandatory_fields;

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (broadcast_listener, storage),
        }
    }
}

impl<T2> BlockchainRpcServiceBuilder<((), T2)> {
    pub fn with_broadcast_listener<T1>(
        self,
        broadcast_listener: T1,
    ) -> BlockchainRpcServiceBuilder<(T1, T2)>
    where
        T1: BroadcastListener,
    {
        let (_, storage) = self.mandatory_fields;

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (broadcast_listener, storage),
        }
    }

    pub fn without_broadcast_listener(
        self,
    ) -> BlockchainRpcServiceBuilder<(NoopBroadcastListener, T2)> {
        let (_, storage) = self.mandatory_fields;

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (NoopBroadcastListener, storage),
        }
    }
}

impl<T1, T2> BlockchainRpcServiceBuilder<(T1, T2)> {
    pub fn with_config(self, config: BlockchainRpcServiceConfig) -> Self {
        Self { config, ..self }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockchainRpcService<B = NoopBroadcastListener> {
    inner: Arc<Inner<B>>,
}

impl BlockchainRpcService<()> {
    pub fn builder() -> BlockchainRpcServiceBuilder<((), ())> {
        BlockchainRpcServiceBuilder {
            config: Default::default(),
            mandatory_fields: ((), ()),
        }
    }
}

impl<B: BroadcastListener> Service<ServiceRequest> for BlockchainRpcService<B> {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(level = "debug", name = "on_blockchain_service_query", skip_all)]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                return BoxFutureOrNoop::Noop;
            }
        };

        tycho_network::match_tl_request!(body, tag = constructor, {
            overlay::Ping as _ => BoxFutureOrNoop::future(async {
                Some(Response::from_tl(overlay::Pong))
            }),
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
            rpc::GetKeyBlockProof as req => {
                tracing::debug!(block_id = %req.block_id, "getKeyBlockProof");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_key_block_proof(&req).await;
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetPersistentStateInfo as req => {
                tracing::debug!(block_id = %req.block_id, "getPersistentStateInfo");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_persistent_state_info(&req);
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetPersistentStatePart as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    limit = %req.limit,
                    offset = %req.offset,
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
                    limit = %req.limit,
                    offset = %req.offset,
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

    #[tracing::instrument(level = "debug", name = "on_blockchain_service_message", skip_all)]
    fn on_message(&self, mut req: ServiceRequest) -> Self::OnMessageFuture {
        use tl_proto::{BytesMeta, TlRead};

        // TODO: Do nothing if `B` is `NoopBroadcastListener` via `castaway` ?

        // Require message body to contain at least two constructors.
        if req.body.len() < 8 {
            return BoxFutureOrNoop::Noop;
        }

        // Skip broadcast prefix
        if req.body.get_u32_le() != overlay::BroadcastPrefix::TL_ID {
            return BoxFutureOrNoop::Noop;
        }

        // Read (CONSUME) the next constructor.
        match req.body.get_u32_le() {
            MessageBroadcastRef::TL_ID => {
                match BytesMeta::read_from(&req.body, &mut 0) {
                    // NOTE: `len` is 24bit integer
                    Ok(meta) if req.body.len() == meta.prefix_len + meta.len + meta.padding => {
                        req.body.advance(meta.prefix_len);
                        req.body.truncate(meta.len);
                    }
                    Ok(_) => {
                        tracing::debug!("malformed external message broadcast");
                        return BoxFutureOrNoop::Noop;
                    }
                    Err(e) => {
                        tracing::debug!("failed to deserialize external message broadcast: {e:?}");
                        return BoxFutureOrNoop::Noop;
                    }
                }

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    inner
                        .broadcast_listener
                        .handle_message(req.metadata, req.body)
                        .await;
                })
            }
            constructor => {
                tracing::debug!("unknown broadcast constructor: {constructor:08x}");
                BoxFutureOrNoop::Noop
            }
        }
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

struct Inner<B> {
    storage: Storage,
    config: BlockchainRpcServiceConfig,
    broadcast_listener: B,
}

impl<B> Inner<B> {
    fn storage(&self) -> &Storage {
        &self.storage
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

            if let Some(id) = iterator.next() {
                anyhow::ensure!(
                    id.root_hash == req.block_id.root_hash,
                    "first block root hash mismatch"
                );
                anyhow::ensure!(
                    id.file_hash == req.block_id.file_hash,
                    "first block file hash mismatch"
                );
            }

            Ok::<_, anyhow::Error>(iterator.take(limit).collect::<Vec<_>>())
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
            let block = match block_handle_storage.load_handle(&req.block_id) {
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
            let next_block_id = match block_handle_storage.load_handle(&req.prev_block_id) {
                Some(handle) if handle.meta().has_next1() => block_connection_storage
                    .load_connection(&req.prev_block_id, BlockConnection::Next1)
                    .context("connection not found")?,
                _ => return Ok(BlockFull::Empty),
            };

            let mut is_link = false;
            let block = match block_handle_storage.load_handle(&next_block_id) {
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

    async fn handle_get_key_block_proof(
        &self,
        req: &rpc::GetKeyBlockProof,
    ) -> overlay::Response<Data> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let get_key_block_proof = async {
            match block_handle_storage.load_handle(&req.block_id) {
                Some(handle) if handle.meta().has_proof() => {
                    block_storage.load_block_proof_raw(&handle, false).await
                }
                _ => anyhow::bail!("proof not found"),
            }
        };

        match get_key_block_proof.await {
            Ok(key_block_proof) => overlay::Response::Ok(Data {
                data: key_block_proof.into(),
            }),
            Err(e) => {
                tracing::warn!("get_key_block_proof failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_archive_info(
        &self,
        req: &rpc::GetArchiveInfo,
    ) -> overlay::Response<ArchiveInfo> {
        let mc_seqno = req.mc_seqno;
        let node_state = self.storage.node_state();

        match node_state.load_last_mc_block_id() {
            Some(last_applied_mc_block) => {
                if mc_seqno > last_applied_mc_block.seqno {
                    return overlay::Response::Ok(ArchiveInfo::NotFound);
                }

                let block_storage = self.storage().block_storage();

                overlay::Response::Ok(match block_storage.get_archive_id(mc_seqno) {
                    Some(id) => ArchiveInfo::Found { id: id as u64 },
                    None => ArchiveInfo::NotFound,
                })
            }
            None => {
                tracing::warn!("get_archive_id failed: no blocks applied");
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
                req.limit as usize,
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

    fn handle_get_persistent_state_info(
        &self,
        req: &rpc::GetPersistentStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        let persistent_state_storage = self.storage().persistent_state_storage();

        let res = 'res: {
            if self.config.serve_persistent_states {
                if let Some(info) = persistent_state_storage.get_state_info(&req.block_id) {
                    break 'res PersistentStateInfo::Found {
                        size: info.size as u64,
                    };
                }
            }
            PersistentStateInfo::NotFound
        };

        overlay::Response::Ok(res)
    }

    async fn handle_get_persistent_state_part(
        &self,
        req: &rpc::GetPersistentStatePart,
    ) -> overlay::Response<Data> {
        const PART_MAX_SIZE: u64 = ByteSize::mib(2).as_u64();

        let persistent_state_storage = self.storage().persistent_state_storage();

        let persistent_state_request_validation = || {
            anyhow::ensure!(
                self.config.serve_persistent_states,
                "persistent states are disabled"
            );
            anyhow::ensure!(req.limit as u64 <= PART_MAX_SIZE, "too large max_size");
            Ok::<_, anyhow::Error>(())
        };

        if let Err(e) = persistent_state_request_validation() {
            tracing::debug!("persistent state request validation failed: {e:?}");
            return overlay::Response::Err(BAD_REQUEST_ERROR_CODE);
        }

        match persistent_state_storage
            .read_state_part(&req.block_id, req.limit, req.offset)
            .await
        {
            Some(data) => overlay::Response::Ok(Data { data: data.into() }),
            None => {
                tracing::debug!("failed to read persistent state part");
                overlay::Response::Err(NOT_FOUND_ERROR_CODE)
            }
        }
    }
}

fn try_handle_prefix(req: &ServiceRequest) -> Result<(u32, &[u8]), tl_proto::TlError> {
    let body = req.as_ref();
    if body.len() < 4 {
        return Err(tl_proto::TlError::UnexpectedEof);
    }

    let constructor = std::convert::identity(body).get_u32_le();
    Ok((constructor, body))
}
