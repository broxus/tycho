use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use anyhow::Context;
use bytes::{Buf, Bytes};
use everscale_types::models::BlockId;
use futures_util::Future;
use serde::{Deserialize, Serialize};
use tycho_network::{try_handle_prefix, InboundRequestMeta, Response, Service, ServiceRequest};
use tycho_storage::{ArchiveId, BlockConnection, KeyBlocksDirection, PersistentStateKind, Storage};
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::metrics::HistogramGuard;

use crate::blockchain_rpc::{BAD_REQUEST_ERROR_CODE, INTERNAL_ERROR_CODE, NOT_FOUND_ERROR_CODE};
use crate::proto::blockchain::*;
use crate::proto::overlay;

const RPC_METHOD_TIMINGS_METRIC: &str = "tycho_blockchain_rpc_method_time";

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
            rpc::GetBlockDataChunk as req => {
                tracing::debug!(block_id = %req.block_id, offset = %req.offset, "getBlockDataChunk");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_block_data_chunk(&req);
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
            rpc::GetPersistentShardStateInfo as req => {
                tracing::debug!(block_id = %req.block_id, "getPersistentShardStateInfo");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_persistent_state_info(&req);
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetPersistentQueueStateInfo as req => {
                tracing::debug!(block_id = %req.block_id, "getPersistentQueueStateInfo");

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_queue_persistent_state_info(&req);
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetPersistentShardStateChunk as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    offset = %req.offset,
                    "getPersistentShardStateChunk"
                );

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_persistent_shard_state_chunk(&req).await;
                    Some(Response::from_tl(res))
                })
            },
            rpc::GetPersistentQueueStateChunk as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    offset = %req.offset,
                    "getPersistentQueueStateChunk"
                );

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_persistent_queue_state_chunk(&req).await;
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
            rpc::GetArchiveChunk as req => {
                tracing::debug!(
                    archive_id = %req.archive_id,
                    offset = %req.offset,
                    "getArchiveChunk"
                );

                let inner = self.inner.clone();
                BoxFutureOrNoop::future(async move {
                    let res = inner.handle_get_archive_chunk(&req).await;
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
                match BytesMeta::read_from(&mut req.body.as_ref()) {
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
                metrics::counter!("tycho_rpc_broadcast_external_message_rx_bytes_total")
                    .increment(req.body.len() as u64);
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
        let label = [("method", "getNextKeyBlockIds")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

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
        let label = [("method", "getBlockFull")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

        match self.get_block_full(&req.block_id).await {
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
        let label = [("method", "getNextBlockFull")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

        let block_handle_storage = self.storage().block_handle_storage();
        let block_connection_storage = self.storage().block_connection_storage();

        let get_next_block_full = async {
            let next_block_id = match block_handle_storage.load_handle(&req.prev_block_id) {
                Some(handle) if handle.has_next1() => block_connection_storage
                    .load_connection(&req.prev_block_id, BlockConnection::Next1)
                    .context("connection not found")?,
                _ => return Ok(BlockFull::NotFound),
            };

            self.get_block_full(&next_block_id).await
        };

        match get_next_block_full.await {
            Ok(block_full) => overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_next_block_full failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    fn handle_get_block_data_chunk(&self, req: &rpc::GetBlockDataChunk) -> overlay::Response<Data> {
        let label = [("method", "getBlockDataChunk")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

        let block_storage = self.storage.block_storage();
        match block_storage.get_block_data_chunk(&req.block_id, req.offset) {
            Ok(Some(data)) => overlay::Response::Ok(Data {
                data: Bytes::from_owner(data),
            }),
            Ok(None) => overlay::Response::Err(NOT_FOUND_ERROR_CODE),
            Err(e) => {
                tracing::warn!("get_block_data_chunk failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_key_block_proof(
        &self,
        req: &rpc::GetKeyBlockProof,
    ) -> overlay::Response<KeyBlockProof> {
        let label = [("method", "getKeyBlockProof")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let get_key_block_proof = async {
            match block_handle_storage.load_handle(&req.block_id) {
                Some(handle) if handle.has_proof() => {
                    let data = block_storage.load_block_proof_raw(&handle).await?;
                    Ok::<_, anyhow::Error>(KeyBlockProof::Found {
                        proof: Bytes::from_owner(data),
                    })
                }
                _ => Ok(KeyBlockProof::NotFound),
            }
        };

        match get_key_block_proof.await {
            Ok(key_block_proof) => overlay::Response::Ok(key_block_proof),
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

        let label = [("method", "getArchiveInfo")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

        match node_state.load_last_mc_block_id() {
            Some(last_applied_mc_block) => {
                if mc_seqno > last_applied_mc_block.seqno {
                    return overlay::Response::Ok(ArchiveInfo::TooNew);
                }

                let block_storage = self.storage().block_storage();

                let id = block_storage.get_archive_id(mc_seqno);
                let size_res = match id {
                    ArchiveId::Found(id) => block_storage.get_archive_size(id),
                    ArchiveId::TooNew | ArchiveId::NotFound => Ok(None),
                };

                overlay::Response::Ok(match (id, size_res) {
                    (ArchiveId::Found(id), Ok(Some(size))) if size > 0 => ArchiveInfo::Found {
                        id: id as u64,
                        size: NonZeroU64::new(size as _).unwrap(),
                        chunk_size: block_storage.archive_chunk_size(),
                    },
                    (ArchiveId::TooNew, Ok(None)) => ArchiveInfo::TooNew,
                    _ => ArchiveInfo::NotFound,
                })
            }
            None => {
                tracing::warn!("get_archive_id failed: no blocks applied");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_archive_chunk(
        &self,
        req: &rpc::GetArchiveChunk,
    ) -> overlay::Response<Data> {
        let label = [("method", "getArchiveChunk")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);

        let block_storage = self.storage.block_storage();

        let get_archive_chunk = || async {
            let archive_slice = block_storage
                .get_archive_chunk(req.archive_id as u32, req.offset)
                .await?;

            Ok::<_, anyhow::Error>(archive_slice)
        };

        match get_archive_chunk().await {
            Ok(data) => overlay::Response::Ok(Data {
                data: Bytes::from_owner(data),
            }),
            Err(e) => {
                tracing::warn!("get_archive_chunk failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    fn handle_get_persistent_state_info(
        &self,
        req: &rpc::GetPersistentShardStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        let label = [("method", "getPersistentStateInfo")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);
        let res = self.read_persistent_state_info(&req.block_id, PersistentStateKind::Shard);
        overlay::Response::Ok(res)
    }

    fn handle_get_queue_persistent_state_info(
        &self,
        req: &rpc::GetPersistentQueueStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        let label = [("method", "getQueuePersistentStateInfo")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);
        let res = self.read_persistent_state_info(&req.block_id, PersistentStateKind::Queue);
        overlay::Response::Ok(res)
    }

    async fn handle_get_persistent_shard_state_chunk(
        &self,
        req: &rpc::GetPersistentShardStateChunk,
    ) -> overlay::Response<Data> {
        let label = [("method", "getPersistentShardStateChunk")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);
        self.read_persistent_state_chunk(&req.block_id, req.offset, PersistentStateKind::Shard)
            .await
    }

    async fn handle_get_persistent_queue_state_chunk(
        &self,
        req: &rpc::GetPersistentQueueStateChunk,
    ) -> overlay::Response<Data> {
        let label = [("method", "getPersistentQueueStateChunk")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);
        self.read_persistent_state_chunk(&req.block_id, req.offset, PersistentStateKind::Queue)
            .await
    }
}

impl<B> Inner<B> {
    async fn get_block_full(&self, block_id: &BlockId) -> anyhow::Result<BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let handle = match block_handle_storage.load_handle(block_id) {
            Some(handle) if handle.has_all_block_parts() => handle,
            _ => return Ok(BlockFull::NotFound),
        };

        let Some(data) = block_storage.get_block_data_chunk(block_id, 0)? else {
            return Ok(BlockFull::NotFound);
        };

        let data_chunk_size = block_storage.block_data_chunk_size();
        let data_size = if data.len() < data_chunk_size.get() as usize {
            // NOTE: Skip one RocksDB read for relatively small blocks
            //       Average block size is 4KB, while the chunk size is 1MB.
            data.len() as u32
        } else {
            match block_storage.get_block_data_size(block_id)? {
                Some(size) => size,
                None => return Ok(BlockFull::NotFound),
            }
        };

        let block = BlockData {
            data: Bytes::from_owner(data),
            size: NonZeroU32::new(data_size).expect("shouldn't happen"),
            chunk_size: data_chunk_size,
        };

        let (proof, queue_diff) = tokio::join!(
            block_storage.load_block_proof_raw(&handle),
            block_storage.load_queue_diff_raw(&handle)
        );

        Ok(BlockFull::Found {
            block_id: *block_id,
            block,
            proof: Bytes::from_owner(proof?),
            queue_diff: Bytes::from_owner(queue_diff?),
        })
    }

    fn read_persistent_state_info(
        &self,
        block_id: &BlockId,
        state_kind: PersistentStateKind,
    ) -> PersistentStateInfo {
        let persistent_state_storage = self.storage().persistent_state_storage();
        if self.config.serve_persistent_states {
            if let Some(info) = persistent_state_storage.get_state_info(block_id, state_kind) {
                return PersistentStateInfo::Found {
                    size: info.size,
                    chunk_size: info.chunk_size,
                };
            }
        }
        PersistentStateInfo::NotFound
    }

    async fn read_persistent_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        state_kind: PersistentStateKind,
    ) -> overlay::Response<Data> {
        let persistent_state_storage = self.storage().persistent_state_storage();

        let persistent_state_request_validation = || {
            anyhow::ensure!(
                self.config.serve_persistent_states,
                "persistent states are disabled"
            );
            Ok::<_, anyhow::Error>(())
        };

        if let Err(e) = persistent_state_request_validation() {
            tracing::debug!("persistent state request validation failed: {e:?}");
            return overlay::Response::Err(BAD_REQUEST_ERROR_CODE);
        }

        match persistent_state_storage
            .read_state_part(block_id, offset, state_kind)
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
