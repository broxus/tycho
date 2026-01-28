use std::num::NonZeroU32;
use std::sync::Arc;

use anyhow::Context;
use bytes::{Buf, Bytes};
#[cfg(feature = "s3")]
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use tycho_block_util::message::validate_external_message;
use tycho_network::{Response, Service, ServiceRequest, try_handle_prefix};
use tycho_types::models::BlockId;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::metrics::HistogramGuard;

use crate::blockchain_rpc::broadcast_listener::{BroadcastListener, NoopBroadcastListener};
use crate::blockchain_rpc::providers::{IntoRpcDataProvider, RpcDataProvider};
use crate::blockchain_rpc::{BAD_REQUEST_ERROR_CODE, INTERNAL_ERROR_CODE, NOT_FOUND_ERROR_CODE};
use crate::proto::blockchain::*;
use crate::proto::overlay;
use crate::storage::{BlockConnection, CoreStorage, KeyBlocksDirection, PersistentStateKind};

const RPC_METHOD_TIMINGS_METRIC: &str = "tycho_blockchain_rpc_method_time";

#[cfg(not(test))]
const BLOCK_DATA_CHUNK_SIZE: u32 = 1024 * 1024; // 1 MB
#[cfg(test)]
const BLOCK_DATA_CHUNK_SIZE: u32 = 10; // 10 bytes so we have zillions of chunks in tests

#[cfg(feature = "s3")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ProxyConfig {
    /// Rate limit (requests per second)
    ///
    /// Default: 100.
    pub rate_limit: NonZeroU32,

    /// Bandwidth limit (bytes per second)
    ///
    /// Default: 100 MiB (0 - unlimited)
    pub bandwidth_limit: ByteSize,
}

#[cfg(feature = "s3")]
impl Default for S3ProxyConfig {
    fn default() -> Self {
        Self {
            rate_limit: NonZeroU32::new(100).unwrap(),
            bandwidth_limit: ByteSize::mib(100),
        }
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

    /// S3 proxy configuration.
    ///
    /// Default: enabled.
    #[cfg(feature = "s3")]
    pub s3_proxy: Option<S3ProxyConfig>,
}

impl Default for BlockchainRpcServiceConfig {
    fn default() -> Self {
        Self {
            max_key_blocks_list_len: 8,
            serve_persistent_states: true,
            #[cfg(feature = "s3")]
            s3_proxy: Some(S3ProxyConfig::default()),
        }
    }
}

pub struct BlockchainRpcServiceBuilder<MandatoryFields> {
    config: BlockchainRpcServiceConfig,
    mandatory_fields: MandatoryFields,
}

impl<B> BlockchainRpcServiceBuilder<(B, CoreStorage, Arc<dyn RpcDataProvider>)>
where
    B: BroadcastListener,
{
    pub fn build(self) -> BlockchainRpcService<B> {
        let (broadcast_listener, storage, rpc_data_provider) = self.mandatory_fields;

        BlockchainRpcService {
            inner: Arc::new(Inner {
                storage,
                rpc_data_provider,
                config: self.config,
                broadcast_listener,
            }),
        }
    }
}

impl<B> BlockchainRpcServiceBuilder<(B, CoreStorage, ())>
where
    B: BroadcastListener,
{
    pub fn build(self) -> BlockchainRpcService<B> {
        let (broadcast_listener, storage, _) = self.mandatory_fields;
        let rpc_data_provider = storage.clone().into_data_provider();

        BlockchainRpcService {
            inner: Arc::new(Inner {
                storage,
                rpc_data_provider,
                config: self.config,
                broadcast_listener,
            }),
        }
    }
}

impl<T1, T3> BlockchainRpcServiceBuilder<(T1, (), T3)> {
    pub fn with_storage(
        self,
        storage: CoreStorage,
    ) -> BlockchainRpcServiceBuilder<(T1, CoreStorage, T3)> {
        let (broadcast_listener, _, archive_provider) = self.mandatory_fields;

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (broadcast_listener, storage, archive_provider),
        }
    }
}

impl<T2, T3> BlockchainRpcServiceBuilder<((), T2, T3)> {
    pub fn with_broadcast_listener<T1>(
        self,
        broadcast_listener: T1,
    ) -> BlockchainRpcServiceBuilder<(T1, T2, T3)>
    where
        T1: BroadcastListener,
    {
        let (_, storage, archive_provider) = self.mandatory_fields;

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (broadcast_listener, storage, archive_provider),
        }
    }

    pub fn without_broadcast_listener(
        self,
    ) -> BlockchainRpcServiceBuilder<(NoopBroadcastListener, T2, T3)> {
        let (_, storage, archive_provider) = self.mandatory_fields;

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (NoopBroadcastListener, storage, archive_provider),
        }
    }
}

impl<T1, T2> BlockchainRpcServiceBuilder<(T1, T2, ())> {
    pub fn with_data_provider<RP: IntoRpcDataProvider>(
        self,
        provider: RP,
    ) -> BlockchainRpcServiceBuilder<(T1, T2, Arc<dyn RpcDataProvider>)> {
        let (broadcast_listener, storage, _) = self.mandatory_fields;
        let rpc_data_provider = provider.into_data_provider();

        BlockchainRpcServiceBuilder {
            config: self.config,
            mandatory_fields: (broadcast_listener, storage, rpc_data_provider),
        }
    }
}

impl<T1, T2, T3> BlockchainRpcServiceBuilder<(T1, T2, T3)> {
    pub fn with_config(self, config: BlockchainRpcServiceConfig) -> Self {
        Self { config, ..self }
    }
}

#[repr(transparent)]
pub struct BlockchainRpcService<B = NoopBroadcastListener> {
    inner: Arc<Inner<B>>,
}

impl<B> Clone for BlockchainRpcService<B> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl BlockchainRpcService<()> {
    pub fn builder() -> BlockchainRpcServiceBuilder<((), (), ())> {
        BlockchainRpcServiceBuilder {
            config: Default::default(),
            mandatory_fields: ((), (), ()),
        }
    }
}

macro_rules! match_request {
     ($req_body:expr, $tag:expr, {
        $(
            #[meta($name:literal $(, $($args:tt)*)?)]
            $([$raw:tt])? $ty:path as $pat:pat => $expr:expr
        ),*$(,)?
    }, $err:pat => $err_exr:expr) => {
        '__match_req: {
            let $err = match $tag {
                $(<$ty>::TL_ID => match tl_proto::deserialize::<$ty>(&($req_body)) {
                        Ok($pat) => break '__match_req match_request!(
                            @expr
                            { $name $($($args)*)? }
                            { $($raw)? }
                            $expr
                        ),
                        Err(e) => e,
                })*
                _ => tl_proto::TlError::UnknownConstructor,
            };
            $err_exr
        }
    };

    // Raw expression (just use it as is).
    (@expr { $name:literal $($args:tt)* } { $($raw:tt)+ } $expr:expr) => {
        $expr
    };
    // Wrapped expression (adds debug log and metrics).
    (@expr { $name:literal $($args:tt)* } { } $expr:expr) => {{
        match_request!(@debug $name {} { $($args)* });

        let __started_at = std::time::Instant::now();

        BoxFutureOrNoop::future(async move {
            scopeguard::defer! {
                metrics::histogram!(
                    RPC_METHOD_TIMINGS_METRIC,
                    "method" => $name
                ).record(__started_at.elapsed());
            }

            Some(Response::from_tl($expr))
        })
    }};

    // Stuff to remove trailing comma from args.
    (@debug $name:literal { } { $(,)? }) => {
        match_request!(@debug_impl $name)
    };
    (@debug $name:literal { $($res:tt)+ } { $(,)? }) => {
        match_request!(@debug_impl $($res)+, $name)
    };
    (@debug $name:literal { $($res:tt)* } { $t:tt $($args:tt)* }) => {
        match_request!(@debug $name { $($res)* $t } { $($args)* })
    };
    (@debug_impl $($args:tt)*) => {
        tracing::debug!($($args)*)
    };
}

impl<B: BroadcastListener> Service<ServiceRequest> for BlockchainRpcService<B> {
    type QueryResponse = Response;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;

    #[tracing::instrument(level = "debug", name = "on_blockchain_service_query", skip_all)]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                return BoxFutureOrNoop::Noop;
            }
        };

        let inner = self.inner.clone();

        // NOTE: update `constructor_to_string` after adding new methods
        match_request!(body, constructor, {
            #[meta("ping")]
            [raw] overlay::Ping as _ => BoxFutureOrNoop::future(async {
                Some(Response::from_tl(overlay::Pong))
            }),

            #[meta(
                "getNextKeyBlockIds",
                block_id = %req.block_id,
                max_size = req.max_size,
            )]
            rpc::GetNextKeyBlockIds as req => inner.handle_get_next_key_block_ids(&req),

            #[meta("getBlockFull", block_id = %req.block_id)]
            rpc::GetBlockFull as req => inner.handle_get_block_full(&req).await,

            #[meta("getNextBlockFull", prev_block_id = %req.prev_block_id)]
            rpc::GetNextBlockFull as req => inner.handle_get_next_block_full(&req).await,

            #[meta(
                "getBlockDataChunk",
                block_id = %req.block_id,
                offset = %req.offset,
            )]
            rpc::GetBlockDataChunk as req => inner.handle_get_block_data_chunk(&req).await,

            #[meta("getKeyBlockProof", block_id = %req.block_id)]
            rpc::GetKeyBlockProof as req => inner.handle_get_key_block_proof(&req).await,

            #[meta("getZerostateProof")]
            rpc::GetZerostateProof as _ => inner.handle_get_zerostate_proof().await,

            #[meta("getPersistentShardStateInfo", block_id = %req.block_id)]
            rpc::GetPersistentShardStateInfo as req => inner.handle_get_persistent_state_info(&req).await,

            #[meta("getPersistentQueueStateInfo", block_id = %req.block_id)]
            rpc::GetPersistentQueueStateInfo as req => inner.handle_get_queue_persistent_state_info(&req).await,

            #[meta(
                "getPersistentShardStateChunk",
                block_id = %req.block_id,
                offset = %req.offset,
            )]
            rpc::GetPersistentShardStateChunk as req => {
                inner.handle_get_persistent_shard_state_chunk(&req).await
            },

            #[meta(
                "getPersistentQueueStateChunk",
                block_id = %req.block_id,
                offset = %req.offset,
            )]
            rpc::GetPersistentQueueStateChunk as req => {
                inner.handle_get_persistent_queue_state_chunk(&req).await
            },

            #[meta("getArchiveInfo", mc_seqno = %req.mc_seqno)]
            rpc::GetArchiveInfo as req => inner.handle_get_archive_info(&req).await,

            #[meta(
                "getArchiveChunk",
                archive_id = %req.archive_id,
                offset = %req.offset,
            )]
            rpc::GetArchiveChunk as req => inner.handle_get_archive_chunk(&req).await,
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
                    if let Err(e) = validate_external_message(&req.body).await {
                        tracing::debug!("invalid external message: {e:?}");
                        return;
                    }

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
}

struct Inner<B> {
    storage: CoreStorage,
    config: BlockchainRpcServiceConfig,
    broadcast_listener: B,
    rpc_data_provider: Arc<dyn RpcDataProvider>,
}

impl<B> Inner<B> {
    fn storage(&self) -> &CoreStorage {
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

    async fn handle_get_block_data_chunk(
        &self,
        req: &rpc::GetBlockDataChunk,
    ) -> overlay::Response<Data> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let handle = match block_handle_storage.load_handle(&req.block_id) {
            Some(handle) if handle.has_data() => handle,
            _ => {
                tracing::debug!("block data not found for chunked read");
                return overlay::Response::Err(NOT_FOUND_ERROR_CODE);
            }
        };

        let offset = req.offset as u64;
        match block_storage
            .load_block_data_range(&handle, offset, BLOCK_DATA_CHUNK_SIZE as u64)
            .await
        {
            Ok(Some(data)) => overlay::Response::Ok(Data { data }),
            Ok(None) => {
                tracing::debug!("block data chunk not found at offset {}", offset);
                overlay::Response::Err(NOT_FOUND_ERROR_CODE)
            }
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

    async fn handle_get_zerostate_proof(&self) -> overlay::Response<ZerostateProof> {
        let storage = self.storage().node_state();
        let proof_opt = storage.load_zerostate_proof_bytes();
        let result = match proof_opt {
            Some(proof) => ZerostateProof::Found { proof },
            None => ZerostateProof::NotFound,
        };

        overlay::Response::Ok(result)
    }

    async fn handle_get_archive_info(
        &self,
        req: &rpc::GetArchiveInfo,
    ) -> overlay::Response<ArchiveInfo> {
        match self.rpc_data_provider.get_archive_info(req.mc_seqno).await {
            Ok(info) => overlay::Response::Ok(info),
            Err(e) => {
                tracing::warn!(mc_seqno = req.mc_seqno, "get_archive_info failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_archive_chunk(
        &self,
        req: &rpc::GetArchiveChunk,
    ) -> overlay::Response<Data> {
        match self
            .rpc_data_provider
            .get_archive_chunk(req.archive_id as u32, req.offset)
            .await
        {
            Ok(data) => overlay::Response::Ok(Data { data }),
            Err(e) => {
                tracing::warn!(
                    archive_id = req.archive_id,
                    offset = req.offset,
                    "get_archive_chunk failed: {e:?}"
                );
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_persistent_state_info(
        &self,
        req: &rpc::GetPersistentShardStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        let label = [("method", "getPersistentShardStateInfo")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);
        match self
            .read_persistent_state_info(&req.block_id, PersistentStateKind::Shard)
            .await
        {
            Ok(info) => overlay::Response::Ok(info),
            Err(e) => {
                tracing::warn!(
                    block_id = ?req.block_id,
                    "get_persistent_state_info failed: {e:?}"
                );
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_queue_persistent_state_info(
        &self,
        req: &rpc::GetPersistentQueueStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        match self
            .read_persistent_state_info(&req.block_id, PersistentStateKind::Queue)
            .await
        {
            Ok(info) => overlay::Response::Ok(info),
            Err(e) => {
                tracing::warn!(
                    block_id = ?req.block_id,
                    "get_persistent_state_info failed: {e:?}"
                );
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    async fn handle_get_persistent_shard_state_chunk(
        &self,
        req: &rpc::GetPersistentShardStateChunk,
    ) -> overlay::Response<Data> {
        self.read_persistent_state_chunk(&req.block_id, req.offset, PersistentStateKind::Shard)
            .await
    }

    async fn handle_get_persistent_queue_state_chunk(
        &self,
        req: &rpc::GetPersistentQueueStateChunk,
    ) -> overlay::Response<Data> {
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

        // Get first chunk of compressed data
        let data = match block_storage
            .load_block_data_range(&handle, 0, BLOCK_DATA_CHUNK_SIZE as u64)
            .await?
        {
            Some(data) => data,
            None => return Ok(BlockFull::NotFound),
        };

        let data_size = if data.len() < BLOCK_DATA_CHUNK_SIZE as usize {
            // Small block - entire data fits in one chunk
            data.len() as u32
        } else {
            // Large block - need to get total size from storage
            match block_storage.get_compressed_block_data_size(&handle)? {
                Some(size) => size as u32,
                None => return Ok(BlockFull::NotFound),
            }
        };

        let block = BlockData {
            data,
            size: NonZeroU32::new(data_size).expect("shouldn't happen"),
            chunk_size: NonZeroU32::new(BLOCK_DATA_CHUNK_SIZE).expect("shouldn't happen"),
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

    async fn read_persistent_state_info(
        &self,
        block_id: &BlockId,
        state_kind: PersistentStateKind,
    ) -> anyhow::Result<PersistentStateInfo> {
        if self.config.serve_persistent_states
            && let Some(info) = self
                .rpc_data_provider
                .get_persistent_state_info(block_id, state_kind)
                .await?
        {
            return Ok(PersistentStateInfo::Found {
                size: info.size,
                chunk_size: info.chunk_size,
            });
        }

        Ok(PersistentStateInfo::NotFound)
    }

    async fn read_persistent_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        state_kind: PersistentStateKind,
    ) -> overlay::Response<Data> {
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

        match self
            .rpc_data_provider
            .get_persistent_state_chunk(block_id, offset, state_kind)
            .await
        {
            Ok(Some(data)) => overlay::Response::Ok(Data { data }),
            Ok(None) => overlay::Response::Err(NOT_FOUND_ERROR_CODE),
            Err(e) => {
                tracing::warn!(
                    ?block_id,
                    offset,
                    ?state_kind,
                    "get_persistent_state_chunk failed: {e:?}"
                );
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }
}
