mod handlers;
mod util;

use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures_util::Future;
use metrics::Label;
use serde::{Deserialize, Serialize};
use tycho_block_util::message::validate_external_message;
use tycho_network::{try_handle_prefix, InboundRequestMeta, Response, Service, ServiceRequest};
use tycho_storage::Storage;
use tycho_util::futures::BoxFutureOrNoop;
use tycho_util::metrics::HistogramGuard;

use crate::blockchain_rpc::service::util::{Constructor, RateLimiter};
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

    fn is_noop(&self) -> bool {
        false
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct NoopBroadcastListener;

impl BroadcastListener for NoopBroadcastListener {
    type HandleMessageFut<'a> = futures_util::future::Ready<()>;

    #[inline]
    fn handle_message(&self, _: Arc<InboundRequestMeta>, _: Bytes) -> Self::HandleMessageFut<'_> {
        futures_util::future::ready(())
    }

    #[inline]
    fn is_noop(&self) -> bool {
        true
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

    pub rate_limits: RateLimits,
}

impl Default for BlockchainRpcServiceConfig {
    fn default() -> Self {
        Self {
            max_key_blocks_list_len: 8,
            serve_persistent_states: true,
            rate_limits: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct RateLimits {
    /// rate limits for methods like `GetPersistentQueueStateInfo`, `GetArchiveInfo`, etc.
    pub info_method_rps: NonZeroU32,
    /// rate limits for methods like `GetPersistentQueueStateChunk`, `GetArchiveChunk`, etc.
    pub chunk_method_rps: NonZeroU32,

    /// Message broadcast rate limits
    pub send_message: NonZeroU32,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            info_method_rps: NonZeroU32::new(100).unwrap(),
            chunk_method_rps: NonZeroU32::new(100).unwrap(),
            send_message: NonZeroU32::new(10_000).unwrap(),
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
                info_rate_limiter: RateLimiter::dashmap_with_hasher(
                    governor::Quota::per_second(self.config.rate_limits.info_method_rps),
                    Default::default(),
                ),
                chunk_rate_limiter: RateLimiter::dashmap_with_hasher(
                    governor::Quota::per_second(self.config.rate_limits.chunk_method_rps),
                    Default::default(),
                ),
                send_message_rate_limiter: RateLimiter::dashmap_with_hasher(
                    governor::Quota::per_second(self.config.rate_limits.send_message),
                    Default::default(),
                ),
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

    #[tracing::instrument(level = "debug", name = "on_blockchain_service_query", skip_all)]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e}");
                return BoxFutureOrNoop::Noop;
            }
        };

        let method = Constructor::from_tl_id(constructor);
        let label = vec![Label::new(
            "method",
            method.map_or("unknown", |m| m.as_str()),
        )];
        let timer = {
            let label = label.clone();
            move || HistogramGuard::begin_with_labels_owned(RPC_METHOD_TIMINGS_METRIC, label)
        };

        if let Some(value) = self.inner.check_rate_limit(&req, method) {
            metrics::counter!("tycho_rpc_rate_limit_exceeded_total", label).increment(1);
            return value;
        }

        let inner = self.inner.clone();

        // NOTE: update `constructor_to_string` after adding new methods
        tycho_network::match_tl_request!(body, tag = constructor,
            {
            overlay::Ping as _ => BoxFutureOrNoop::future(async {
                Some(Response::from_tl(overlay::Pong))
            }),
            rpc::GetNextKeyBlockIds as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    max_size = req.max_size,
                    "getNextKeyBlockIds",
                );
                timed_future(timer, async move {
                    Some(Response::from_tl(inner.handle_get_next_key_block_ids(&req)))
                })
            },
            rpc::GetBlockFull as req => {
                tracing::debug!(block_id = %req.block_id, "getBlockFull");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_block_full(&req).await))
                })
            },
            rpc::GetNextBlockFull as req => {
                tracing::debug!(prev_block_id = %req.prev_block_id, "getNextBlockFull");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_next_block_full(&req).await))
                })
            },
            rpc::GetBlockDataChunk as req => {
                tracing::debug!(block_id = %req.block_id, offset = %req.offset, "getBlockDataChunk");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_block_data_chunk(&req)))
                })
            },
            rpc::GetKeyBlockProof as req => {
                tracing::debug!(block_id = %req.block_id, "getKeyBlockProof");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_key_block_proof(&req).await))
                })
            },
            rpc::GetPersistentShardStateInfo as req => {
                tracing::debug!(block_id = %req.block_id, "getPersistentShardStateInfo");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_persistent_state_info(&req)))
                })
            },
            rpc::GetPersistentQueueStateInfo as req => {
                tracing::debug!(block_id = %req.block_id, "getPersistentQueueStateInfo");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_queue_persistent_state_info(&req)))
                })
            },
            rpc::GetPersistentShardStateChunk as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    offset = %req.offset,
                    "getPersistentShardStateChunk"
                );
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_persistent_shard_state_chunk(&req).await))
                })
            },
            rpc::GetPersistentQueueStateChunk as req => {
                tracing::debug!(
                    block_id = %req.block_id,
                    offset = %req.offset,
                    "getPersistentQueueStateChunk"
                );
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_persistent_queue_state_chunk(&req).await))
                })
            },
            rpc::GetArchiveInfo as req => {
                tracing::debug!(mc_seqno = %req.mc_seqno, "getArchiveInfo");
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_archive_info(&req).await))
                })
            },
            rpc::GetArchiveChunk as req => {
                tracing::debug!(
                    archive_id = %req.archive_id,
                    offset = %req.offset,
                    "getArchiveChunk"
                );
                 timed_future(timer,async move {
                    Some(Response::from_tl(inner.handle_get_archive_chunk(&req).await))
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

        if self.inner.broadcast_listener.is_noop() {
            return BoxFutureOrNoop::Noop;
        }

        if self
            .inner
            .send_message_rate_limiter
            .check_key(&req.metadata.peer_id)
            .is_err()
        {
            metrics::counter!("tycho_rpc_rate_limit_exceeded_total", "method" => "sendMessage")
                .increment(1);
            return BoxFutureOrNoop::Noop;
        }

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

pub fn timed_future<F, Timer, TimerRet, T>(t: Timer, f: F) -> BoxFutureOrNoop<T>
where
    F: Future<Output = T> + Send + 'static,
    Timer: FnOnce() -> TimerRet + Send + 'static,
    TimerRet: Send + 'static,
    T: 'static,
{
    let future = async move {
        let _timer = t();
        f.await
    };
    BoxFutureOrNoop::future(future)
}

struct Inner<B> {
    storage: Storage,
    config: BlockchainRpcServiceConfig,
    broadcast_listener: B,
    info_rate_limiter: RateLimiter,
    chunk_rate_limiter: RateLimiter,
    send_message_rate_limiter: RateLimiter,
}

impl<B> Inner<B> {
    fn storage(&self) -> &Storage {
        &self.storage
    }

    fn check_rate_limit(
        &self,
        req: &ServiceRequest,
        method: Option<Constructor>,
    ) -> Option<BoxFutureOrNoop<Option<Response>>> {
        let rate_limiter = match method {
            Some(
                Constructor::GetPersistentQueueStateChunk
                | Constructor::GetArchiveChunk
                | Constructor::GetPersistentShardStateChunk
                | Constructor::GetBlockDataChunk
                | Constructor::GetNextBlockFull
                | Constructor::GetBlockFull
                | Constructor::GetNextKeyBlockIds
                | Constructor::GetKeyBlockProof,
            ) => &self.chunk_rate_limiter,
            Some(
                Constructor::GetPersistentShardStateInfo
                | Constructor::GetPersistentQueueStateInfo
                | Constructor::GetArchiveInfo
                | Constructor::Ping,
            ) => &self.info_rate_limiter,
            None => return Some(BoxFutureOrNoop::Noop),
        };

        rate_limiter
            .check_key(&req.metadata.peer_id)
            .err()
            .map(|_| BoxFutureOrNoop::Noop)
    }
}
