use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tarpc::server::Channel;
use tycho_core::block_strider::{GcSubscriber, ManualGcTrigger};
use tycho_storage::Storage;

use crate::error::ServerResult;
use crate::profiler::{MemoryProfiler, StubMemoryProfiler};
use crate::proto::{self, ControlServer as _};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlServerConfig {
    /// Unix socket path to listen for incoming control connections.
    ///
    /// Default: `/var/venom/data/tycho.sock`
    pub socket_path: PathBuf,
}

impl Default for ControlServerConfig {
    fn default() -> Self {
        Self {
            socket_path: crate::DEFAULT_SOCKET_PATH.into(),
        }
    }
}

pub struct ControlEndpoint {
    inner: BoxFuture<'static, ()>,
}

impl ControlEndpoint {
    pub async fn bind(
        config: &ControlServerConfig,
        server: ControlServer,
    ) -> std::io::Result<Self> {
        use tarpc::tokio_serde::formats::Bincode;

        let mut listener =
            tarpc::serde_transport::unix::listen(&config.socket_path, Bincode::default).await?;
        listener.config_mut().max_frame_length(usize::MAX);

        let inner = listener
            // Ignore accept errors.
            .filter_map(|r| futures_util::future::ready(r.ok()))
            .map(tarpc::server::BaseChannel::with_defaults)
            .map(move |channel| {
                channel.execute(server.clone().serve()).for_each(|f| {
                    tokio::spawn(f);
                    futures_util::future::ready(())
                })
            })
            // Max 1 channel.
            .buffer_unordered(1)
            .for_each(|_| async {})
            .boxed();

        Ok(Self { inner })
    }

    pub async fn serve(self) {
        self.inner.await;
    }
}

pub struct ControlServerBuilder<MandatoryFields = (Storage, GcSubscriber)> {
    mandatory_fields: MandatoryFields,
    memory_profiler: Option<Arc<dyn MemoryProfiler>>,
}

impl ControlServerBuilder {
    pub fn build(self) -> ControlServer {
        let (storage, gc_subscriber) = self.mandatory_fields;
        let memory_profiler = self
            .memory_profiler
            .unwrap_or_else(|| Arc::new(StubMemoryProfiler));

        ControlServer {
            inner: Arc::new(Inner {
                gc_subscriber,
                storage,
                memory_profiler,
            }),
        }
    }
}

impl<T2> ControlServerBuilder<((), T2)> {
    pub fn with_storage(self, storage: Storage) -> ControlServerBuilder<(Storage, T2)> {
        let (_, t2) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (storage, t2),
            memory_profiler: self.memory_profiler,
        }
    }
}

impl<T1> ControlServerBuilder<(T1, ())> {
    pub fn with_gc_subscriber(
        self,
        gc_subscriber: GcSubscriber,
    ) -> ControlServerBuilder<(T1, GcSubscriber)> {
        let (t1, _) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (t1, gc_subscriber),
            memory_profiler: self.memory_profiler,
        }
    }
}

impl<T> ControlServerBuilder<T> {
    pub fn with_memory_profiler(
        self,
        memory_profiler: Arc<dyn MemoryProfiler>,
    ) -> ControlServerBuilder<T> {
        ControlServerBuilder {
            mandatory_fields: self.mandatory_fields,
            memory_profiler: Some(memory_profiler),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ControlServer {
    inner: Arc<Inner>,
}

impl ControlServer {
    pub fn builder() -> ControlServerBuilder<((), ())> {
        ControlServerBuilder {
            mandatory_fields: ((), ()),
            memory_profiler: None,
        }
    }
}

impl proto::ControlServer for ControlServer {
    async fn ping(self, _: Context) -> u64 {
        tycho_util::time::now_millis()
    }

    async fn trigger_archives_gc(self, _: Context, trigger: ManualGcTrigger) {
        self.inner.gc_subscriber.trigger_archives_gc(trigger);
    }

    async fn trigger_blocks_gc(self, _: Context, trigger: ManualGcTrigger) {
        self.inner.gc_subscriber.trigger_blocks_gc(trigger);
    }

    async fn trigger_states_gc(self, _: Context, trigger: ManualGcTrigger) {
        self.inner.gc_subscriber.trigger_states_gc(trigger);
    }

    async fn set_memory_profiler_enabled(self, _: Context, enabled: bool) -> bool {
        self.inner.memory_profiler.set_enabled(enabled).await
    }

    async fn dump_memory_profiler(self, _: Context) -> ServerResult<Vec<u8>> {
        self.inner.memory_profiler.dump().await.map_err(Into::into)
    }

    async fn get_block(
        self,
        _: Context,
        req: proto::BlockRequest,
    ) -> ServerResult<proto::BlockResponse> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let Some(handle) = handles.load_handle(&req.block_id) else {
            return Ok(proto::BlockResponse::NotFound);
        };

        let data = blocks.load_block_data_raw(&handle).await?;
        Ok(proto::BlockResponse::Found { data })
    }

    async fn get_block_proof(
        self,
        _: Context,
        req: proto::BlockProofRequest,
    ) -> ServerResult<proto::BlockProofResponse> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let Some(handle) = handles.load_handle(&req.block_id) else {
            return Ok(proto::BlockProofResponse::NotFound);
        };

        let data = blocks.load_block_proof_raw(&handle).await?;
        Ok(proto::BlockProofResponse::Found { data })
    }

    async fn get_archive_info(
        self,
        _: Context,
        req: proto::ArchiveInfoRequest,
    ) -> ServerResult<proto::ArchiveInfoResponse> {
        let blocks = self.inner.storage.block_storage();

        let Some(id) = blocks.get_archive_id(req.mc_seqno) else {
            return Ok(proto::ArchiveInfoResponse::NotFound);
        };

        let Some(size) = blocks.get_archive_size(id)? else {
            return Ok(proto::ArchiveInfoResponse::NotFound);
        };

        Ok(proto::ArchiveInfoResponse::Found(proto::ArchiveInfo {
            id,
            size: NonZeroU64::new(size as _).unwrap(),
            chunk_size: blocks.archive_chunk_size(),
        }))
    }

    async fn get_archive_chunk(
        self,
        _: Context,
        req: proto::ArchiveSliceRequest,
    ) -> ServerResult<proto::ArchiveSliceResponse> {
        let blocks = self.inner.storage.block_storage();

        let data = blocks.get_archive_chunk(req.archive_id, req.offset).await?;

        Ok(proto::ArchiveSliceResponse { data })
    }
}

struct Inner {
    gc_subscriber: GcSubscriber,
    storage: Storage,
    memory_profiler: Arc<dyn MemoryProfiler>,
}

type Context = tarpc::context::Context;
