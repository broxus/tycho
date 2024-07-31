use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{GcSubscriber, ManualGcTrigger};
use tycho_storage::Storage;

use crate::server::{
    ArchiveInfo, ArchiveInfoRequest, ArchiveInfoResponse, ArchiveSliceRequest,
    ArchiveSliceResponse, BlockProofRequest, BlockProofResponse, BlockRequest, BlockResponse,
    ControlServer, ServerResult,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlServerStdImplConfig {
    /// TCP socket address to listen for incoming control connections.
    ///
    /// Default: `127.0.0.1:9000`
    // TODO: Replace with unix socket
    pub listen_addr: SocketAddr,
}

impl Default for ControlServerStdImplConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9000)),
        }
    }
}

#[async_trait::async_trait]
pub trait MemoryProfiler: Send + Sync + 'static {
    async fn set_enabled(&self, enabled: bool) -> bool;
    async fn dump(&self) -> Result<Vec<u8>>;
}

#[async_trait::async_trait]
impl<T: MemoryProfiler> MemoryProfiler for Arc<T> {
    async fn set_enabled(&self, enabled: bool) -> bool {
        T::set_enabled(self, enabled).await
    }

    async fn dump(&self) -> Result<Vec<u8>> {
        T::dump(self).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StubMemoryProfiler;

#[async_trait::async_trait]
impl MemoryProfiler for StubMemoryProfiler {
    async fn set_enabled(&self, _: bool) -> bool {
        false
    }

    async fn dump(&self) -> Result<Vec<u8>> {
        anyhow::bail!("stub memory profiler does not support dumping data")
    }
}

pub struct ControlServerStdBuilder<MandatoryFields = (Storage, GcSubscriber)> {
    mandatory_fields: MandatoryFields,
    memory_profiler: Option<Arc<dyn MemoryProfiler>>,
}

impl ControlServerStdBuilder {
    pub fn build(self) -> ControlServerStdImpl {
        let (storage, gc_subscriber) = self.mandatory_fields;
        let memory_profiler = self
            .memory_profiler
            .unwrap_or_else(|| Arc::new(StubMemoryProfiler));

        ControlServerStdImpl {
            inner: Arc::new(Inner {
                gc_subscriber,
                storage,
                memory_profiler,
            }),
        }
    }
}

impl<T2> ControlServerStdBuilder<((), T2)> {
    pub fn with_storage(self, storage: Storage) -> ControlServerStdBuilder<(Storage, T2)> {
        let (_, t2) = self.mandatory_fields;
        ControlServerStdBuilder {
            mandatory_fields: (storage, t2),
            memory_profiler: self.memory_profiler,
        }
    }
}

impl<T1> ControlServerStdBuilder<(T1, ())> {
    pub fn with_gc_subscriber(
        self,
        gc_subscriber: GcSubscriber,
    ) -> ControlServerStdBuilder<(T1, GcSubscriber)> {
        let (t1, _) = self.mandatory_fields;
        ControlServerStdBuilder {
            mandatory_fields: (t1, gc_subscriber),
            memory_profiler: self.memory_profiler,
        }
    }
}

impl<T> ControlServerStdBuilder<T> {
    pub fn with_memory_profiler(
        self,
        memory_profiler: Arc<dyn MemoryProfiler>,
    ) -> ControlServerStdBuilder<T> {
        ControlServerStdBuilder {
            mandatory_fields: self.mandatory_fields,
            memory_profiler: Some(memory_profiler),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ControlServerStdImpl {
    inner: Arc<Inner>,
}

impl ControlServerStdImpl {
    pub fn builder() -> ControlServerStdBuilder<((), ())> {
        ControlServerStdBuilder {
            mandatory_fields: ((), ()),
            memory_profiler: None,
        }
    }
}

impl ControlServer for ControlServerStdImpl {
    async fn ping(self, _: crate::Context) -> u64 {
        tycho_util::time::now_millis()
    }

    async fn trigger_archives_gc(self, _: crate::Context, trigger: ManualGcTrigger) {
        self.inner.gc_subscriber.trigger_archives_gc(trigger);
    }

    async fn trigger_blocks_gc(self, _: crate::Context, trigger: ManualGcTrigger) {
        self.inner.gc_subscriber.trigger_blocks_gc(trigger);
    }

    async fn trigger_states_gc(self, _: crate::Context, trigger: ManualGcTrigger) {
        self.inner.gc_subscriber.trigger_states_gc(trigger);
    }

    async fn set_memory_profiler_enabled(self, _: crate::Context, enabled: bool) -> bool {
        self.inner.memory_profiler.set_enabled(enabled).await
    }

    async fn dump_memory_profiler(self, _: crate::Context) -> ServerResult<Vec<u8>> {
        self.inner.memory_profiler.dump().await.map_err(Into::into)
    }

    async fn get_block(self, _: crate::Context, req: BlockRequest) -> ServerResult<BlockResponse> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let Some(handle) = handles.load_handle(&req.block_id) else {
            return Ok(BlockResponse::NotFound);
        };

        let data = blocks.load_block_data_raw(&handle).await?;
        Ok(BlockResponse::Found { data })
    }

    async fn get_block_proof(
        self,
        _: crate::Context,
        req: BlockProofRequest,
    ) -> ServerResult<BlockProofResponse> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let Some(handle) = handles.load_handle(&req.block_id) else {
            return Ok(BlockProofResponse::NotFound);
        };

        let is_link = !req.block_id.is_masterchain();
        let data = blocks.load_block_proof_raw(&handle, is_link).await?;
        Ok(BlockProofResponse::Found { data })
    }

    async fn get_archive_info(
        self,
        _: crate::Context,
        req: ArchiveInfoRequest,
    ) -> ServerResult<ArchiveInfoResponse> {
        let blocks = self.inner.storage.block_storage();

        let Some(id) = blocks.get_archive_id(req.mc_seqno) else {
            return Ok(ArchiveInfoResponse::NotFound);
        };

        let Some(size) = blocks.get_archive_size(id)? else {
            return Ok(ArchiveInfoResponse::NotFound);
        };

        Ok(ArchiveInfoResponse::Found(ArchiveInfo {
            id,
            size: size as u64,
        }))
    }

    async fn get_archive_slice(
        self,
        _: crate::Context,
        req: ArchiveSliceRequest,
    ) -> ServerResult<ArchiveSliceResponse> {
        let blocks = self.inner.storage.block_storage();

        let Some(data) =
            blocks.get_archive_slice(req.archive_id, req.offset as usize, req.limit as usize)?
        else {
            return Err(anyhow::anyhow!("archive not found").into());
        };

        Ok(ArchiveSliceResponse { data })
    }
}

crate::impl_serve!(ControlServerStdImpl);

struct Inner {
    gc_subscriber: GcSubscriber,
    storage: Storage,
    memory_profiler: Arc<dyn MemoryProfiler>,
}
