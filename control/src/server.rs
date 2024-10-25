use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use everscale_crypto::ed25519;
use everscale_types::cell::HashBytes;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tarpc::server::Channel;
use tycho_core::block_strider::{GcSubscriber, ManualGcTrigger};
use tycho_network::Network;
use tycho_storage::{ArchiveId, Storage};

use crate::error::{ServerError, ServerResult};
use crate::profiler::{MemoryProfiler, StubMemoryProfiler};
use crate::proto::{self, ArchiveInfo, ControlServer as _};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlServerConfig {
    /// Whether to recreate the socket file if it already exists.
    ///
    /// NOTE: If the `socket_path` from multiple instances on the same machine
    /// points to the same file, every instance will just "grab" it to itself.
    ///
    /// Default: `false`
    pub overwrite_socket: bool,

    /// Maximum number of parallel connections.
    ///
    /// Default: `100`
    pub max_connections: usize,
}

impl Default for ControlServerConfig {
    fn default() -> Self {
        Self {
            overwrite_socket: false,
            max_connections: 100,
        }
    }
}

pub struct ControlEndpoint {
    inner: BoxFuture<'static, ()>,
    socket_path: PathBuf,
}

impl ControlEndpoint {
    pub async fn bind<P: AsRef<Path>>(
        config: &ControlServerConfig,
        server: ControlServer,
        socket_path: P,
    ) -> std::io::Result<Self> {
        use tarpc::tokio_serde::formats::Bincode;

        let socket_path = socket_path.as_ref().to_path_buf();
        if config.overwrite_socket && socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let mut listener =
            tarpc::serde_transport::unix::listen(&socket_path, Bincode::default).await?;
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
            // Max N channels.
            .buffer_unordered(config.max_connections)
            .for_each(|_| async {})
            .boxed();

        Ok(Self { inner, socket_path })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn serve(mut self) {
        (&mut self.inner).await;
    }
}

impl Drop for ControlEndpoint {
    fn drop(&mut self) {
        _ = std::fs::remove_file(&self.socket_path);
    }
}

pub struct ControlServerBuilder<MandatoryFields = (Network, Storage, GcSubscriber)> {
    mandatory_fields: MandatoryFields,
    memory_profiler: Option<Arc<dyn MemoryProfiler>>,
    validator_keypair: Option<Arc<ed25519::KeyPair>>,
}

impl ControlServerBuilder {
    pub fn build(self) -> ControlServer {
        let (network, storage, gc_subscriber) = self.mandatory_fields;
        let memory_profiler = self
            .memory_profiler
            .unwrap_or_else(|| Arc::new(StubMemoryProfiler));

        let info = proto::NodeInfoResponse {
            public_addr: network.remote_addr().to_string(),
            local_addr: network.local_addr(),
            adnl_id: HashBytes(network.peer_id().to_bytes()),
            validator_public_key: self
                .validator_keypair
                .as_ref()
                .map(|k| HashBytes(k.public_key.to_bytes())),
        };

        ControlServer {
            inner: Arc::new(Inner {
                info,
                gc_subscriber,
                storage,
                memory_profiler,
                validator_keypair: self.validator_keypair,
            }),
        }
    }
}

impl<T2, T3> ControlServerBuilder<((), T2, T3)> {
    pub fn with_network(self, network: &Network) -> ControlServerBuilder<(Network, T2, T3)> {
        let (_, t2, t3) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (network.clone(), t2, t3),
            memory_profiler: self.memory_profiler,
            validator_keypair: self.validator_keypair,
        }
    }
}

impl<T1, T3> ControlServerBuilder<(T1, (), T3)> {
    pub fn with_storage(self, storage: Storage) -> ControlServerBuilder<(T1, Storage, T3)> {
        let (t1, _, t3) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (t1, storage, t3),
            memory_profiler: self.memory_profiler,
            validator_keypair: self.validator_keypair,
        }
    }
}

impl<T1, T2> ControlServerBuilder<(T1, T2, ())> {
    pub fn with_gc_subscriber(
        self,
        gc_subscriber: GcSubscriber,
    ) -> ControlServerBuilder<(T1, T2, GcSubscriber)> {
        let (t1, t2, _) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (t1, t2, gc_subscriber),
            memory_profiler: self.memory_profiler,
            validator_keypair: self.validator_keypair,
        }
    }
}

impl<T> ControlServerBuilder<T> {
    pub fn with_memory_profiler(mut self, memory_profiler: Arc<dyn MemoryProfiler>) -> Self {
        self.memory_profiler = Some(memory_profiler);
        self
    }

    pub fn with_validator_keypair(mut self, keypair: Arc<ed25519::KeyPair>) -> Self {
        self.validator_keypair = Some(keypair);
        self
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ControlServer {
    inner: Arc<Inner>,
}

impl ControlServer {
    pub fn builder() -> ControlServerBuilder<((), (), ())> {
        ControlServerBuilder {
            mandatory_fields: ((), (), ()),
            memory_profiler: None,
            validator_keypair: None,
        }
    }
}

impl proto::ControlServer for ControlServer {
    async fn ping(self, _: Context) -> u64 {
        tycho_util::time::now_millis()
    }

    async fn get_node_info(self, _: tarpc::context::Context) -> proto::NodeInfoResponse {
        self.inner.info.clone()
    }

    async fn trigger_archives_gc(self, _: Context, req: proto::TriggerGcRequest) {
        self.inner.gc_subscriber.trigger_archives_gc(req.into());
    }

    async fn trigger_blocks_gc(self, _: Context, req: proto::TriggerGcRequest) {
        self.inner.gc_subscriber.trigger_blocks_gc(req.into());
    }

    async fn trigger_states_gc(self, _: Context, req: proto::TriggerGcRequest) {
        self.inner.gc_subscriber.trigger_states_gc(req.into());
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

        let data = blocks.load_block_data_raw(&handle).await?.to_vec();
        Ok(proto::BlockResponse::Found { data })
    }

    async fn get_block_proof(
        self,
        _: Context,
        req: proto::BlockRequest,
    ) -> ServerResult<proto::BlockResponse> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let Some(handle) = handles.load_handle(&req.block_id) else {
            return Ok(proto::BlockResponse::NotFound);
        };

        let data = blocks.load_block_proof_raw(&handle).await?.to_vec();
        Ok(proto::BlockResponse::Found { data })
    }

    async fn get_queue_diff(
        self,
        _: Context,
        req: proto::BlockRequest,
    ) -> ServerResult<proto::BlockResponse> {
        let blocks = self.inner.storage.block_storage();
        let handles = self.inner.storage.block_handle_storage();

        let Some(handle) = handles.load_handle(&req.block_id) else {
            return Ok(proto::BlockResponse::NotFound);
        };

        let data = blocks.load_queue_diff_raw(&handle).await?.to_vec();
        Ok(proto::BlockResponse::Found { data })
    }

    async fn get_archive_info(
        self,
        _: Context,
        req: proto::ArchiveInfoRequest,
    ) -> ServerResult<proto::ArchiveInfoResponse> {
        let blocks = self.inner.storage.block_storage();

        let id = match blocks.get_archive_id(req.mc_seqno) {
            ArchiveId::Found(id) => id,
            ArchiveId::TooNew => return Ok(proto::ArchiveInfoResponse::TooNew),
            ArchiveId::NotFound => return Ok(proto::ArchiveInfoResponse::NotFound),
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

        let data = blocks
            .get_archive_chunk(req.archive_id, req.offset)
            .await?
            .to_vec();
        Ok(proto::ArchiveSliceResponse { data })
    }

    async fn get_archive_ids(self, _: tarpc::context::Context) -> ServerResult<Vec<ArchiveInfo>> {
        let storage = self.inner.storage.block_storage();
        let ids = storage
            .list_archive_ids()
            .into_iter()
            .filter_map(|id| {
                let size = storage.get_archive_size(id).unwrap()?;
                Some(ArchiveInfo {
                    id,
                    size: NonZeroU64::new(size as _).unwrap(),
                    chunk_size: storage.archive_chunk_size(),
                })
            })
            .collect();
        Ok(ids)
    }

    async fn get_block_ids(
        self,
        _: tarpc::context::Context,
        req: proto::BlockListRequest,
    ) -> ServerResult<proto::BlockListResponse> {
        let storage = self.inner.storage.block_storage();
        let (blocks, continuation) = storage.list_blocks(req.continuation).await?;
        Ok(proto::BlockListResponse {
            blocks,
            continuation,
        })
    }

    async fn sign_elections_payload(
        self,
        _: tarpc::context::Context,
        req: proto::ElectionsPayloadRequest,
    ) -> ServerResult<proto::ElectionsPayloadResponse> {
        let Some(keypair) = self.inner.validator_keypair.as_ref() else {
            return Err(ServerError::new(
                "control server was created without a keystore",
            ));
        };

        if keypair.public_key.as_bytes() != req.public_key.as_array() {
            return Err(ServerError::new(
                "no validator key found for the specified public key",
            ));
        }

        let data = build_elections_data_to_sign(&req);
        let signature = keypair.sign_raw(&data);

        Ok(proto::ElectionsPayloadResponse {
            data,
            public_key: HashBytes(keypair.public_key.to_bytes()),
            signature: Box::new(signature),
        })
    }
}

struct Inner {
    info: proto::NodeInfoResponse,
    gc_subscriber: GcSubscriber,
    storage: Storage,
    memory_profiler: Arc<dyn MemoryProfiler>,
    validator_keypair: Option<Arc<ed25519::KeyPair>>,
}

type Context = tarpc::context::Context;

impl From<ManualGcTrigger> for proto::TriggerGcRequest {
    fn from(value: ManualGcTrigger) -> Self {
        match value {
            ManualGcTrigger::Exact(mc_seqno) => Self::Exact(mc_seqno),
            ManualGcTrigger::Distance(distance) => Self::Distance(distance),
        }
    }
}

impl From<proto::TriggerGcRequest> for ManualGcTrigger {
    fn from(value: proto::TriggerGcRequest) -> Self {
        match value {
            proto::TriggerGcRequest::Exact(mc_seqno) => Self::Exact(mc_seqno),
            proto::TriggerGcRequest::Distance(distance) => Self::Distance(distance),
        }
    }
}

fn build_elections_data_to_sign(req: &proto::ElectionsPayloadRequest) -> Vec<u8> {
    const TL_ID: u32 = 0x654C5074;

    let mut data = Vec::with_capacity(4 + 4 + 4 + 32 + 32);
    data.extend_from_slice(&TL_ID.to_be_bytes());
    data.extend_from_slice(&req.election_id.to_be_bytes());
    data.extend_from_slice(&req.max_factor.to_be_bytes());
    data.extend_from_slice(req.address.as_slice());
    data.extend_from_slice(req.adnl_addr.as_array());
    data
}
