use std::borrow::Cow;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use anyhow::{Context as _, Result};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use parking_lot::RwLock;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tarpc::server::Channel;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::config::build_elections_data_to_sign;
use tycho_block_util::state::RefMcStateHandle;
use tycho_core::block_strider::{StateSubscriber, StateSubscriberContext};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_core::storage::{ArchiveId, BlockHandle, BlockStorage, CoreStorage, ManualGcTrigger};
use tycho_crypto::ed25519;
use tycho_network::{
    DhtClient, Network, NetworkExt, OverlayId, OverlayService, PeerId, PeerResolverHandle, Request,
};
use tycho_types::cell::Lazy;
use tycho_types::models::{
    AccountState, DepthBalanceInfo, Message, OptionalAccount, ShardAccount, ShardIdent, StdAddr,
};
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_util::{FastHashMap, FastHashSet};

use crate::collator::Collator;
use crate::error::{ServerError, ServerResult};
use crate::profiler::{MemoryProfiler, StubMemoryProfiler};
use crate::proto::{self, ArchiveInfo, ControlServer as _};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlServerVersion {
    pub version: String,
    pub build: String,
}

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

        // TODO: Add some kind of file lock and use a raw fd.
        if socket_path.exists() {
            // There is no reliable way to guarantee that the socket file
            // was removed when the node is stopped. In case of panic
            // or crash it will leave it as is.
            //
            // The `overwrite_socket` setting might be a bit dangerous to use,
            // so we try check here whether the file is in use.

            match std::os::unix::net::UnixStream::connect(&socket_path) {
                // There is already a listener on this socket, but the
                // config says that we must replace the file with a new one.
                Ok(_) if config.overwrite_socket => {
                    tracing::warn!("overwriting an existing control socket");
                    std::fs::remove_file(&socket_path)?;
                }
                // There is already a listener on this socket. Fallback to `listen`,
                // it will fail with a proper error.
                Ok(_) => {}
                // `ConnectionRefused` error for Unix sockets means that there
                // are no listeners, so we can safely remove the file.
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(&socket_path)?;
                }
                // We can ignore all other errors since the stream creation
                // is not the main intention of this check. Fallback to `listen`,
                // it will fail with a proper error.
                Err(_) => {}
            }
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

pub struct ControlServerBuilder<MandatoryFields = (Network, CoreStorage, BlockchainRpcClient)> {
    mandatory_fields: MandatoryFields,
    memory_profiler: Option<Arc<dyn MemoryProfiler>>,
    validator_keypair: Option<Arc<ed25519::KeyPair>>,
    collator: Option<Arc<dyn Collator>>,
    dht_client: Option<DhtClient>,
    overlay_service: Option<OverlayService>,
}

impl ControlServerBuilder {
    pub async fn build(self, version: ControlServerVersion) -> Result<ControlServer> {
        let (network, storage, blockchain_rpc_client) = self.mandatory_fields;
        let memory_profiler = self
            .memory_profiler
            .unwrap_or_else(|| Arc::new(StubMemoryProfiler));

        let config_response = 'config: {
            let Some(mc_block_id) = storage.node_state().load_last_mc_block_id() else {
                break 'config None;
            };

            let mc_state = storage
                .shard_state_storage()
                .load_state(mc_block_id.seqno, &mc_block_id)
                .await?;

            let config = mc_state.config_params()?;

            Some(Arc::new(proto::BlockchainConfigResponse {
                global_id: mc_state.as_ref().global_id,
                mc_seqno: mc_state.block_id().seqno,
                gen_utime: mc_state.as_ref().gen_utime,
                config: BocRepr::encode_rayon(config)?.into(),
            }))
        };

        let node_info = proto::NodeInfo {
            version: version.version,
            build: version.build,
            public_addr: network.remote_addr().to_string(),
            local_addr: network.local_addr(),
            adnl_id: HashBytes(network.peer_id().to_bytes()),
            collator: match self.collator {
                None => None,
                Some(collator) => {
                    let global_version = collator.get_global_version().await;
                    Some(proto::CollatorInfo { global_version })
                }
            },
        };

        let manual_compaction = ManualCompaction::new(storage.clone());

        Ok(ControlServer {
            inner: Arc::new(Inner {
                node_info,
                config_response: ArcSwapOption::new(config_response),
                storage,
                blockchain_rpc_client,
                manual_compaction,
                memory_profiler,
                validator_keypair: self.validator_keypair,
                dht_client: self.dht_client,
                overlay_service: self.overlay_service,
                mc_accounts: Default::default(),
                sc_accounts: Default::default(),
            }),
        })
    }
}

impl<T2, T3> ControlServerBuilder<((), T2, T3)> {
    pub fn with_network(self, network: &Network) -> ControlServerBuilder<(Network, T2, T3)> {
        let (_, t2, t3) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (network.clone(), t2, t3),
            memory_profiler: self.memory_profiler,
            validator_keypair: self.validator_keypair,
            collator: self.collator,
            dht_client: self.dht_client,
            overlay_service: self.overlay_service,
        }
    }
}

impl<T1, T3> ControlServerBuilder<(T1, (), T3)> {
    pub fn with_storage(self, storage: CoreStorage) -> ControlServerBuilder<(T1, CoreStorage, T3)> {
        let (t1, _, t3) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (t1, storage, t3),
            memory_profiler: self.memory_profiler,
            validator_keypair: self.validator_keypair,
            collator: self.collator,
            dht_client: self.dht_client,
            overlay_service: self.overlay_service,
        }
    }
}

impl<T1, T2> ControlServerBuilder<(T1, T2, ())> {
    pub fn with_blockchain_rpc_client(
        self,
        client: BlockchainRpcClient,
    ) -> ControlServerBuilder<(T1, T2, BlockchainRpcClient)> {
        let (t1, t2, _) = self.mandatory_fields;
        ControlServerBuilder {
            mandatory_fields: (t1, t2, client),
            memory_profiler: self.memory_profiler,
            validator_keypair: self.validator_keypair,
            collator: self.collator,
            dht_client: self.dht_client,
            overlay_service: self.overlay_service,
        }
    }
}

impl<T> ControlServerBuilder<T> {
    pub fn with_memory_profiler(mut self, memory_profiler: Arc<dyn MemoryProfiler>) -> Self {
        self.memory_profiler = Some(memory_profiler);
        self
    }

    pub fn with_collator(mut self, collator: Arc<dyn Collator>) -> Self {
        self.collator = Some(collator);
        self
    }

    pub fn with_validator_keypair(mut self, keypair: Arc<ed25519::KeyPair>) -> Self {
        self.validator_keypair = Some(keypair);
        self
    }

    pub fn with_dht_client(mut self, client: DhtClient) -> Self {
        self.dht_client = Some(client);
        self
    }

    pub fn with_overlay_service(mut self, service: OverlayService) -> Self {
        self.overlay_service = Some(service);
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
            collator: None,
            dht_client: None,
            overlay_service: None,
        }
    }
}

impl proto::ControlServer for ControlServer {
    async fn ping(self, _: Context) -> u64 {
        tycho_util::time::now_millis()
    }

    async fn get_status(
        self,
        ctx: tarpc::context::Context,
    ) -> ServerResult<proto::NodeStatusResponse> {
        let node_state = self.inner.storage.node_state();
        let block_handles = self.inner.storage.block_handle_storage();

        let init_block_id = node_state.load_init_mc_block_id();

        // TODO: Use handle from cached mc accounts.
        //       (but in that case we must fill the cache on init).
        let last_applied_block = node_state
            .load_last_mc_block_id()
            .and_then(|block_id| block_handles.load_handle(&block_id))
            .map(|handle| proto::LastAppliedBlock {
                block_id: *handle.id(),
                gen_utime: handle.gen_utime(),
            });

        let status_at = tycho_util::time::now_sec();
        let node_info = self.inner.node_info.clone();

        let validator_status = match &self.inner.validator_keypair {
            None => None,
            Some(keypair) => {
                let public_key = HashBytes(keypair.public_key.to_bytes());

                let parse_config = |res: proto::BlockchainConfigResponse| {
                    let res = res.parse()?;
                    let elector_address = res.config.get_elector_address()?;

                    let current_vset = res.config.get_current_validator_set()?;
                    let in_current_vset = current_vset
                        .list
                        .iter()
                        .any(|vld| vld.public_key == public_key);

                    let next_vset = res.config.get_next_validator_set()?;
                    let has_next_vset = next_vset.is_some();
                    let in_next_vset = match next_vset {
                        None => false,
                        Some(vset) => vset.list.iter().any(|vld| vld.public_key == public_key),
                    };

                    Ok::<_, anyhow::Error>((
                        elector_address,
                        in_current_vset,
                        in_next_vset,
                        has_next_vset,
                    ))
                };

                let parse_elector = |res: proto::AccountStateResponse| {
                    #[derive(Debug, Load)]
                    struct CurrentElectionData {
                        _elect_at: u32,
                        _elect_close: u32,
                        _min_stake: Tokens,
                        _total_stake: Tokens,
                        members: Dict<HashBytes, ()>,
                    }

                    type PartialElectorData = Option<Lazy<CurrentElectionData>>;

                    let res = res.parse()?;
                    let Some(account) = res.state.load_account()? else {
                        anyhow::bail!("elector account not found");
                    };

                    let data = match account.state {
                        AccountState::Active(state) => state.data,
                        _ => None,
                    }
                    .context("elector data is empty")?;

                    let Some(current_elections) = data.parse::<PartialElectorData>()? else {
                        // No current elections
                        return Ok(false);
                    };
                    let current_elections = current_elections.load()?;

                    let is_elected = current_elections.members.contains_key(public_key)?;
                    Ok::<_, anyhow::Error>(is_elected)
                };

                let res = self.clone().get_blockchain_config(ctx).await?;
                let (elector_address, in_current_vset, in_next_vset, has_next_vset) =
                    parse_config(res).map_err(|e| {
                        ServerError::new(format!("failed to parse blockchain config: {e:?}"))
                    })?;

                let mut is_elected = in_next_vset;
                if !is_elected && !has_next_vset {
                    let req = proto::AccountStateRequest {
                        address: StdAddr::new(-1, elector_address),
                    };
                    let res = self.get_account_state(ctx, req).await?;
                    is_elected = parse_elector(res).map_err(|e| {
                        ServerError::new(format!("failed to parse elector state: {e:?}"))
                    })?;
                }

                Some(proto::ValidatorStatus {
                    public_key,
                    in_current_vset,
                    in_next_vset,
                    is_elected,
                })
            }
        };

        Ok(proto::NodeStatusResponse {
            status_at,
            node_info,
            init_block_id,
            last_applied_block,
            validator_status,
        })
    }

    async fn trigger_archives_gc(self, _: Context, req: proto::TriggerGcRequest) {
        self.inner.storage.trigger_archives_gc(req.into());
    }

    async fn trigger_blocks_gc(self, _: Context, req: proto::TriggerGcRequest) {
        self.inner.storage.trigger_blocks_gc(req.into());
    }

    async fn trigger_states_gc(self, _: Context, req: proto::TriggerGcRequest) {
        self.inner.storage.trigger_states_gc(req.into());
    }

    async fn trigger_compaction(self, _: Context, req: proto::TriggerCompactionRequest) {
        self.inner.manual_compaction.trigger_compaction(req);
    }

    async fn set_memory_profiler_enabled(self, _: Context, enabled: bool) -> bool {
        self.inner.memory_profiler.set_enabled(enabled).await
    }

    async fn dump_memory_profiler(self, _: Context) -> ServerResult<Vec<u8>> {
        self.inner.memory_profiler.dump().await.map_err(Into::into)
    }

    async fn get_neighbours_info(self, _: Context) -> ServerResult<proto::NeighboursInfoResponse> {
        let neighbours = self
            .inner
            .blockchain_rpc_client
            .overlay_client()
            .neighbours()
            .get_active_neighbours()
            .iter()
            .map(|x| {
                let stats = x.get_stats();
                proto::NeighbourInfo {
                    id: HashBytes(x.peer_id().to_bytes()),
                    expires_at: x.expires_at_secs(),
                    score: stats.score,
                    failed_requests: stats.failed_requests,
                    total_requests: stats.total_requests,
                    roundtrip_ms: stats.avg_roundtrip.unwrap_or_default().as_millis() as u64,
                }
            })
            .collect::<_>();

        Ok(proto::NeighboursInfoResponse { neighbours })
    }

    async fn broadcast_external_message(
        self,
        _: Context,
        req: proto::BroadcastExtMsgRequest,
    ) -> ServerResult<()> {
        Boc::decode(&req.message)
            .ok()
            .and_then(|msg| {
                let msg = msg.parse::<Message<'_>>().ok()?;
                msg.info.is_external_in().then_some(())
            })
            .ok_or_else(|| ServerError::new("invalid external message"))?;

        self.inner
            .blockchain_rpc_client
            .broadcast_external_message(&req.message)
            .await;
        Ok(())
    }

    async fn get_account_state(
        self,
        _: Context,
        req: proto::AccountStateRequest,
    ) -> ServerResult<proto::AccountStateResponse> {
        let (block_handle, account) = 'state: {
            // Try fast path first.
            let (block_handle, tracker_handle) = {
                // NOTE: Extending lifetimes of guards here.
                let mc_guard;
                let sc_guard;
                let cached = if req.address.is_masterchain() {
                    mc_guard = self.inner.mc_accounts.read();
                    mc_guard.as_ref()
                } else {
                    sc_guard = self.inner.sc_accounts.read();
                    sc_guard.iter().find_map(|(s, cached)| {
                        s.contains_account(&req.address.address).then_some(cached)
                    })
                };

                let Some(cached) = cached else {
                    return Err(ServerError::new("shard state not found"));
                };

                let block_handle = cached.block_handle.clone();
                match cached.try_get(&req.address.address)? {
                    // No cached accounts map, so we need to load the state (go to slow path)
                    CacheItem::Unavailable => (block_handle, cached.tracker_handle.clone()),
                    // No account state by the latest known block (fast path done)
                    CacheItem::NotFound => break 'state (block_handle, None),
                    // Found an account state (fast path done)
                    CacheItem::Loaded(account) => break 'state (block_handle, Some(account)),
                }
            };

            // Fallback to slow path

            // Load the state
            let state = self
                .inner
                .storage
                .shard_state_storage()
                .load_state(block_handle.ref_by_mc_seqno(), block_handle.id())
                .await?;

            // Find the account state in it
            match state.as_ref().load_accounts()?.get(req.address.address)? {
                None => (block_handle, None),
                Some((_, account)) => (
                    block_handle,
                    Some(LoadedAccount {
                        account,
                        tracker_handle,
                    }),
                ),
            }
        };

        // TODO: Store serialized instead?
        let state = BocRepr::encode_rayon(match &account {
            None => empty_shard_account(),
            Some(account) => &account.account,
        })?
        .into();

        Ok(proto::AccountStateResponse {
            mc_seqno: block_handle.ref_by_mc_seqno(),
            gen_utime: block_handle.gen_utime(),
            state,
        })
    }

    async fn get_blockchain_config(
        self,
        _: Context,
    ) -> ServerResult<proto::BlockchainConfigResponse> {
        match self.inner.config_response.load().as_deref().cloned() {
            Some(response) => Ok(response),
            None => Err(ServerError::new("not ready")),
        }
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

        let data = blocks.load_block_data_decompressed(&handle).await?;
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

        let data = blocks.load_block_proof_raw(&handle).await?;
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

        let data = blocks.load_queue_diff_raw(&handle).await?;
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
            chunk_size: BlockStorage::DEFAULT_BLOB_CHUNK_SIZE,
        }))
    }

    async fn get_archive_chunk(
        self,
        _: Context,
        req: proto::ArchiveSliceRequest,
    ) -> ServerResult<proto::ArchiveSliceResponse> {
        let blocks = self.inner.storage.block_storage();

        let data = {
            let data = blocks.get_archive_chunk(req.archive_id, req.offset).await?;
            Bytes::copy_from_slice(data.as_ref())
        };
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
                    chunk_size: BlockStorage::DEFAULT_BLOB_CHUNK_SIZE,
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

    async fn get_overlay_ids(
        self,
        _: tarpc::context::Context,
    ) -> ServerResult<proto::OverlayIdsResponse> {
        let Some(overlay_service) = self.inner.overlay_service.as_ref() else {
            return Err(ServerError::new(
                "control server was created without a overlay service",
            ));
        };

        fn map_overlay_ids<T>(overlays: FastHashMap<OverlayId, T>) -> FastHashSet<HashBytes> {
            overlays.into_keys().map(|id| HashBytes(id.0)).collect()
        }

        Ok(proto::OverlayIdsResponse {
            public_overlays: map_overlay_ids(overlay_service.public_overlays()),
            private_overlays: map_overlay_ids(overlay_service.private_overlays()),
        })
    }

    async fn get_overlay_peers(
        self,
        _: tarpc::context::Context,
        req: proto::OverlayPeersRequest,
    ) -> ServerResult<proto::OverlayPeersResponse> {
        let service = self.inner.overlay_service.as_ref().ok_or_else(|| {
            ServerError::new("control server was created without am overlay service")
        })?;

        fn make_peer_item(
            peer_id: &PeerId,
            entry_created_at: Option<u32>,
            handle: &PeerResolverHandle,
        ) -> proto::OverlayPeer {
            proto::OverlayPeer {
                peer_id: HashBytes(peer_id.0),
                entry_created_at,
                info: handle
                    .load_handle()
                    .map(|handle| map_peer_info(&handle.peer_info())),
            }
        }

        let overlay_id = OverlayId::wrap(req.overlay_id.as_array());
        let overlay_type;
        let peers = 'peers: {
            if let Some(overlay) = service.public_overlays().get(overlay_id) {
                overlay_type = proto::OverlayType::Public;
                break 'peers overlay
                    .read_entries()
                    .iter()
                    .map(|item| {
                        make_peer_item(
                            &item.entry.peer_id,
                            Some(item.entry.created_at),
                            &item.resolver_handle,
                        )
                    })
                    .collect::<Vec<_>>();
            }

            if let Some(overlay) = service.private_overlays().get(overlay_id) {
                overlay_type = proto::OverlayType::Private;
                break 'peers overlay
                    .read_entries()
                    .iter()
                    .map(|item| make_peer_item(&item.peer_id, None, &item.resolver_handle))
                    .collect::<Vec<_>>();
            }

            return Err(ServerError::new("overlay not found"));
        };

        Ok(proto::OverlayPeersResponse {
            overlay_type,
            peers,
        })
    }

    async fn dht_find_node(
        self,
        _: tarpc::context::Context,
        req: proto::DhtFindNodeRequest,
    ) -> ServerResult<proto::DhtFindNodeResponse> {
        use tycho_network::proto::dht;

        let dht_client = self
            .inner
            .dht_client
            .as_ref()
            .ok_or_else(|| ServerError::new("control server was created without DHT client"))?;

        let nodes = if let Some(peer_id) = req.peer_id {
            // TODO: Move `FindNode` into dht client.
            let request = Request::from_tl(dht::rpc::FindNode {
                key: req.key.0,
                k: req.k,
            });

            let response = dht_client
                .network()
                .query(PeerId::wrap(peer_id.as_array()), request)
                .await?;

            let dht::NodeResponse { nodes } = response.parse_tl().map_err(|e| {
                ServerError::new(format!("failed to deserialize DHT response: {e:?}"))
            })?;

            nodes
        } else {
            dht_client
                .service()
                .find_local_closest(req.key.as_array(), req.k as usize)
        };

        Ok(proto::DhtFindNodeResponse {
            nodes: nodes
                .into_iter()
                .map(|item| proto::DhtFindNodeResponseItem {
                    peer_id: HashBytes(item.id.0),
                    info: map_peer_info(&item),
                })
                .collect(),
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

        let data = build_elections_data_to_sign(
            req.election_id,
            req.stake_factor,
            &req.address,
            &req.adnl_addr,
        );
        let data = extend_signature_with_id(&data, req.signature_id);
        let signature = keypair.sign_raw(&data);

        Ok(proto::ElectionsPayloadResponse {
            data: data.into_owned().into(),
            public_key: HashBytes(keypair.public_key.to_bytes()),
            signature: Box::new(signature),
        })
    }
}

impl StateSubscriber for ControlServer {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        let res = self.inner.handle_state_impl(cx);
        futures_util::future::ready(res)
    }
}

struct Inner {
    node_info: proto::NodeInfo,
    config_response: ArcSwapOption<proto::BlockchainConfigResponse>,
    storage: CoreStorage,
    blockchain_rpc_client: BlockchainRpcClient,
    manual_compaction: ManualCompaction,
    memory_profiler: Arc<dyn MemoryProfiler>,
    validator_keypair: Option<Arc<ed25519::KeyPair>>,
    dht_client: Option<DhtClient>,
    overlay_service: Option<OverlayService>,
    mc_accounts: RwLock<Option<CachedAccounts>>,
    sc_accounts: RwLock<FastHashMap<ShardIdent, CachedAccounts>>,
}

impl Inner {
    fn handle_state_impl(&self, cx: &StateSubscriberContext) -> Result<()> {
        let block_id = cx.block.id();
        let block_handle = self
            .storage
            .block_handle_storage()
            .load_handle(block_id)
            .context("block handle not found")?;

        // Get a weak reference to the accounts dictionary root.
        let accounts_dict_root = {
            let accounts = cx.state.as_ref().load_accounts()?;
            let (dict_root, _) = accounts.into_parts();
            dict_root.into_root().as_ref().map(Cell::downgrade)
        };

        // Store a tracker handle to delay the GC.
        let tracker_handle = cx.state.ref_mc_state_handle().clone();

        let cached = CachedAccounts {
            block_handle,
            accounts_dict_root,
            tracker_handle,
        };

        // Update the cache.
        if block_id.is_masterchain() {
            *self.mc_accounts.write() = Some(cached);

            // Update config response cache
            let config = cx.state.config_params()?;
            let config_response = Arc::new(proto::BlockchainConfigResponse {
                global_id: cx.state.as_ref().global_id,
                mc_seqno: block_id.seqno,
                gen_utime: cx.state.as_ref().gen_utime,
                config: BocRepr::encode_rayon(config)?.into(),
            });
            self.config_response.store(Some(config_response));
        } else {
            // TODO: Handle split/merge like in `tycho-rpc`.
            self.sc_accounts.write().insert(block_id.shard, cached);
        }

        Ok(())
    }
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

/// A bit more weak version of `CachedAccounts` from the `tycho-rpc`.
struct CachedAccounts {
    block_handle: BlockHandle,
    accounts_dict_root: Option<WeakCell>,
    tracker_handle: RefMcStateHandle,
}

impl CachedAccounts {
    fn try_get(&self, addr: &HashBytes) -> Result<CacheItem> {
        let Some(dict_root) = &self.accounts_dict_root else {
            return Ok(CacheItem::NotFound);
        };

        let Some(dict_root) = dict_root.upgrade() else {
            return Ok(CacheItem::Unavailable);
        };

        match ShardAccountsDict::from_raw(Some(dict_root)).get(addr)? {
            Some((_, account)) => Ok(CacheItem::Loaded(LoadedAccount {
                account,
                tracker_handle: self.tracker_handle.clone(),
            })),
            None => Ok(CacheItem::NotFound),
        }
    }
}

enum CacheItem {
    Unavailable,
    NotFound,
    Loaded(LoadedAccount),
}

struct LoadedAccount {
    account: ShardAccount,

    // NOTE: Stored to delay the GC.
    #[allow(unused)]
    tracker_handle: RefMcStateHandle,
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;

fn empty_shard_account() -> &'static ShardAccount {
    static EMPTY: OnceLock<ShardAccount> = OnceLock::new();
    EMPTY.get_or_init(|| ShardAccount {
        account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
        last_trans_hash: HashBytes::ZERO,
        last_trans_lt: 0,
    })
}

fn extend_signature_with_id(data: &[u8], signature_id: Option<i32>) -> Cow<'_, [u8]> {
    match signature_id {
        Some(signature_id) => {
            let mut result = Vec::with_capacity(4 + data.len());
            result.extend_from_slice(&signature_id.to_be_bytes());
            result.extend_from_slice(data);
            Cow::Owned(result)
        }
        None => Cow::Borrowed(data),
    }
}

fn map_peer_info(info: &tycho_network::PeerInfo) -> proto::PeerInfo {
    proto::PeerInfo {
        address_list: info.address_list.iter().map(ToString::to_string).collect(),
        created_at: info.created_at,
        expires_at: info.expires_at,
    }
}

#[derive(Clone)]
struct ManualCompaction {
    trigger: ManualTriggerTx,
    handle: AbortHandle,
}

impl ManualCompaction {
    pub fn new(storage: CoreStorage) -> Self {
        let (compaction_trigger, manual_compaction_rx) =
            watch::channel(None::<proto::TriggerCompactionRequest>);

        let watcher = tokio::spawn(Self::watcher(manual_compaction_rx, storage.clone()));

        Self {
            trigger: compaction_trigger,
            handle: watcher.abort_handle(),
        }
    }

    pub fn trigger_compaction(&self, trigger: proto::TriggerCompactionRequest) {
        self.trigger.send_replace(Some(trigger));
    }

    #[tracing::instrument(skip_all)]
    async fn watcher(mut manual_rx: ManualTriggerRx, storage: CoreStorage) {
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        let ctx = storage.context();
        loop {
            if manual_rx.changed().await.is_err() {
                break;
            }

            let Some(trigger) = manual_rx.borrow_and_update().clone() else {
                continue;
            };

            if !ctx.trigger_rocksdb_compaction(&trigger.database) {
                tracing::warn!(
                    db_name = trigger.database,
                    "tried to trigger compaction for an unknown DB instance"
                );
            }
        }
    }
}

impl Drop for ManualCompaction {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

type ManualTriggerTx = watch::Sender<Option<proto::TriggerCompactionRequest>>;
type ManualTriggerRx = watch::Receiver<Option<proto::TriggerCompactionRequest>>;
