use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use everscale_crypto::ed25519;
use everscale_types::models::*;
use futures_util::future;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::queue::{QueueConfig, QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::storage::QueueStateImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::MempoolAdapterStdImpl;
use tycho_collator::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};
use tycho_collator::state_node::{CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::types::CollatorConfig;
use tycho_collator::validator::{
    ValidatorNetworkContext, ValidatorStdImpl, ValidatorStdImplConfig,
};
use tycho_control::{ControlEndpoint, ControlServer, ControlServerConfig, ControlServerVersion};
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, BlockProvider, BlockProviderExt,
    BlockStrider, BlockSubscriberExt, BlockchainBlockProvider, BlockchainBlockProviderConfig,
    ColdBootType, FileZerostateProvider, GcSubscriber, MetricsSubscriber, OptionalBlockStuff,
    PersistentBlockStriderState, PsSubscriber, ShardStateApplier, Starter, StarterConfig,
    StateSubscriber, StateSubscriberContext, StorageBlockProvider,
};
use tycho_core::blockchain_rpc::{
    BlockchainRpcClient, BlockchainRpcService, BroadcastListener, SelfBroadcastListener,
};
use tycho_core::global_config::{GlobalConfig, MempoolGlobalConfig, ZerostateId};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_network::{
    DhtClient, DhtService, InboundRequestMeta, Network, OverlayService, PeerResolver,
    PublicOverlay, Router,
};
use tycho_rpc::{RpcConfig, RpcState};
use tycho_storage::{NodeSyncState, Storage};
use tycho_util::futures::JoinTask;

pub use self::config::{ElectionsConfig, NodeConfig, NodeKeys, SimpleElectionsConfig};
#[cfg(feature = "jemalloc")]
use crate::util::alloc::JemallocMemoryProfiler;

mod config;

pub struct Node {
    keypair: Arc<ed25519::KeyPair>,

    zerostate: ZerostateId,

    network: Network,
    dht_client: DhtClient,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    storage: Storage,
    rpc_mempool_adapter: RpcMempoolAdapter,
    blockchain_rpc_client: BlockchainRpcClient,

    starter_config: StarterConfig,
    rpc_config: Option<RpcConfig>,
    control_config: ControlServerConfig,
    control_socket: PathBuf,
    blockchain_block_provider_config: BlockchainBlockProviderConfig,
    archive_block_provider_config: ArchiveBlockProviderConfig,

    collator_config: CollatorConfig,
    validator_config: ValidatorStdImplConfig,
    internal_queue_config: QueueConfig,
    mempool_config_override: Option<MempoolGlobalConfig>,
}

impl Node {
    pub async fn new(
        public_addr: SocketAddr,
        keys: NodeKeys,
        node_config: NodeConfig,
        global_config: GlobalConfig,
        control_socket: PathBuf,
    ) -> Result<Self> {
        // Setup network
        let keypair = Arc::new(ed25519::KeyPair::from(&keys.as_secret()));
        let local_id = keypair.public_key.into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id)
            .with_config(node_config.dht)
            .build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_config(node_config.overlay)
            .with_dht_service(dht_service.clone())
            .build();

        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();

        let local_addr = SocketAddr::from((node_config.local_ip, node_config.port));

        let network = Network::builder()
            .with_config(node_config.network)
            .with_private_key(keys.secret.0)
            .with_remote_addr(public_addr)
            .build(local_addr, router)
            .context("failed to build node network")?;

        dht_tasks.spawn(&network);
        overlay_tasks.spawn(&network);

        let dht_client = dht_service.make_client(&network);
        let peer_resolver = dht_service
            .make_peer_resolver()
            .with_config(node_config.peer_resolver)
            .build(&network);

        let mut bootstrap_peers = 0usize;
        for peer in global_config.bootstrap_peers {
            let is_new = dht_client.add_peer(Arc::new(peer))?;
            bootstrap_peers += is_new as usize;
        }

        tracing::info!(
            %local_id,
            %local_addr,
            %public_addr,
            bootstrap_peers,
            "initialized network"
        );

        // Setup storage
        let storage = Storage::builder()
            .with_config(node_config.storage)
            .with_rpc_storage(
                node_config
                    .rpc
                    .as_ref()
                    .is_some_and(|x| x.storage.is_full()),
            )
            .build()
            .await
            .context("failed to create storage")?;
        tracing::info!(
            root_dir = %storage.root().path().display(),
            "initialized storage"
        );

        // Setup blockchain rpc
        let zerostate = global_config.zerostate;

        let rpc_mempool_adapter = RpcMempoolAdapter {
            inner: Arc::new(MempoolAdapterStdImpl::new(
                keypair.clone(),
                &network,
                &peer_resolver,
                &overlay_service,
                storage.mempool_storage(),
                &node_config.mempool,
            )),
        };

        let blockchain_rpc_service = BlockchainRpcService::builder()
            .with_config(node_config.blockchain_rpc_service)
            .with_storage(storage.clone())
            .with_broadcast_listener(rpc_mempool_adapter.clone())
            .build();

        let public_overlay = PublicOverlay::builder(zerostate.compute_public_overlay_id())
            .with_peer_resolver(peer_resolver.clone())
            .named("blockchain_rpc")
            .build(blockchain_rpc_service);
        overlay_service.add_public_overlay(&public_overlay);

        let blockchain_rpc_client = BlockchainRpcClient::builder()
            .with_config(node_config.blockchain_rpc_client)
            .with_public_overlay_client(PublicOverlayClient::new(
                network.clone(),
                public_overlay,
                node_config.public_overlay_client,
            ))
            .with_self_broadcast_listener(rpc_mempool_adapter.clone())
            .build();

        tracing::info!(
            overlay_id = %blockchain_rpc_client.overlay().overlay_id(),
            "initialized blockchain rpc"
        );

        Ok(Self {
            keypair,
            network,
            zerostate,
            dht_client,
            peer_resolver,
            overlay_service,
            storage,
            rpc_mempool_adapter,
            blockchain_rpc_client,
            starter_config: node_config.starter,
            rpc_config: node_config.rpc,
            control_config: node_config.control,
            control_socket,
            blockchain_block_provider_config: node_config.blockchain_block_provider,
            archive_block_provider_config: node_config.archive_block_provider,
            collator_config: node_config.collator,
            validator_config: node_config.validator,
            internal_queue_config: node_config.internal_queue,
            mempool_config_override: global_config.mempool,
        })
    }

    pub async fn wait_for_neighbours(&self) {
        // Ensure that there are some neighbours
        tracing::info!("waiting for initial neighbours");
        self.blockchain_rpc_client
            .overlay_client()
            .neighbours()
            .wait_for_peers(1)
            .await;
        tracing::info!("found initial neighbours");
    }

    /// Initialize the node and return the init block id.
    pub async fn boot(&self, zerostates: Option<Vec<PathBuf>>) -> Result<BlockId> {
        let node_state = self.storage.node_state();
        let last_mc_block_id = match node_state.load_last_mc_block_id() {
            Some(block_id) => block_id,
            None => {
                Starter::new(
                    self.storage.clone(),
                    self.blockchain_rpc_client.clone(),
                    self.zerostate,
                    self.starter_config.clone(),
                )
                .cold_boot(
                    ColdBootType::LatestPersistent,
                    zerostates.map(FileZerostateProvider),
                )
                .await?
            }
        };

        tracing::info!(
            %last_mc_block_id,
            "boot finished"
        );

        Ok(last_mc_block_id)
    }

    pub async fn run(self, last_block_id: &BlockId) -> Result<()> {
        // Force load last applied state
        let mc_state = self
            .storage
            .shard_state_storage()
            .load_state(last_block_id)
            .await?;

        let validator_subscriber = self
            .blockchain_rpc_client
            .overlay_client()
            .validators_resolver()
            .clone();

        {
            let config = mc_state.config_params()?;
            let current_validator_set = config.get_current_validator_set()?;
            validator_subscriber.update_validator_set(&current_validator_set);
        }

        // Create mempool adapter
        let mempool_adapter = self.rpc_mempool_adapter.inner.clone();
        if let Some(global) = self.mempool_config_override.as_ref() {
            let future = mempool_adapter.set_config(|config| {
                if let Some(consensus_config) = &global.consensus_config {
                    config.set_consensus_config(consensus_config)?;
                } // else: will be set from mc state after sync
                config.set_genesis(global.genesis_info);
                Ok::<_, anyhow::Error>(())
            });
            future.await?;
        };

        // Create RPC
        let (rpc_block_subscriber, rpc_state_subscriber) = if let Some(config) = &self.rpc_config {
            let rpc_state = RpcState::builder()
                .with_config(config.clone())
                .with_storage(self.storage.clone())
                .with_blockchain_rpc_client(self.blockchain_rpc_client.clone())
                .build();

            rpc_state.init(last_block_id).await?;

            let endpoint = rpc_state
                .bind_endpoint()
                .await
                .context("failed to setup RPC server endpoint")?;

            tracing::info!(listen_addr = %config.listen_addr, "RPC server started");
            tokio::task::spawn(async move {
                if let Err(e) = endpoint.serve().await {
                    tracing::error!("RPC server failed: {e:?}");
                }
                tracing::info!("RPC server stopped");
            });

            Some(rpc_state.split())
        } else {
            None
        }
        .unzip();

        // Create collator
        tracing::info!("starting collator");

        let queue_state_factory = QueueStateImplFactory::new(self.storage.clone());

        let queue_factory = QueueFactoryStdImpl {
            state: queue_state_factory,
            config: self.internal_queue_config,
        };
        let queue = queue_factory.create();
        let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);

        // We should clear uncommitted queue state because it may contain incorrect diffs
        // that were created before node restart. We will restore queue strictly above last committed state
        message_queue_adapter.clear_uncommitted_state()?;

        let validator = ValidatorStdImpl::new(
            ValidatorNetworkContext {
                network: self.dht_client.network().clone(),
                peer_resolver: self.peer_resolver.clone(),
                overlays: self.overlay_service.clone(),
                zerostate_id: self.zerostate.as_block_id(),
            },
            self.keypair.clone(),
            self.validator_config,
        );

        // Explicitly handle the initial state
        let sync_context = match self.storage.node_state().get_node_sync_state() {
            None => anyhow::bail!("Failed to determine node sync state"),
            Some(NodeSyncState::PersistentState) => CollatorSyncContext::Persistent,
            Some(NodeSyncState::Blocks) => CollatorSyncContext::Historical,
        };

        let collation_manager = CollationManager::start(
            self.keypair.clone(),
            self.collator_config.clone(),
            Arc::new(message_queue_adapter),
            |listener| StateNodeAdapterStdImpl::new(listener, self.storage.clone(), sync_context),
            mempool_adapter,
            validator.clone(),
            CollatorStdImplFactory,
            self.mempool_config_override.clone(),
        );
        let collator = CollatorStateSubscriber {
            adapter: collation_manager.state_node_adapter().clone(),
        };
        collator.adapter.handle_state(&mc_state).await?;

        // NOTE: Make sure to drop the state after handling it
        drop(mc_state);

        tracing::info!("collator started");

        let gc_subscriber = GcSubscriber::new(self.storage.clone());
        let ps_subscriber = PsSubscriber::new(self.storage.clone());

        // Create control server
        let control_server = {
            let mut builder = ControlServer::builder()
                .with_network(&self.network)
                .with_gc_subscriber(gc_subscriber.clone())
                .with_storage(self.storage.clone())
                .with_blockchain_rpc_client(self.blockchain_rpc_client.clone())
                .with_validator_keypair(self.keypair.clone())
                .with_collator(Arc::new(CollatorControl {
                    config: self.collator_config.clone(),
                }));

            #[cfg(feature = "jemalloc")]
            if let Some(profiler) = JemallocMemoryProfiler::connect() {
                builder = builder.with_memory_profiler(Arc::new(profiler));
            }

            builder
                .build(ControlServerVersion {
                    version: crate::TYCHO_VERSION.to_owned(),
                    build: crate::TYCHO_BUILD.to_owned(),
                })
                .await?
        };

        // Spawn control server endpoint
        // NOTE: This variable is used as a guard to abort the server future on drop.
        let _control_endpoint = {
            let endpoint = ControlEndpoint::bind(
                &self.control_config,
                control_server.clone(),
                self.control_socket,
            )
            .await
            .context("failed to setup control server endpoint")?;

            tracing::info!(socket_path = %endpoint.socket_path().display(), "control server started");

            JoinTask::new(async move {
                scopeguard::defer! {
                    tracing::info!("control server stopped");
                }

                endpoint.serve().await;
            })
        };

        // Create block strider
        let archive_block_provider = ArchiveBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.archive_block_provider_config.clone(),
        );

        let blockchain_block_provider = BlockchainBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.blockchain_block_provider_config,
        );

        // TODO: Uncomment when archive block provider can initiate downloads for shard blocks.
        // blockchain_block_provider =
        //     blockchain_block_provider.with_fallback(archive_block_provider.clone());

        let storage_block_provider = StorageBlockProvider::new(self.storage.clone());

        let collator_block_provider = CollatorBlockProvider {
            adapter: collation_manager.state_node_adapter().clone(),
        };

        let strider_state =
            PersistentBlockStriderState::new(self.zerostate.as_block_id(), self.storage.clone());

        let block_strider = BlockStrider::builder()
            .with_provider(
                collator
                    .new_sync_point(CollatorSyncContext::Historical)
                    .chain(archive_block_provider)
                    .chain(collator.new_sync_point(CollatorSyncContext::Recent))
                    .chain((
                        blockchain_block_provider,
                        storage_block_provider,
                        collator_block_provider,
                    )),
            )
            .with_state(strider_state)
            .with_block_subscriber(
                (
                    ShardStateApplier::new(
                        self.storage.clone(),
                        (
                            collator,
                            rpc_state_subscriber,
                            ps_subscriber,
                            control_server,
                        ),
                    ),
                    rpc_block_subscriber,
                    validator_subscriber,
                    MetricsSubscriber,
                )
                    .chain(gc_subscriber),
            )
            .build();

        // Run block strider
        tracing::info!("block strider started");
        block_strider.run().await?;
        tracing::info!("block strider finished");

        Ok(())
    }
}

struct SetSyncContext {
    adapter: Arc<dyn StateNodeAdapter>,
    ctx: CollatorSyncContext,
}

impl BlockProvider for SetSyncContext {
    type GetNextBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
    type GetBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
    type CleanupFut<'a> = futures_util::future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, _: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.adapter.set_sync_context(self.ctx);
        futures_util::future::ready(None)
    }

    fn get_block<'a>(&'a self, _: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        futures_util::future::ready(None)
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

struct CollatorStateSubscriber {
    adapter: Arc<dyn StateNodeAdapter>,
}

impl CollatorStateSubscriber {
    fn new_sync_point(&self, ctx: CollatorSyncContext) -> SetSyncContext {
        SetSyncContext {
            adapter: self.adapter.clone(),
            ctx,
        }
    }
}

impl StateSubscriber for CollatorStateSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.adapter.handle_state(&cx.state)
    }
}

struct CollatorBlockProvider {
    adapter: Arc<dyn StateNodeAdapter>,
}

impl BlockProvider for CollatorBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.adapter.wait_for_block_next(prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        self.adapter.wait_for_block(&block_id_relation.block_id)
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

struct CollatorControl {
    config: CollatorConfig,
}

#[async_trait::async_trait]
impl tycho_control::Collator for CollatorControl {
    async fn get_global_version(&self) -> GlobalVersion {
        GlobalVersion {
            version: self.config.supported_block_version,
            capabilities: self.config.supported_capabilities,
        }
    }
}

#[derive(Clone)]
struct RpcMempoolAdapter {
    inner: Arc<MempoolAdapterStdImpl>,
}

impl BroadcastListener for RpcMempoolAdapter {
    type HandleMessageFut<'a> = futures_util::future::Ready<()>;

    fn handle_message(
        &self,
        _: Arc<InboundRequestMeta>,
        message: Bytes,
    ) -> Self::HandleMessageFut<'_> {
        self.inner.send_external(message);
        futures_util::future::ready(())
    }
}

#[async_trait::async_trait]
impl SelfBroadcastListener for RpcMempoolAdapter {
    async fn handle_message(&self, message: Bytes) {
        self.inner.send_external(message);
    }
}
