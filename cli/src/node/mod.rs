use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_types::models::*;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::queue::{QueueConfig, QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::persistent_state::PersistentStateImplFactory;
use tycho_collator::internal_queue::state::session_state::SessionStateImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::MempoolAdapterStdImpl;
use tycho_collator::queue_adapter::MessageQueueAdapterStdImpl;
use tycho_collator::state_node::{CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::types::CollatorConfig;
use tycho_collator::validator::{
    ValidatorNetworkContext, ValidatorStdImpl, ValidatorStdImplConfig,
};
use tycho_control::{ControlEndpoint, ControlServer, ControlServerConfig};
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, BlockProvider, BlockProviderExt,
    BlockStrider, BlockSubscriberExt, BlockchainBlockProvider, BlockchainBlockProviderConfig,
    FileZerostateProvider, GcSubscriber, MetricsSubscriber, OptionalBlockStuff,
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
use tycho_util::cli::error::ResultExt;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::{resolve_public_ip, signal};
use tycho_util::futures::JoinTask;

use self::config::{NodeConfig, NodeKeys};
pub use self::control::CmdControl;
use crate::node::config::MetricsConfig;
use crate::util::alloc::spawn_allocator_metrics_loop;
#[cfg(feature = "jemalloc")]
use crate::util::alloc::JemallocMemoryProfiler;
use crate::BaseArgs;

pub mod config;
mod control;

const SERVICE_NAME: &str = "tycho-node";

/// Manage node.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        match self.cmd {
            SubCmd::Run(cmd) => cmd.run(args),
            SubCmd::InitConfig(cmd) => cmd.run(),
            SubCmd::Control(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Run(CmdRun),
    InitConfig(CmdInitConfig),
    #[clap(flatten)]
    Control(CmdControl),
}

/// Generate a default node config.
#[derive(Parser)]
struct CmdInitConfig {
    /// path to the output file
    output: PathBuf,

    /// overwrite the existing config
    #[clap(short, long)]
    force: bool,
}

impl CmdInitConfig {
    fn run(self) -> Result<()> {
        if self.output.exists() && !self.force {
            anyhow::bail!("config file already exists, use --force to overwrite");
        }

        NodeConfig::default()
            .save_to_file(self.output)
            .wrap_err("failed to save node config")
    }
}

/// Run a Tycho node.
#[derive(Parser)]
struct CmdRun {
    /// Path to the node config. Default: `$TYCHO_HOME/config.json`
    #[clap(long)]
    config: Option<PathBuf>,

    /// Path to the global config. Default: `$TYCHO_HOME/global-config.json`
    #[clap(long)]
    global_config: Option<PathBuf>,

    /// Path to the node keys. Default: `$TYCHO_HOME/keys.json`
    #[clap(long)]
    keys: Option<PathBuf>,

    /// Path to the logger config.
    #[clap(long)]
    logger_config: Option<PathBuf>,

    /// List of zerostate files to import.
    #[clap(long)]
    import_zerostate: Option<Vec<PathBuf>>,

    /// Last know applied master block seqno to recover from
    #[allow(clippy::option_option)]
    #[clap(long)]
    pub from_mc_block_seqno: Option<Option<u32>>,
}

impl CmdRun {
    fn run(self, args: BaseArgs) -> Result<()> {
        let config_path = self
            .config
            .clone()
            .unwrap_or_else(|| args.home.join("config.json"));
        let node_config =
            NodeConfig::from_file(config_path).wrap_err("failed to load node config")?;

        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .thread_name(|_| "rayon_worker".to_string())
            .num_threads(node_config.threads.rayon_threads)
            .build_global()
            .unwrap();

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(node_config.threads.tokio_workers)
            .build()?
            .block_on(async move {
                let run_fut = tokio::spawn(self.run_impl(args, node_config));
                let stop_fut = signal::any_signal(signal::TERMINATION_SIGNALS);
                tokio::select! {
                    res = run_fut => res.unwrap(),
                    signal = stop_fut => match signal {
                        Ok(signal) => {
                            tracing::info!(?signal, "received termination signal");
                            Ok(())
                        }
                        Err(e) => Err(e.into()),
                    }
                }
            })
    }

    async fn run_impl(self, args: BaseArgs, node_config: NodeConfig) -> Result<()> {
        init_logger(&node_config.logger, self.logger_config)?;
        set_abort_with_tracing();

        if let Some(metrics_config) = &node_config.metrics {
            init_metrics(metrics_config)?;
        }

        let node = {
            let global_config_path = self
                .global_config
                .unwrap_or_else(|| args.home.join("global-config.json"));
            let global_config = GlobalConfig::from_file(global_config_path)
                .wrap_err("failed to load global config")?;

            let keys_path = self.keys.unwrap_or_else(|| args.home.join("keys.json"));
            let keys = NodeKeys::from_file(keys_path).wrap_err("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            Node::new(socket_addr, keys, node_config, global_config).await?
        };

        node.wait_for_neighbours().await;

        let init_block_id = node
            .boot(self.import_zerostate)
            .await
            .wrap_err("failed to init node")?;

        tracing::info!(%init_block_id, "node initialized");

        let from_mc_block_seqno = self.from_mc_block_seqno.unwrap_or_default();

        node.run(&init_block_id, from_mc_block_seqno).await?;

        Ok(())
    }
}

fn init_metrics(config: &MetricsConfig) -> Result<()> {
    use metrics_exporter_prometheus::Matcher;
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.75, 1.0, 2.5, 5.0,
        7.5, 10.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0,
    ];

    const EXPONENTIAL_LONG_SECONDS: &[f64] = &[
        0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
        14400.0, 28800.0, 43200.0, 86400.0,
    ];

    const EXPONENTIAL_THREADS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_time".to_string()), EXPONENTIAL_SECONDS)?
        .set_buckets_for_metric(Matcher::Suffix("_threads".to_string()), EXPONENTIAL_THREADS)?
        .set_buckets_for_metric(
            Matcher::Suffix("_time_long".to_string()),
            EXPONENTIAL_LONG_SECONDS,
        )?
        .with_http_listener(config.listen_addr)
        .install()
        .wrap_err("failed to initialize a metrics exporter")?;

    #[cfg(feature = "jemalloc")]
    spawn_allocator_metrics_loop();

    Ok(())
}

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

    state_tracker: MinRefMcStateTracker,

    starter_config: StarterConfig,
    rpc_config: Option<RpcConfig>,
    control_config: Option<ControlServerConfig>,
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
            .wrap_err("failed to build node network")?;

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
            .with_rpc_storage(node_config.rpc.is_some())
            .build()
            .await
            .wrap_err("failed to create storage")?;
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

        // Setup block strider
        let state_tracker = MinRefMcStateTracker::default();

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
            state_tracker,
            starter_config: node_config.starter,
            rpc_config: node_config.rpc,
            control_config: node_config.control,
            blockchain_block_provider_config: node_config.blockchain_block_provider,
            archive_block_provider_config: node_config.archive_block_provider,
            collator_config: node_config.collator,
            validator_config: node_config.validator,
            internal_queue_config: node_config.internal_queue,
            mempool_config_override: global_config.mempool,
        })
    }

    async fn wait_for_neighbours(&self) {
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
    async fn boot(&self, zerostates: Option<Vec<PathBuf>>) -> Result<BlockId> {
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
                .cold_boot(zerostates.map(FileZerostateProvider))
                .await?
            }
        };

        tracing::info!(
            %last_mc_block_id,
            "boot finished"
        );

        Ok(last_mc_block_id)
    }

    async fn run(self, last_block_id: &BlockId, last_mc_block_seqno: Option<u32>) -> Result<()> {
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
            mempool_adapter.override_config(|config| {
                config.set_consensus_config(&global.consensus_config);
                config.set_genesis(global.start_round, global.genesis_time_millis);
            });
        }

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
                .wrap_err("failed to setup RPC server endpoint")?;

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

        let session_state_factory = SessionStateImplFactory::new(self.storage.clone());
        let persistent_state_factory = PersistentStateImplFactory::new(self.storage.clone());

        let queue_factory = QueueFactoryStdImpl {
            session_state_factory,
            persistent_state_factory,
            config: self.internal_queue_config,
        };
        let queue = queue_factory.create();
        let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);

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
            last_mc_block_seqno,
            #[cfg(test)]
            vec![],
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

        // Create RPC
        // NOTE: This variable is used as a guard to abort the server future on drop.
        let _control_state = if let Some(config) = &self.control_config {
            let server = {
                let mut builder = ControlServer::builder()
                    .with_network(&self.network)
                    .with_gc_subscriber(gc_subscriber.clone())
                    .with_storage(self.storage.clone())
                    .with_validator_keypair(self.keypair.clone());

                #[cfg(feature = "jemalloc")]
                if let Some(profiler) = JemallocMemoryProfiler::connect() {
                    builder = builder.with_memory_profiler(Arc::new(profiler));
                }

                builder.build()
            };

            let endpoint = ControlEndpoint::bind(config, server)
                .await
                .wrap_err("failed to setup control server endpoint")?;

            tracing::info!(socket_path = %config.socket_path.display(), "control server started");
            Some(JoinTask::new(async move {
                scopeguard::defer! {
                    tracing::info!("control server stopped");
                }

                endpoint.serve().await;
            }))
        } else {
            None
        };

        // Create block strider
        let blockchain_block_provider = BlockchainBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.blockchain_block_provider_config.clone(),
        );

        let storage_block_provider = StorageBlockProvider::new(self.storage.clone());

        let collator_block_provider = CollatorBlockProvider {
            adapter: collation_manager.state_node_adapter().clone(),
        };

        let strider_state =
            PersistentBlockStriderState::new(self.zerostate.as_block_id(), self.storage.clone());

        let archive_block_provider = ArchiveBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.archive_block_provider_config.clone(),
        );

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
                        self.state_tracker.clone(),
                        self.storage.clone(),
                        (collator, rpc_state_subscriber, ps_subscriber),
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

    fn get_next_block<'a>(&'a self, _: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.adapter.set_sync_context(self.ctx);
        futures_util::future::ready(None)
    }

    fn get_block<'a>(&'a self, _: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        futures_util::future::ready(None)
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

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.adapter.wait_for_block_next(prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        self.adapter.wait_for_block(&block_id_relation.block_id)
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
