use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use everscale_crypto::ed25519;
use everscale_types::models::*;
use everscale_types::prelude::*;
use futures_util::future::BoxFuture;
use tracing_subscriber::Layer;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::queue::{QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::persistent_state::PersistentStateImplFactory;
use tycho_collator::internal_queue::state::session_state::SessionStateImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::MempoolAdapterStdImpl;
use tycho_collator::queue_adapter::MessageQueueAdapterStdImpl;
use tycho_collator::state_node::{StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::types::{CollationConfig, ValidatorNetwork};
use tycho_collator::validator::client::retry::BackoffConfig;
use tycho_collator::validator::config::ValidatorConfig;
use tycho_collator::validator::validator::ValidatorStdImplFactory;
use tycho_core::block_strider::{
    BlockProvider, BlockStrider, BlockSubscriberExt, BlockchainBlockProvider,
    BlockchainBlockProviderConfig, GcSubscriber, MetricsSubscriber, OptionalBlockStuff,
    PersistentBlockStriderState, ShardStateApplier, StateSubscriber, StateSubscriberContext,
    StorageBlockProvider,
};
use tycho_core::blockchain_rpc::{
    BlockchainRpcClient, BlockchainRpcService, BroadcastListener, SelfBroadcastListener,
};
use tycho_core::global_config::{GlobalConfig, ZerostateId};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_network::{
    DhtClient, DhtService, InboundRequestMeta, Network, OverlayService, PeerId, PeerResolver,
    PublicOverlay, Router,
};
use tycho_rpc::{RpcConfig, RpcState};
use tycho_storage::{BlockHandle, BlockMetaData, Storage};
use tycho_util::FastHashMap;

use self::config::{MetricsConfig, NodeConfig, NodeKeys};
use crate::node::boot::cold_boot;
use crate::util::alloc::memory_profiler;
#[cfg(feature = "jemalloc")]
use crate::util::alloc::spawn_allocator_metrics_loop;
use crate::util::error::ResultExt;
use crate::util::logger::{is_systemd_child, LoggerConfig};
use crate::util::signal;

mod boot;
mod config;

const SERVICE_NAME: &str = "tycho-node";

/// Run a Tycho node.
#[derive(Parser)]
pub struct CmdRun {
    /// dump the template of the zero state config
    #[clap(
        short = 'i',
        long,
        conflicts_with_all = ["config", "global_config", "keys", "logger_config", "import_zerostate"]
    )]
    init_config: Option<PathBuf>,

    /// overwrite the existing config
    #[clap(short, long)]
    force: bool,

    /// path to the node config
    #[clap(long, required_unless_present = "init_config")]
    config: Option<PathBuf>,

    /// path to the global config
    #[clap(long, required_unless_present = "init_config")]
    global_config: Option<PathBuf>,

    /// path to the node keys
    #[clap(long, required_unless_present = "init_config")]
    keys: Option<PathBuf>,

    /// path to the logger config
    #[clap(long)]
    logger_config: Option<PathBuf>,

    /// list of zerostate files to import
    #[clap(long)]
    import_zerostate: Option<Vec<PathBuf>>,
}

impl CmdRun {
    pub fn run(self) -> Result<()> {
        if let Some(init_config_path) = self.init_config {
            return NodeConfig::default()
                .save_to_file(init_config_path)
                .wrap_err("failed to save node config");
        }

        let node_config = NodeConfig::from_file(self.config.as_ref().unwrap())
            .wrap_err("failed to load node config")?;

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
                let run_fut = tokio::spawn(self.run_impl(node_config));
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

    async fn run_impl(self, node_config: NodeConfig) -> Result<()> {
        init_logger(self.logger_config)?;

        let node = {
            if let Some(metrics_config) = &node_config.metrics {
                init_metrics(metrics_config)?;
            }

            #[cfg(feature = "jemalloc")]
            tokio::spawn(memory_profiler(node_config.profiling.profiling_dir.clone()));

            let global_config = GlobalConfig::from_file(self.global_config.unwrap())
                .wrap_err("failed to load global config")?;

            let keys =
                NodeKeys::from_file(self.keys.unwrap()).wrap_err("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            Arc::new(Node::new(socket_addr, keys, node_config, global_config)?)
        };

        // Ensure that there are some neighbours
        tracing::info!("waiting for initial neighbours");
        node.blockchain_rpc_client
            .overlay_client()
            .neighbours()
            .wait_for_peers(1)
            .await;
        tracing::info!("found initial neighbours");

        let init_block_id = node
            .try_init(self.import_zerostate)
            .await
            .wrap_err("failed to init node")?;
        tracing::info!(%init_block_id, "node initialized");

        node.run(&init_block_id).await?;

        Ok(())
    }
}

fn init_logger(logger_config: Option<PathBuf>) -> Result<()> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{fmt, reload, EnvFilter};

    let try_make_filter = {
        let logger_config = logger_config.clone();
        move || {
            Ok::<_, anyhow::Error>(match &logger_config {
                None => EnvFilter::builder()
                    .with_default_directive(tracing::Level::INFO.into())
                    .from_env_lossy(),
                Some(path) => LoggerConfig::load_from(path)
                    .wrap_err("failed to load logger config")?
                    .build_subscriber(),
            })
        }
    };

    let (layer, handle) = reload::Layer::new(try_make_filter()?);

    let subscriber = tracing_subscriber::registry()
        .with(layer)
        .with(if is_systemd_child() {
            fmt::layer().without_time().with_ansi(false).boxed()
        } else {
            fmt::layer().boxed()
        });
    tracing::subscriber::set_global_default(subscriber).unwrap();

    if let Some(logger_config) = logger_config {
        tokio::spawn(async move {
            tracing::info!(
                logger_config = %logger_config.display(),
                "started watching for changes in logger config"
            );

            let get_metadata = move || {
                std::fs::metadata(&logger_config)
                    .ok()
                    .and_then(|m| m.modified().ok())
            };

            let mut last_modified = get_metadata();

            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                let modified = get_metadata();
                if last_modified == modified {
                    continue;
                }
                last_modified = modified;

                match try_make_filter() {
                    Ok(filter) => {
                        if handle.reload(filter).is_err() {
                            break;
                        }
                        tracing::info!("reloaded logger config");
                    }
                    Err(e) => tracing::error!(%e, "failed to reload logger config"),
                }
            }

            tracing::info!("stopped watching for changes in logger config");
        });
    }

    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;

        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        panic!("PANIC: {}", info);
    }));

    Ok(())
}

fn init_metrics(config: &MetricsConfig) -> Result<()> {
    use metrics_exporter_prometheus::Matcher;
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        60.0, 120.0, 300.0, 600.0, 3600.0,
    ];

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_time".to_string()), EXPONENTIAL_SECONDS)?
        .with_http_listener(config.listen_addr)
        .install()
        .wrap_err("failed to initialize a metrics exporter")?;

    #[cfg(feature = "jemalloc")]
    spawn_allocator_metrics_loop();

    Ok(())
}

async fn resolve_public_ip(ip: Option<IpAddr>) -> Result<IpAddr> {
    match ip {
        Some(address) => Ok(address),
        None => match public_ip::addr_v4().await {
            Some(address) => Ok(IpAddr::V4(address)),
            None => anyhow::bail!("failed to resolve public IP address"),
        },
    }
}

pub struct Node {
    keypair: Arc<ed25519::KeyPair>,

    zerostate: ZerostateId,

    dht_client: DhtClient,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    storage: Storage,
    rpc_mempool_adapter: RpcMempoolAdapter,
    blockchain_rpc_client: BlockchainRpcClient,

    state_tracker: MinRefMcStateTracker,

    rpc_config: Option<RpcConfig>,
    blockchain_block_provider_config: BlockchainBlockProviderConfig,

    pub collation_config: CollationConfig,
}

impl Node {
    pub fn new(
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
            .with_service_name(SERVICE_NAME)
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
            .wrap_err("failed to create storage")?;
        tracing::info!(
            root_dir = %storage.root().path().display(),
            "initialized storage"
        );

        // Setup blockchain rpc
        let zerostate = global_config.zerostate;

        let rpc_mempool_adapter = RpcMempoolAdapter {
            inner: MempoolAdapterStdImpl::new(),
        };

        let blockchain_rpc_service = BlockchainRpcService::builder()
            .with_config(node_config.blockchain_rpc_service)
            .with_storage(storage.clone())
            .with_broadcast_listener(rpc_mempool_adapter.clone())
            .build();

        let public_overlay = PublicOverlay::builder(zerostate.compute_public_overlay_id())
            .with_peer_resolver(peer_resolver.clone())
            .build(blockchain_rpc_service);
        overlay_service.add_public_overlay(&public_overlay);

        let blockchain_rpc_client = BlockchainRpcClient::builder()
            .with_public_overlay_client(PublicOverlayClient::new(
                network,
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
            zerostate,
            dht_client,
            peer_resolver,
            overlay_service,
            storage,
            rpc_mempool_adapter,
            blockchain_rpc_client,
            state_tracker,
            rpc_config: node_config.rpc,
            blockchain_block_provider_config: node_config.blockchain_block_provider,
            collation_config: node_config.collator,
        })
    }

    /// Initialize the node and return the init block id.
    async fn try_init(self: &Arc<Self>, zerostates: Option<Vec<PathBuf>>) -> Result<BlockId> {
        let node_state = self.storage.node_state();

        let last_key_block_id = match node_state.load_last_mc_block_id() {
            Some(block_id) => {
                tracing::info!("warm init");
                block_id
            }
            None => {
                tracing::info!("cold init");

                let last_mc_block_id = cold_boot(self, zerostates).await?;

                node_state.store_init_mc_block_id(&last_mc_block_id);
                node_state.store_last_mc_block_id(&last_mc_block_id);

                last_mc_block_id
            }
        };

        Ok(last_key_block_id)
    }

    async fn run(&self, last_block_id: &BlockId) -> Result<()> {
        // Force load last applied state
        let mc_state = self
            .storage
            .shard_state_storage()
            .load_state(last_block_id)
            .await?;

        // Run mempool adapter
        let mempool_adapter = self.rpc_mempool_adapter.inner.clone();
        mempool_adapter.run(
            self.keypair.clone(),
            self.dht_client.clone(),
            self.overlay_service.clone(),
            get_validator_peer_ids(&mc_state)?,
        );

        // Create RPC
        let rpc_state = if let Some(config) = &self.rpc_config {
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

            Some(rpc_state)
        } else {
            None
        };

        // Create collator
        tracing::info!("starting collator");

        let session_state_factory = SessionStateImplFactory::new(self.storage.clone());
        let persistent_state_factory = PersistentStateImplFactory::new(self.storage.clone());

        let queue_factory = QueueFactoryStdImpl {
            session_state_factory,
            persistent_state_factory,
        };
        let queue = queue_factory.create();
        let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);

        let collation_manager = CollationManager::start(
            self.keypair.clone(),
            self.collation_config.clone(),
            Arc::new(message_queue_adapter),
            |listener| StateNodeAdapterStdImpl::new(listener, self.storage.clone()),
            mempool_adapter,
            ValidatorStdImplFactory {
                network: ValidatorNetwork {
                    overlay_service: self.overlay_service.clone(),
                    peer_resolver: self.peer_resolver.clone(),
                    dht_client: self.dht_client.clone(),
                },
                // TODO: Move into node config
                config: ValidatorConfig {
                    error_backoff_config: BackoffConfig {
                        min_delay: Duration::from_millis(50),
                        max_delay: Duration::from_secs(10),
                        factor: 2.0,
                        max_times: usize::MAX,
                    },
                    request_timeout: Duration::from_secs(1),
                    delay_between_requests: Duration::from_millis(50),
                    request_signatures_backoff_config: BackoffConfig {
                        min_delay: Duration::from_millis(50),
                        max_delay: Duration::from_secs(1),
                        factor: 2.0,
                        max_times: usize::MAX,
                    },
                },
            },
            CollatorStdImplFactory,
            #[cfg(test)]
            vec![],
        );

        let collator_state_subscriber = CollatorStateSubscriber {
            adapter: collation_manager.state_node_adapter().clone(),
        };

        // Explicitly handle the initial state
        collator_state_subscriber
            .adapter
            .handle_state(&mc_state)
            .await?;

        // NOTE: Make sure to drop the state after handling it
        drop(mc_state);

        tracing::info!("collator started");

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

        let block_strider = BlockStrider::builder()
            .with_provider((
                (blockchain_block_provider, storage_block_provider),
                collator_block_provider,
            ))
            .with_state(strider_state)
            .with_block_subscriber(
                (
                    ShardStateApplier::new(
                        self.state_tracker.clone(),
                        self.storage.clone(),
                        (collator_state_subscriber, rpc_state),
                    ),
                    MetricsSubscriber,
                )
                    .chain(GcSubscriber::new(self.storage.clone())),
            )
            .build();

        // Run block strider
        tracing::info!("block strider started");
        block_strider.run().await?;
        tracing::info!("block strider finished");

        Ok(())
    }
}

struct CollatorStateSubscriber {
    adapter: Arc<dyn StateNodeAdapter>,
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

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        self.adapter.wait_for_block(block_id)
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

fn get_validator_peer_ids(mc_state: &ShardStateStuff) -> Result<Vec<PeerId>> {
    let config = mc_state.config_params()?;
    let validator_set = config.params.get_current_validator_set()?.list;

    Ok(validator_set
        .into_iter()
        .map(|x| PeerId(x.public_key.0))
        .collect::<Vec<_>>())
}

fn load_zerostate(tracker: &MinRefMcStateTracker, path: &PathBuf) -> Result<ShardStateStuff> {
    let data = std::fs::read(path).wrap_err("failed to read file")?;
    let file_hash = Boc::file_hash(&data);

    let root = Boc::decode(data).wrap_err("failed to decode BOC")?;
    let root_hash = *root.repr_hash();

    let state = root
        .parse::<ShardStateUnsplit>()
        .wrap_err("failed to parse state")?;

    anyhow::ensure!(state.seqno == 0, "not a zerostate");

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    ShardStateStuff::from_root(&block_id, root, tracker)
}

fn make_shard_state(
    tracker: &MinRefMcStateTracker,
    global_id: i32,
    shard_ident: ShardIdent,
    now: u32,
) -> Result<ShardStateStuff> {
    let state = ShardStateUnsplit {
        global_id,
        shard_ident,
        gen_utime: now,
        min_ref_mc_seqno: u32::MAX,
        ..Default::default()
    };

    let root = CellBuilder::build_from(&state)?;
    let root_hash = *root.repr_hash();
    let file_hash = Boc::file_hash(Boc::encode(&root));

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    ShardStateStuff::from_root(&block_id, root, tracker)
}
