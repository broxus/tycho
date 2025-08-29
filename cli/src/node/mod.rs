use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::future;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::queue::{QueueConfig, QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::storage::QueueStateImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::{
    MempoolAdapter, MempoolAdapterSingleNodeImpl, MempoolAdapterStdImpl,
};
use tycho_collator::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};
use tycho_collator::state_node::{CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::types::CollatorConfig;
use tycho_collator::validator::{
    ValidatorNetworkContext, ValidatorStdImpl, ValidatorStdImplConfig,
};
use tycho_control::{ControlEndpoint, ControlServer, ControlServerConfig, ControlServerVersion};
use tycho_core::block_strider::{
    BlockProvider, BlockProviderExt, BlockSubscriberExt, ColdBootType, GcSubscriber,
    MetricsSubscriber, OptionalBlockStuff, PsSubscriber, ShardStateApplier, StateSubscriber,
    StateSubscriberContext,
};
use tycho_core::blockchain_rpc::{BroadcastListener, SelfBroadcastListener};
use tycho_core::global_config::{GlobalConfig, MempoolGlobalConfig};
use tycho_core::node::{NodeBase, NodeKeys};
use tycho_core::storage::NodeSyncState;
use tycho_network::InboundRequestMeta;
use tycho_rpc::{RpcConfig, RpcState};
use tycho_types::models::*;
use tycho_util::futures::JoinTask;
use tycho_wu_tuner::service::WuTunerServiceBuilder;

pub use self::config::{ElectionsConfig, NodeConfig, SimpleElectionsConfig};
#[cfg(feature = "jemalloc")]
use crate::util::alloc::JemallocMemoryProfiler;

mod config;

pub struct Node {
    base: NodeBase,
    overwrite_cold_boot_type: Option<ColdBootType>,

    queue_state_factory: QueueStateImplFactory,
    rpc_mempool_adapter: RpcMempoolAdapter,
    rpc_config: Option<RpcConfig>,
    control_config: ControlServerConfig,
    control_socket: PathBuf,

    collator_config: CollatorConfig,
    validator_config: ValidatorStdImplConfig,
    internal_queue_config: QueueConfig,
    mempool_config_override: Option<MempoolGlobalConfig>,

    /// Path to the work units tuner config.
    wu_tuner_config_path: PathBuf,

    is_single_node: bool,
}

impl Node {
    // TODO: Move args into a new `NodeParams` structure.
    pub async fn new(
        public_addr: SocketAddr,
        keys: NodeKeys,
        node_config: NodeConfig,
        global_config: GlobalConfig,
        control_socket: PathBuf,
        wu_tuner_config_path: PathBuf,
    ) -> Result<Self> {
        let is_single_node = node_config.single_node;

        let base = NodeBase::builder(&node_config.base, &global_config)
            .init_network(public_addr, &keys.as_secret())?
            .init_storage()
            .await?;

        let rpc_mempool_adapter = if node_config.single_node {
            let local_peer_id = base.network().peer_id();

            RpcMempoolAdapter {
                inner: Arc::new(MempoolAdapterSingleNodeImpl::new(
                    &node_config.mempool,
                    local_peer_id,
                )?),
            }
        } else {
            RpcMempoolAdapter {
                inner: Arc::new(MempoolAdapterStdImpl::new(
                    base.keypair().clone(),
                    base.network(),
                    base.peer_resolver(),
                    base.overlay_service(),
                    base.storage_context(),
                    &node_config.mempool,
                )?),
            }
        };

        let base = base
            .init_blockchain_rpc(rpc_mempool_adapter.clone(), rpc_mempool_adapter.clone())?
            .build()?;

        // Setup queue storage
        let queue_state_factory = QueueStateImplFactory::new(base.storage_context.clone())?;

        Ok(Self {
            base,
            overwrite_cold_boot_type: None,
            queue_state_factory,
            rpc_mempool_adapter,
            rpc_config: node_config.rpc,
            control_config: node_config.control,
            control_socket,
            collator_config: node_config.collator,
            validator_config: node_config.validator,
            internal_queue_config: node_config.internal_queue,
            mempool_config_override: global_config.mempool,
            wu_tuner_config_path,
            is_single_node,
        })
    }

    pub fn overwrite_cold_boot_type(&mut self, cold_boot_type: ColdBootType) {
        self.overwrite_cold_boot_type = Some(cold_boot_type);
    }

    pub async fn wait_for_neighbours(&self) {
        // Ensure that there are some neighbours
        self.base.wait_for_neighbours(1).await;
    }

    /// Initialize the node and return the init block id.
    pub async fn boot(&self, zerostates: Option<Vec<PathBuf>>) -> Result<BlockId> {
        let boot_type = if self.is_single_node {
            ColdBootType::Genesis
        } else {
            self.overwrite_cold_boot_type
                .unwrap_or(ColdBootType::LatestPersistent)
        };

        self.base
            .boot(
                boot_type,
                zerostates,
                Some(Box::new(QueueStateHandler {
                    storage: self.queue_state_factory.storage.clone(),
                })),
            )
            .await
    }

    pub async fn run(self, last_block_id: &BlockId) -> Result<()> {
        let base = &self.base;

        // Force load last applied state
        let mc_state = base
            .core_storage
            .shard_state_storage()
            .load_state(last_block_id)
            .await?;

        {
            let config = mc_state.config_params()?;
            let current_validator_set = config.get_current_validator_set()?;
            base.validator_resolver()
                .update_validator_set(&current_validator_set);
        }

        // Create mempool adapter
        let mempool_adapter = self.rpc_mempool_adapter.inner.clone();
        if let Some(global) = self.mempool_config_override.as_ref() {
            mempool_adapter
                .update_delayed_config(global.consensus_config.as_ref(), &global.genesis_info)
                .await?;
        }

        // Create RPC
        let (rpc_block_subscriber, rpc_state_subscriber) = if let Some(config) = &self.rpc_config {
            RpcState::init_simple(last_block_id, base, config)
                .await
                .map(Some)?
        } else {
            None
        }
        .unzip();

        // start work units tuner
        let wu_tuner = WuTunerServiceBuilder::with_config_path(self.wu_tuner_config_path.clone())
            .with_updater(crate::util::rpc_wu_updater::update_wu_params)
            .build()
            .start();

        // Create collator
        tracing::info!("starting collator");

        let queue_factory = QueueFactoryStdImpl {
            state: self.queue_state_factory,
            config: self.internal_queue_config,
        };
        let queue = queue_factory.create()?;
        let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);

        // We should clear uncommitted queue state because it may contain incorrect diffs
        // that were created before node restart. We will restore queue strictly above last committed state
        let top_shards = mc_state.get_top_shards()?;
        message_queue_adapter.clear_uncommitted_state(&top_shards)?;

        let validator = ValidatorStdImpl::new(
            ValidatorNetworkContext {
                network: base.network.clone(),
                peer_resolver: base.peer_resolver.clone(),
                overlays: base.overlay_service.clone(),
                zerostate_id: base.global_config.zerostate.as_block_id(),
            },
            base.keypair.clone(),
            self.validator_config,
        );

        // Explicitly handle the initial state
        let sync_context = match base.core_storage.node_state().get_node_sync_state() {
            None => anyhow::bail!("Failed to determine node sync state"),
            Some(NodeSyncState::PersistentState) => CollatorSyncContext::Persistent,
            Some(NodeSyncState::Blocks) => CollatorSyncContext::Historical,
        };

        let collation_manager = CollationManager::start(
            base.keypair.clone(),
            self.collator_config.clone(),
            Arc::new(message_queue_adapter),
            |listener| {
                StateNodeAdapterStdImpl::new(listener, base.core_storage.clone(), sync_context)
            },
            mempool_adapter,
            validator.clone(),
            CollatorStdImplFactory {
                wu_tuner_event_sender: Some(wu_tuner.event_sender.clone()),
            },
            self.mempool_config_override.clone(),
        );
        let collator = CollatorStateSubscriber {
            adapter: collation_manager.state_node_adapter().clone(),
        };
        collator.adapter.handle_state(&mc_state).await?;

        // NOTE: Make sure to drop the state after handling it
        drop(mc_state);

        tracing::info!("collator started");

        let gc_subscriber = GcSubscriber::new(base.core_storage.clone());
        let ps_subscriber = PsSubscriber::new(base.core_storage.clone());

        // Create control server
        let control_server = {
            let mut builder = ControlServer::builder()
                .with_network(&base.network)
                .with_gc_subscriber(gc_subscriber.clone())
                .with_storage(base.core_storage.clone())
                .with_blockchain_rpc_client(base.blockchain_rpc_client.clone())
                .with_validator_keypair(base.keypair.clone())
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

        // TODO: Uncomment when archive block provider can initiate downloads for shard blocks.
        // blockchain_block_provider =
        //     blockchain_block_provider.with_fallback(archive_block_provider.clone());

        let archive_block_provider = base.build_archive_block_provider();
        let blockchain_block_provider = base.build_blockchain_block_provider();
        let storage_block_provider = base.build_storage_block_provider();
        let collator_block_provider = CollatorBlockProvider {
            adapter: collation_manager.state_node_adapter().clone(),
        };

        let block_strider = base.build_strider(
            collator
                .new_sync_point(CollatorSyncContext::Historical)
                .chain(archive_block_provider)
                .chain(collator.new_sync_point(CollatorSyncContext::Recent))
                .chain((
                    blockchain_block_provider,
                    storage_block_provider,
                    collator_block_provider,
                )),
            (
                ShardStateApplier::new(
                    base.core_storage.clone(),
                    (
                        collator,
                        rpc_state_subscriber,
                        ps_subscriber,
                        control_server,
                    ),
                ),
                rpc_block_subscriber,
                base.validator_resolver().clone(),
                MetricsSubscriber,
            )
                .chain(gc_subscriber),
        );

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
    inner: Arc<dyn MempoolAdapter>,
}

impl BroadcastListener for RpcMempoolAdapter {
    type HandleMessageFut<'a> = futures_util::future::Ready<()>;

    fn handle_message(
        &self,
        _: Arc<InboundRequestMeta>,
        message: Bytes,
    ) -> Self::HandleMessageFut<'_> {
        self.inner.accept_external(message);
        futures_util::future::ready(())
    }
}

#[async_trait::async_trait]
impl SelfBroadcastListener for RpcMempoolAdapter {
    async fn handle_message(&self, message: Bytes) {
        self.inner.accept_external(message);
    }
}

struct QueueStateHandler {
    storage: tycho_collator::storage::InternalQueueStorage,
}

#[async_trait::async_trait]
impl tycho_core::block_strider::QueueStateHandler for QueueStateHandler {
    async fn import_from_file(
        &self,
        top_update: &OutMsgQueueUpdates,
        file: std::fs::File,
        block_id: &BlockId,
    ) -> Result<()> {
        self.storage
            .import_from_file(top_update, file, *block_id)
            .await
    }
}
