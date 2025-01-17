use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args;
use everscale_crypto::ed25519;
use everscale_types::models::*;
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, BlockProviderExt, BlockStrider,
    BlockSubscriber, BlockSubscriberExt, BlockchainBlockProvider, BlockchainBlockProviderConfig,
    FileZerostateProvider, GcSubscriber, MetricsSubscriber, PersistentBlockStriderState, Starter,
    StarterConfig, StorageBlockProvider,
};
use tycho_core::blockchain_rpc::{
    BlockchainRpcClient, BlockchainRpcService, NoopBroadcastListener,
};
use tycho_core::global_config::{GlobalConfig, ZerostateId};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_network::{
    DhtClient, DhtService, Network, OverlayService, PeerResolver, PublicOverlay, Router,
};
use tycho_rpc::{RpcConfig, RpcState};
use tycho_storage::Storage;
use tycho_util::cli::resolve_public_ip;

use crate::config::{NodeConfig, NodeKeys};

/// Run a Tycho node.
#[derive(Args, Clone)]
pub struct CmdRun {
    /// dump the template of the zero state config
    #[clap(
        short = 'i',
        long,
        conflicts_with_all = ["config", "global_config", "keys", "logger_config", "import_zerostate"]
    )]
    pub init_config: Option<PathBuf>,

    /// overwrite the existing config
    #[clap(short, long)]
    pub force: bool,

    /// path to the node config
    #[clap(long, required_unless_present = "init_config")]
    pub config: Option<PathBuf>,

    /// path to the global config
    #[clap(long, required_unless_present = "init_config")]
    pub global_config: Option<PathBuf>,

    /// path to the node keys
    #[clap(long, required_unless_present = "init_config")]
    pub keys: Option<PathBuf>,

    /// path to the logger config
    #[clap(long)]
    pub logger_config: Option<PathBuf>,

    /// list of zerostate files to import
    #[clap(long)]
    pub import_zerostate: Option<Vec<PathBuf>>,
}

impl CmdRun {
    pub async fn create<C>(self, node_config: NodeConfig<C>) -> Result<Node<C>>
    where
        C: Clone,
    {
        if let Some(metrics) = &node_config.metrics {
            tycho_util::cli::metrics::init_metrics(metrics)?;
        }

        let keys_path = self.keys.unwrap();
        let keys = if keys_path.exists() {
            NodeKeys::from_file(keys_path).context("failed to load node keys")?
        } else {
            let keys = NodeKeys::generate();
            keys.save_to_file(keys_path)?;
            keys
        };

        let node = {
            let global_config = GlobalConfig::from_file(self.global_config.unwrap())
                .context("failed to load global config")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            Node::new(socket_addr, keys, node_config, global_config).await?
        };

        Ok(node)
    }
}

pub struct Node<C> {
    zerostate: ZerostateId,

    network: Network,
    dht_client: DhtClient,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,

    rpc_config: Option<RpcConfig>,
    blockchain_block_provider_config: BlockchainBlockProviderConfig,
    archive_block_provider_config: ArchiveBlockProviderConfig,
    starter_config: StarterConfig,

    run_handle: Option<tokio::task::JoinHandle<()>>,

    config: NodeConfig<C>,
}

impl<C> Node<C> {
    pub async fn new(
        public_addr: SocketAddr,
        keys: NodeKeys,
        node_config: NodeConfig<C>,
        global_config: GlobalConfig,
    ) -> Result<Node<C>>
    where
        C: Clone,
    {
        // Setup network
        let keypair = Arc::new(ed25519::KeyPair::from(&keys.as_secret()));
        let local_id = keypair.public_key.into();

        let config = node_config.clone();

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

        let blockchain_rpc_service = BlockchainRpcService::builder()
            .with_config(node_config.blockchain_rpc_service)
            .with_storage(storage.clone())
            .with_broadcast_listener(NoopBroadcastListener)
            .build();

        let public_overlay = PublicOverlay::builder(zerostate.compute_public_overlay_id())
            .named("blockchain_rpc")
            .with_peer_resolver(peer_resolver.clone())
            .build(blockchain_rpc_service);
        overlay_service.add_public_overlay(&public_overlay);

        let blockchain_rpc_client = BlockchainRpcClient::builder()
            .with_config(node_config.blockchain_rpc_client)
            .with_public_overlay_client(PublicOverlayClient::new(
                network.clone(),
                public_overlay,
                node_config.public_overlay_client,
            ))
            .build();

        tracing::info!(
            overlay_id = %blockchain_rpc_client.overlay().overlay_id(),
            "initialized blockchain rpc"
        );

        Ok(Self {
            zerostate,
            network,
            dht_client,
            peer_resolver,
            overlay_service,
            storage,
            blockchain_rpc_client,
            config,
            rpc_config: node_config.rpc,
            blockchain_block_provider_config: node_config.blockchain_block_provider,
            archive_block_provider_config: node_config.archive_block_provider,
            starter_config: node_config.starter,
            run_handle: None,
        })
    }

    pub async fn init(&self, import_zerostate: Option<Vec<PathBuf>>) -> Result<BlockId> {
        self.wait_for_neighbours().await;

        let init_block_id = self
            .boot(import_zerostate)
            .await
            .context("failed to init node")?;

        tracing::info!(%init_block_id, "node initialized");

        Ok(init_block_id)
    }

    async fn wait_for_neighbours(&self) {
        // Ensure that there are some neighbours
        tracing::info!("waiting for initial neighbours");
        self.blockchain_rpc_client
            .overlay_client()
            .neighbours()
            .wait_for_peers(3)
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

    pub async fn run<S>(&mut self, subscriber: S) -> Result<()>
    where
        S: BlockSubscriber,
    {
        // Create block strider
        let archive_block_provider = ArchiveBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.archive_block_provider_config.clone(),
        );

        let storage_block_provider = StorageBlockProvider::new(self.storage.clone());

        let strider_state =
            PersistentBlockStriderState::new(self.zerostate.as_block_id(), self.storage.clone());

        let blockchain_block_provider = BlockchainBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.blockchain_block_provider_config.clone(),
        )
        .with_fallback(archive_block_provider.clone());

        let gc_subscriber = GcSubscriber::new(self.storage.clone());

        let block_strider = BlockStrider::builder()
            .with_provider(
                archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
            )
            .with_state(strider_state)
            .with_block_subscriber((subscriber, MetricsSubscriber).chain(gc_subscriber))
            .build();

        // Run block strider
        let handle = tokio::spawn(async move {
            tracing::info!("block strider started");
            if let Err(e) = block_strider.run().await {
                tracing::error!(%e, "block strider failed");
            }
            tracing::info!("block strider finished");
        });
        self.run_handle = Some(handle);

        Ok(())
    }

    pub async fn update_validator_set(&self, last_block_id: &BlockId) -> Result<()> {
        // notify subscriber with an initial validators list
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

        Ok(())
    }
    pub async fn create_rpc(&self, last_block_id: &BlockId) -> Result<Option<RpcState>> {
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
                .context("failed to setup RPC server endpoint")?;

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

        Ok(rpc_state)
    }

    pub fn dht_client(&self) -> &DhtClient {
        &self.dht_client
    }

    pub fn peer_resolver(&self) -> &PeerResolver {
        &self.peer_resolver
    }

    pub fn overlay_service(&self) -> &OverlayService {
        &self.overlay_service
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn blockchain_rpc_client(&self) -> &BlockchainRpcClient {
        &self.blockchain_rpc_client
    }

    pub fn network(&self) -> &Network {
        &self.network
    }

    pub fn config(&self) -> &NodeConfig<C> {
        &self.config
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.run_handle.take() {
            handle.abort();
        }
    }
}
