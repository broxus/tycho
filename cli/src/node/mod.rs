use std::io::IsTerminal;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use everscale_crypto::ed25519;
use tracing_subscriber::EnvFilter;
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_core::block_strider::{
    BlockStrider, BlockchainBlockProvider, BlockchainBlockProviderConfig, NoopSubscriber,
    PersistentBlockStriderState, StorageBlockProvider,
};
use tycho_core::blockchain_rpc::{BlockchainRpcClient, BlockchainRpcService};
use tycho_core::global_config::{GlobalConfig, ZerostateId};
use tycho_core::overlay_client::PublicOverlayClient;
use tycho_network::{
    DhtClient, DhtService, Network, OverlayService, PeerResolver, PublicOverlay, Router,
};
use tycho_storage::Storage;

use crate::util::error::ResultExt;
use crate::util::logger::LoggerConfig;

use self::config::{NodeConfig, NodeKeys};

mod config;

const SERVICE_NAME: &str = "tycho-node";

/// Run a Tycho node.
#[derive(Parser)]
pub struct CmdRun {
    /// dump the template of the zero state config
    #[clap(
        short = 'i',
        long,
        conflicts_with_all = ["config", "global_config", "keys", "logger_config"]
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
}

impl CmdRun {
    pub fn run(self) -> Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(self.run_impl())
    }

    async fn run_impl(self) -> Result<()> {
        if let Some(init_config_path) = self.init_config {
            return NodeConfig::default()
                .save_to_file(init_config_path)
                .wrap_err("failed to save node config");
        }

        init_logger(self.logger_config)?;

        let node = {
            let node_config = NodeConfig::from_file(self.config.unwrap())
                .wrap_err("failed to load node config")?;

            let global_config = GlobalConfig::from_file(self.global_config.unwrap())
                .wrap_err("failed to load global config")?;

            let keys = config::NodeKeys::from_file(&self.keys.unwrap())
                .wrap_err("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip.into(), node_config.port);

            Node::new(socket_addr, keys, node_config, global_config)?
        };

        node.run().await?;

        Ok(())
    }
}

fn init_logger(logger_config: Option<PathBuf>) -> Result<()> {
    let filter = match logger_config {
        None => EnvFilter::builder()
            .with_default_directive(tracing::Level::INFO.into())
            .from_env_lossy(),
        Some(path) => LoggerConfig::load_from(path)
            .wrap_err("failed to load logger config")?
            .build_subscriber(),
    };

    let logger = tracing_subscriber::fmt().with_env_filter(filter);

    if std::io::stdout().is_terminal() {
        logger.init();
    } else {
        logger.without_time().init();
    }

    Ok(())
}

async fn resolve_public_ip(ip: Option<Ipv4Addr>) -> Result<Ipv4Addr> {
    match ip {
        Some(address) => Ok(address),
        None => match public_ip::addr_v4().await {
            Some(address) => Ok(address),
            None => anyhow::bail!("failed to resolve public IP address"),
        },
    }
}

pub struct Node {
    pub zerostate: ZerostateId,

    pub network: Network,
    pub dht_client: DhtClient,
    pub peer_resolver: PeerResolver,
    pub overlay_service: OverlayService,
    pub storage: Storage,
    pub blockchain_rpc_client: BlockchainRpcClient,

    pub state_tracker: MinRefMcStateTracker,
    pub blockchain_block_provider_config: BlockchainBlockProviderConfig,
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
        let storage = Storage::new(node_config.storage).wrap_err("failed to create storage")?;
        tracing::info!(
            root_dir = %storage.root().path().display(),
            "initialized storage"
        );

        // Setup blockchain rpc
        let blockchain_rpc_service =
            BlockchainRpcService::new(storage.clone(), node_config.blockchain_rpc_service);

        let public_overlay =
            PublicOverlay::builder(global_config.zerostate.compute_public_overlay_id())
                .with_peer_resolver(peer_resolver.clone())
                .build(blockchain_rpc_service);
        overlay_service.add_public_overlay(&public_overlay);

        let blockchain_rpc_client = BlockchainRpcClient::new(PublicOverlayClient::new(
            network.clone(),
            public_overlay,
            node_config.public_overlay_client,
        ));

        tracing::info!(
            overlay_id = %blockchain_rpc_client.overlay().overlay_id(),
            "initialized blockchain rpc"
        );

        // Setup block strider
        let state_tracker = MinRefMcStateTracker::default();

        Ok(Self {
            zerostate: global_config.zerostate,
            network,
            dht_client,
            peer_resolver,
            overlay_service,
            blockchain_rpc_client,
            storage,
            state_tracker,
            blockchain_block_provider_config: node_config.blockchain_block_provider,
        })
    }

    async fn run(&self) -> Result<()> {
        let blockchain_block_provider = BlockchainBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.storage.clone(),
            self.blockchain_block_provider_config.clone(),
        );

        let storage_block_provider = StorageBlockProvider::new(self.storage.clone());

        let strider_state =
            PersistentBlockStriderState::new(self.zerostate.as_block_id(), self.storage.clone());

        let block_strider = BlockStrider::builder()
            .with_provider((blockchain_block_provider, storage_block_provider))
            .with_state(strider_state)
            .with_state_subscriber(
                self.state_tracker.clone(),
                self.storage.clone(),
                NoopSubscriber,
            )
            .build();

        tracing::info!("block strider started");

        block_strider.run().await?;

        tracing::info!("block strider finished");
        Ok(())
    }
}
