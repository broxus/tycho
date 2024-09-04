use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use everscale_crypto::ed25519;
use tokio::sync::mpsc;
use tycho_consensus::prelude::{Engine, InputBuffer};
use tycho_consensus::test_utils::AnchorConsumer;
use tycho_core::global_config::GlobalConfig;
use tycho_network::{DhtClient, OverlayService, PeerId, PeerResolver};
use tycho_storage::Storage;
use tycho_util::cli::error::ResultExt;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::{resolve_public_ip, signal};

use crate::node::config::{NodeConfig, NodeKeys};

/// run a node
#[derive(Parser)]
pub struct CmdRun {
    /// path to the node config
    #[clap(long)]
    config: PathBuf,

    /// path to the global config
    #[clap(long)]
    global_config: PathBuf,

    /// path to the node keys
    #[clap(long)]
    keys: PathBuf,

    /// path to the logger config
    #[clap(long)]
    logger_config: Option<PathBuf>,

    /// Round of a new consensus genesis
    #[clap(long)]
    mempool_start_round: Option<u32>,
}

impl CmdRun {
    pub fn run(self) -> Result<()> {
        let node_config =
            NodeConfig::from_file(&self.config).wrap_err("failed to load node config")?;

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
        init_logger(&node_config.logger, self.logger_config)?;
        set_abort_with_tracing();

        let mempool = {
            let global_config = GlobalConfig::from_file(self.global_config)
                .wrap_err("failed to load global config")?;

            let keys = NodeKeys::from_file(self.keys).wrap_err("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            Mempool::new(socket_addr, keys, node_config, global_config).await?
        };

        let (engine, anchor_consumer) = mempool
            .boot(self.mempool_start_round)
            .await
            .wrap_err("failed to init mempool")?;

        tokio::spawn(anchor_consumer.drain());

        tracing::info!("starting mempool");

        engine.run().await;

        Ok(())
    }
}

struct Mempool {
    keypair: Arc<ed25519::KeyPair>,

    dht_client: DhtClient,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    storage: Storage,
    all_peers: Vec<PeerId>,
}

impl Mempool {
    pub async fn new(
        public_addr: SocketAddr,
        keys: NodeKeys,
        node_config: NodeConfig,
        global_config: GlobalConfig,
    ) -> Result<Self> {
        let local_addr = SocketAddr::from((node_config.local_ip, node_config.port));

        let (dht_client, peer_resolver, overlay_service) =
            tycho_consensus::test_utils::from_validator(
                local_addr,
                &keys.as_secret(),
                Some(public_addr),
                node_config.dht,
                Some(node_config.peer_resolver),
                Some(node_config.overlay),
                node_config.network,
            );

        let keypair = Arc::new(ed25519::KeyPair::from(&keys.as_secret()));
        let local_id: PeerId = keypair.public_key.into();

        let all_peers = global_config
            .bootstrap_peers
            .iter()
            .map(|info| info.id)
            .collect::<Vec<_>>();

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
            .build()
            .await
            .wrap_err("failed to create storage")?;
        tracing::info!(
            root_dir = %storage.root().path().display(),
            "initialized storage"
        );

        Ok(Self {
            keypair,
            dht_client,
            peer_resolver,
            overlay_service,
            all_peers,
            storage,
        })
    }

    pub async fn boot(self, mempool_start_round: Option<u32>) -> Result<(Engine, AnchorConsumer)> {
        let local_id = self.dht_client.network().peer_id();

        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        let mut anchor_consumer = AnchorConsumer::default();
        anchor_consumer.add(*local_id, committed_rx);

        let mut engine = Engine::new(
            self.keypair.clone(),
            self.dht_client.network(),
            &self.peer_resolver,
            &self.overlay_service,
            self.storage.mempool_storage(),
            committed_tx,
            anchor_consumer.collator_round(),
            InputBuffer::new_stub(
                NonZeroUsize::new(100).unwrap(),
                NonZeroUsize::new(5).unwrap(),
            ),
            mempool_start_round,
        );

        engine.init_with_genesis(&self.all_peers);

        tracing::info!("mempool engine initialized");

        Ok((engine, anchor_consumer))
    }
}
