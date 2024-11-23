use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use everscale_crypto::ed25519;
use tokio::sync::mpsc;
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::prelude::{Engine, InputBuffer, MempoolAdapterStore, MempoolConfigBuilder};
use tycho_consensus::test_utils::{test_logger, AnchorConsumer};
use tycho_core::block_strider::{FileZerostateProvider, ZerostateProvider};
use tycho_core::global_config::{GlobalConfig, MempoolGlobalConfig, ZerostateId};
use tycho_network::{DhtClient, OverlayService, PeerId, PeerResolver};
use tycho_storage::{NewBlockMeta, Storage};
use tycho_util::cli::logger::init_logger;
use tycho_util::cli::{resolve_public_ip, signal};

use crate::node::{NodeConfig, NodeKeys};

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

    /// list of zerostate files to import
    #[clap(long)]
    import_zerostate: Option<Vec<PathBuf>>,

    /// simulate anchor id from last signed mc block to start with (otherwise may be paused)
    #[allow(clippy::option_option, reason = "run-mempool.sh")]
    #[clap(long)]
    top_known_anchor: Option<Option<u32>>,

    /// step is an amount of points produced by node for payload to grow in size
    #[arg(short, long, default_value_t = 0)]
    payload_step: usize,

    /// number of steps in which payload will increase from 0 to max configured value
    #[arg(short, long, default_value_t = NonZeroUsize::new(1).unwrap())]
    steps_until_full: NonZeroUsize,
}

impl CmdRun {
    pub fn run(self) -> Result<()> {
        let node_config =
            NodeConfig::from_file(&self.config).context("failed to load node config")?;

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
        test_logger::set_print_panic_hook(true);

        let mempool = {
            let global_config = GlobalConfig::from_file(self.global_config)
                .context("failed to load global config")?;

            let keys = NodeKeys::from_file(self.keys).context("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            Mempool::new(socket_addr, keys, node_config, global_config).await?
        };

        let mc_zerostate = mempool.load_zerostate(self.import_zerostate).await?;

        let input_buffer = InputBuffer::new_stub(self.payload_step, self.steps_until_full);

        let (engine, anchor_consumer) = mempool
            .boot(input_buffer, mc_zerostate)
            .await
            .context("failed to init mempool")?;

        anchor_consumer.top_known_anchor().set_max_raw(
            self.top_known_anchor
                .unwrap_or_default()
                .unwrap_or_default(),
        );

        tokio::spawn(anchor_consumer.drain());

        tracing::info!("starting mempool");

        engine.run().await;

        Ok(())
    }
}

struct Mempool {
    keypair: Arc<ed25519::KeyPair>,

    zerostate: ZerostateId,

    dht_client: DhtClient,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    storage: Storage,

    bootstrap_peers: Vec<PeerId>,

    config_builder: MempoolConfigBuilder,
    config_override: Option<MempoolGlobalConfig>,
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

        let bootstrap_peers = global_config
            .bootstrap_peers
            .iter()
            .map(|info| info.id)
            .collect::<Vec<_>>();

        let mut peer_count = 0usize;
        for peer in global_config.bootstrap_peers {
            let is_new = dht_client.add_peer(Arc::new(peer))?;
            peer_count += is_new as usize;
        }

        tracing::info!(
            %local_id,
            %local_addr,
            %public_addr,
            bootstrap_peers = peer_count,
            "initialized network"
        );

        // Setup storage
        let storage = Storage::builder()
            .with_config(node_config.storage)
            .build()
            .await
            .context("failed to create storage")?;

        tracing::info!(
            root_dir = %storage.root().path().display(),
            "initialized storage"
        );

        let mut config_builder = MempoolConfigBuilder::default();
        config_builder.set_node_config(&node_config.mempool);

        Ok(Self {
            keypair,
            zerostate: global_config.zerostate,
            dht_client,
            peer_resolver,
            overlay_service,
            bootstrap_peers,
            storage,
            config_builder,
            config_override: global_config.mempool,
        })
    }

    pub async fn boot(
        mut self,
        input_buffer: InputBuffer,
        zerostate: ShardStateStuff,
    ) -> Result<(Engine, AnchorConsumer)> {
        let local_id = self.dht_client.network().peer_id();

        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        let mut anchor_consumer = AnchorConsumer::default();
        anchor_consumer.add(*local_id, committed_rx);

        // FIXME load genesis data from McStateExtra instead of using default
        let global_config = self.config_override.unwrap_or(MempoolGlobalConfig {
            start_round: 0,
            genesis_time_millis: 0,
            consensus_config: None,
        });

        self.config_builder
            .set_genesis(global_config.start_round, global_config.genesis_time_millis);

        let consensus_config = match &global_config.consensus_config {
            Some(consensus_config) => consensus_config,
            None => {
                let config = zerostate.config_params()?;
                &config.params.get_consensus_config()?
            }
        };
        self.config_builder.set_consensus_config(consensus_config);

        let engine = Engine::new(
            self.keypair.clone(),
            self.dht_client.network(),
            &self.peer_resolver,
            &self.overlay_service,
            &MempoolAdapterStore::new(
                self.storage.mempool_storage().clone(),
                anchor_consumer.commit_round().clone(),
            ),
            input_buffer,
            committed_tx,
            anchor_consumer.top_known_anchor(),
            &self.config_builder.build()?,
        );

        engine.set_start_peers(&self.bootstrap_peers);

        tracing::info!("mempool engine initialized");

        Ok((engine, anchor_consumer))
    }

    async fn load_zerostate(
        &self,
        import_zerostate: Option<Vec<PathBuf>>,
    ) -> Result<ShardStateStuff> {
        let mc_zerostate = import_zerostate
            .map(FileZerostateProvider)
            .context("no zerostates provided")?
            .load_zerostates(self.storage.shard_state_storage().min_ref_mc_state())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|state| state.block_id() == &self.zerostate.as_block_id())
            .context("no masterchain zerostate provided")?;

        let (mc_zerostate_handle, _) = self.storage.block_handle_storage().create_or_load_handle(
            mc_zerostate.block_id(),
            NewBlockMeta {
                is_key_block: true,
                gen_utime: mc_zerostate.as_ref().gen_utime,
                ref_by_mc_seqno: 0,
            },
        );

        self.storage
            .shard_state_storage()
            .store_state(&mc_zerostate_handle, &mc_zerostate)
            .await?;

        Ok(mc_zerostate)
    }
}
