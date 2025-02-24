use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use everscale_crypto::ed25519;
use tokio::sync::{mpsc, oneshot};
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::prelude::{
    EngineBinding, EngineCreated, EngineNetworkArgs, InputBuffer, MempoolAdapterStore,
    MempoolConfigBuilder, MempoolMergedConfig,
};
use tycho_consensus::test_utils::{test_logger, AnchorConsumer};
use tycho_core::block_strider::{FileZerostateProvider, ZerostateProvider};
use tycho_core::global_config::{GlobalConfig, ZerostateId};
use tycho_network::PeerId;
use tycho_storage::{NewBlockMeta, Storage};
use tycho_util::cli::logger::init_logger;
use tycho_util::cli::{resolve_public_ip, signal};
use tycho_util::futures::JoinTask;

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
    import_zerostate: Vec<PathBuf>,

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
            .build_global()?;

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(node_config.threads.tokio_workers)
            .build()?
            .block_on(self.run_impl(node_config))
    }

    async fn run_impl(self, node_config: NodeConfig) -> Result<()> {
        init_logger(&node_config.logger, self.logger_config.clone())?;
        test_logger::set_print_panic_hook(true);

        let stop_fut = signal::any_signal(signal::TERMINATION_SIGNALS);

        let top_known_anchor = self.top_known_anchor.flatten().unwrap_or_default();

        let mempool = Mempool::new(self, node_config).await?;

        let (engine, anchor_consumer) = mempool.boot().await.context("init mempool")?;

        (anchor_consumer.top_known_anchor).set_max_raw(top_known_anchor);

        let (engine_stop_tx, mut engine_stop_rx) = oneshot::channel();
        let _engine = engine.run(engine_stop_tx);

        let _drain_anchors = JoinTask::new(anchor_consumer.drain());

        tracing::info!("started mempool");

        tokio::select! {
            _ = &mut engine_stop_rx => bail!("engine exited"),
            signal = stop_fut => match signal {
                Ok(signal) => {
                    tracing::info!(?signal, "received termination signal");
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        }
    }
}

struct Mempool {
    net_args: EngineNetworkArgs,
    bootstrap_peers: Vec<PeerId>,

    storage: Storage,
    input_buffer: InputBuffer,
    merged_conf: MempoolMergedConfig,
}

impl Mempool {
    async fn new(cmd: CmdRun, node_config: NodeConfig) -> Result<Mempool> {
        let global_config =
            GlobalConfig::from_file(&cmd.global_config).context("failed to load global config")?;

        let bootstrap_peers = global_config
            .bootstrap_peers
            .iter()
            .map(|info| info.id)
            .collect::<Vec<_>>();

        let net_args = {
            let keys = NodeKeys::from_file(&cmd.keys).context("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let public_addr = SocketAddr::new(public_ip, node_config.port);

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

            let key_pair = Arc::new(ed25519::KeyPair::from(&keys.as_secret()));
            let local_id: PeerId = key_pair.public_key.into();

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

            EngineNetworkArgs {
                key_pair,
                network: dht_client.network().clone(),
                peer_resolver,
                overlay_service,
            }
        };

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

        let mc_zerostate = load_mc_zerostate(
            FileZerostateProvider(cmd.import_zerostate),
            &storage,
            &global_config.zerostate,
        )
        .await?;

        let mut config_builder = MempoolConfigBuilder::new(&node_config.mempool);

        config_builder.set_genesis(match &global_config.mempool {
            Some(global_config) => global_config.genesis_info,
            None => mc_zerostate.state_extra()?.consensus_info.genesis_info,
        });

        config_builder.set_consensus_config(&match (global_config.mempool)
            .and_then(|global| global.consensus_config)
        {
            Some(consensus_config) => consensus_config,
            None => (mc_zerostate.config_params()?.params).get_consensus_config()?,
        })?;

        let merged_conf = config_builder.build()?;

        let input_buffer = InputBuffer::new_stub(
            cmd.payload_step,
            cmd.steps_until_full,
            merged_conf.consensus(),
        );

        Ok(Mempool {
            net_args,
            bootstrap_peers,

            storage,
            input_buffer,
            merged_conf: config_builder.build()?,
        })
    }

    pub async fn boot(&self) -> Result<(EngineCreated, AnchorConsumer)> {
        let local_id = self.net_args.network.peer_id();

        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        let mut anchor_consumer = AnchorConsumer::default();
        anchor_consumer.add(*local_id, committed_rx);

        let bind = EngineBinding {
            mempool_adapter_store: MempoolAdapterStore::new(
                self.storage.mempool_storage().clone(),
                anchor_consumer.commit_round.clone(),
            ),
            input_buffer: self.input_buffer.clone(),
            top_known_anchor: anchor_consumer.top_known_anchor.clone(),
            output: committed_tx,
        };

        let engine = EngineCreated::new(bind, &self.net_args, &self.merged_conf);

        engine.handle().set_start_peers(&self.bootstrap_peers);

        tracing::info!("mempool engine initialized");

        Ok((engine, anchor_consumer))
    }
}

async fn load_mc_zerostate(
    provider: FileZerostateProvider,
    storage: &Storage,
    mc_zerostate_id: &ZerostateId,
) -> Result<ShardStateStuff> {
    let zerostates = provider
        .load_zerostates(storage.shard_state_storage().min_ref_mc_state())
        .collect::<Result<Vec<_>, _>>()?;

    let mc_block_id = mc_zerostate_id.as_block_id();

    let mc_zerostate = zerostates
        .into_iter()
        .find(|state| state.block_id() == &mc_block_id)
        .context("no masterchain zerostate provided")?;

    let (mc_zerostate_handle, _) = storage.block_handle_storage().create_or_load_handle(
        mc_zerostate.block_id(),
        NewBlockMeta {
            is_key_block: true,
            gen_utime: mc_zerostate.as_ref().gen_utime,
            ref_by_mc_seqno: 0,
        },
    );

    storage
        .shard_state_storage()
        .store_state(&mc_zerostate_handle, &mc_zerostate, Default::default())
        .await?;

    Ok(mc_zerostate)
}
