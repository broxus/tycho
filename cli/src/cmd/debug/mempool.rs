use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio::signal::unix;
use tokio::sync::{mpsc, oneshot};
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::prelude::{
    EngineBinding, EngineNetworkArgs, EngineSession, InitPeers, InputBuffer, MempoolConfigBuilder,
    MempoolDb, MempoolMergedConfig, Moderator,
};
use tycho_consensus::test_utils::{AnchorConsumer, LastAnchorFile, test_logger};
use tycho_core::block_strider::{FileZerostateProvider, ZerostateProvider};
use tycho_core::global_config::{GlobalConfig, ZerostateId};
use tycho_core::node::NodeKeys;
use tycho_core::storage::{CoreStorage, NewBlockMeta};
use tycho_crypto::ed25519;
use tycho_network::PeerId;
use tycho_storage::StorageContext;
use tycho_types::cell::HashBytes;
use tycho_util::cli::logger::init_logger;
use tycho_util::cli::metrics::init_metrics;
use tycho_util::cli::{resolve_public_ip, signal};
use tycho_util::futures::JoinTask;

use crate::node::NodeConfig;

/// run a node
#[derive(Parser)]
pub struct CmdRun {
    /// path to the node config
    #[clap(long)]
    config: PathBuf,

    /// path to the global config
    #[clap(long)]
    global_config: PathBuf,

    #[clap(flatten)]
    key_variant: KeyVariant,

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

    /// `ctrl+c` will restart mempool engine keeping the cli while the value exceeds 1,
    /// and cli will be shut down when value reaches 0
    #[allow(clippy::option_option, reason = "run-mempool.sh")]
    #[arg(long)]
    restarts: Option<Option<u8>>,

    /// step is an amount of points produced by node for payload to grow in size
    #[arg(short, long, default_value_t = 0)]
    payload_step: usize,

    /// number of steps in which payload will increase from 0 to max configured value
    #[arg(short, long, default_value_t = NonZeroUsize::new(1).unwrap())]
    steps_until_full: NonZeroUsize,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
pub struct KeyVariant {
    /// ed25519 secret key
    #[clap(long)]
    key: Option<String>,

    /// path to the node ed25519 keypair
    #[clap(long)]
    keys: Option<PathBuf>,
}

impl TryFrom<&KeyVariant> for NodeKeys {
    type Error = anyhow::Error;

    fn try_from(variant: &KeyVariant) -> std::result::Result<Self, Self::Error> {
        anyhow::ensure!(
            variant.key.is_some() ^ variant.keys.is_some(),
            "strictly one of the two arg options must be provided; got: key={}, keys={}",
            variant.key.is_some(),
            variant.keys.is_some()
        );
        if let Some(secret) = &variant.key {
            Ok(NodeKeys {
                secret: <HashBytes as std::str::FromStr>::from_str(secret)
                    .context("failed to parse node secret key")?,
            })
        } else if let Some(path) = &variant.keys {
            NodeKeys::from_file(path).context("failed to load node keys")
        } else {
            unreachable!()
        }
    }
}

impl CmdRun {
    pub fn run(self) -> Result<()> {
        let node_config =
            NodeConfig::from_file(&self.config).context("failed to load node config")?;

        node_config.threads.init_global_rayon_pool()?;

        node_config
            .threads
            .build_tokio_runtime()?
            .block_on(self.run_impl(node_config))
    }

    async fn run_impl(self, node_config: NodeConfig) -> Result<()> {
        init_logger(&node_config.logger, self.logger_config.clone())?;
        test_logger::set_print_panic_hook(true);

        if let Some(metrics_config) = &node_config.metrics {
            init_metrics(metrics_config)?;
        }

        let mut stop_fut = any_signal_repeatable(signal::TERMINATION_SIGNALS);

        let mut restarts_remain: u8 = self.restarts.unwrap_or_default().unwrap_or_default();

        let mempool = Mempool::new(self, node_config).await?;
        let file_storage = mempool.mempool_db.file_storage()?;

        loop {
            let (engine_stop_tx, mut engine_stop_rx) = oneshot::channel();

            let (session, anchor_consumer) =
                mempool.boot(engine_stop_tx).context("init mempool")?;

            let mut last_anchor_file = LastAnchorFile::reopen_in(&file_storage)?;
            (anchor_consumer.top_known_anchor).set_max_raw(last_anchor_file.read()?);

            let drain_anchors = JoinTask::new(anchor_consumer.drain(last_anchor_file));

            tracing::info!("started mempool");

            tokio::select! {
                result = &mut engine_stop_rx => bail!("engine exited as {result:?}"),
                stop = stop_fut.recv() => match stop {
                    Some(signal) => {
                        drop(drain_anchors);
                        session.stop().await;
                        restarts_remain = restarts_remain.saturating_sub(1);
                        tracing::warn!(?signal, ?restarts_remain, "received termination");
                        if restarts_remain == 0 {
                            return Ok(());
                        }
                    }
                    None => bail!("cli signal listener channel dropped"),
                }
            }
        }
    }
}

struct Mempool {
    net_args: EngineNetworkArgs,
    init_peers: InitPeers,

    mempool_db: Arc<MempoolDb>,
    input_buffer: InputBuffer,
    merged_conf: MempoolMergedConfig,
}

impl Mempool {
    async fn new(cmd: CmdRun, node_config: NodeConfig) -> Result<Mempool> {
        let global_config =
            GlobalConfig::from_file(&cmd.global_config).context("failed to load global config")?;

        let init_peers =
            InitPeers::new((global_config.bootstrap_peers.iter().map(|info| info.id)).collect());

        let core_storage = {
            let ctx = StorageContext::new(node_config.storage.clone())
                .await
                .context("failed to create storage context")?;
            let mut core_storage = node_config.core_storage.clone();
            if std::mem::replace(&mut core_storage.blob_db.pre_create_cas_tree, false) {
                tracing::warn!("Cas_tree will not be created, blob_db config ignored");
            }
            CoreStorage::open(ctx, core_storage)
                .await
                .context("failed to create storage")?
        };
        let mempool_db = MempoolDb::open(core_storage.context().clone())?;

        let net_args = {
            let keys = NodeKeys::try_from(&cmd.key_variant)?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let public_addr = SocketAddr::new(public_ip, node_config.port);

            let local_addr = SocketAddr::from((node_config.local_ip, node_config.port));

            let (dht_client, peer_resolver, overlay_service) =
                tycho_consensus::test_utils::from_validator(
                    local_addr,
                    &keys.as_secret(),
                    Some(public_addr),
                    node_config.dht.clone(),
                    Some(node_config.peer_resolver.clone()),
                    Some(node_config.overlay.clone()),
                    node_config.network.clone(),
                );

            let key_pair = Arc::new(ed25519::KeyPair::from(&keys.as_secret()));
            let local_id: PeerId = key_pair.public_key.into();

            let mut peer_count = 0usize;
            for peer in global_config.bootstrap_peers {
                let is_new = dht_client.add_peer(Arc::new(peer))?;
                peer_count += is_new as usize;
            }

            let moderator = Moderator::new(
                dht_client.network(),
                mempool_db.clone(),
                node_config.mempool.moderator,
                crate::version_string(),
            )?;

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
                moderator,
            }
        };

        // Setup storage

        let mut last_anchor_file = LastAnchorFile::reopen_in(&mempool_db.file_storage()?)?;
        let last_anchor_opt = last_anchor_file.read_opt()?;
        last_anchor_file.update(
            cmd.top_known_anchor
                .flatten()
                .or(last_anchor_opt)
                .unwrap_or_default(),
        )?;

        tracing::info!(
            root_dir = %core_storage.context().root_dir().path().display(),
            "initialized storage"
        );

        let mc_zerostate = load_mc_zerostate(
            FileZerostateProvider(cmd.import_zerostate),
            &core_storage,
            &global_config.zerostate,
        )
        .await?;

        let mut config_builder = MempoolConfigBuilder::new(&node_config.mempool.node);

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
            &merged_conf.conf.consensus,
        );

        Ok(Mempool {
            net_args,
            init_peers,

            mempool_db,
            input_buffer,
            merged_conf,
        })
    }

    pub fn boot(
        &self,
        engine_stop_tx: oneshot::Sender<()>,
    ) -> Result<(EngineSession, AnchorConsumer)> {
        let local_id = self.net_args.network.peer_id();

        let (anchors_tx, anchors_rx) = mpsc::unbounded_channel();
        let mut anchor_consumer = AnchorConsumer::default();
        anchor_consumer.add(*local_id, anchors_rx);

        let bind = EngineBinding {
            mempool_db: self.mempool_db.clone(),
            input_buffer: self.input_buffer.clone(),
            top_known_anchor: anchor_consumer.top_known_anchor.clone(),
            commit_finished: anchor_consumer.commit_finished.clone(),
            anchors_tx,
        };

        let session = EngineSession::new(
            bind,
            &self.net_args,
            &self.merged_conf,
            self.init_peers.clone(),
            engine_stop_tx,
        );

        tracing::info!("mempool engine initialized");

        Ok((session, anchor_consumer))
    }
}

async fn load_mc_zerostate(
    provider: FileZerostateProvider,
    storage: &CoreStorage,
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

/// Version of [`any_signal()`](tycho_util::cli::signal::any_signal)
/// that allows to receive same signal multiple times
fn any_signal_repeatable<I, T>(signals: I) -> mpsc::UnboundedReceiver<unix::SignalKind>
where
    I: IntoIterator<Item = T>,
    T: Into<unix::SignalKind> + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel();

    let mut signal_zip_rx = signals
        .into_iter()
        .map(|signal| {
            let signal = signal.into();
            let signal_rx = unix::signal(signal).expect("Failed subscribing on unix signals");
            (signal, signal_rx)
        })
        .collect::<Vec<_>>();

    tokio::spawn(async move {
        loop {
            let any_signal = futures_util::future::select_all(signal_zip_rx.iter_mut().map(
                |(signal, signal_rx)| {
                    Box::pin(async move {
                        match signal_rx.recv().await {
                            Some(()) => signal,
                            None => panic!("no more signal {signal:?} can be received"),
                        }
                    })
                },
            ));

            let signal = *any_signal.await.0;

            match tx.send(signal) {
                Ok(()) => {}
                Err(e) => panic!("failed to send signal {signal:?}: {e}"),
            }
        }
    });

    rx
}
