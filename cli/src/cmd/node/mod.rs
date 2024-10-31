use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tycho_core::global_config::GlobalConfig;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::{resolve_public_ip, signal};

pub use self::control::CmdControl;
use crate::node::{MetricsConfig, Node, NodeConfig, NodeKeys};
use crate::util::alloc::spawn_allocator_metrics_loop;
use crate::BaseArgs;

mod control;

/// Manage the node.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        match self.cmd {
            SubCmd::Run(cmd) => cmd.run(args),
            SubCmd::Control(cmd) => cmd.run(args),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Run(CmdRun),
    #[clap(flatten)]
    Control(CmdControl),
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

    /// Path to the node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    keys: Option<PathBuf>,

    /// Path to the control socket. Default: `$TYCHO_HOME/control.sock`
    #[clap(long)]
    control_socket: Option<PathBuf>,

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
        let node_config = NodeConfig::from_file(args.node_config_path(self.config.as_ref()))
            .context("failed to load node config")?
            .with_relative_paths(&args.home);

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
            let global_config =
                GlobalConfig::from_file(args.global_config_path(self.global_config.as_ref()))
                    .context("failed to load global config")?;

            let keys = NodeKeys::from_file(args.node_keys_path(self.keys.as_ref()))
                .context("failed to load node keys")?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            let control_socket = args.control_socket_path(self.control_socket.as_ref());

            Node::new(
                socket_addr,
                keys,
                node_config,
                global_config,
                control_socket,
            )
            .await?
        };

        node.wait_for_neighbours().await;

        let init_block_id = node
            .boot(self.import_zerostate)
            .await
            .context("failed to init node")?;

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
        .context("failed to initialize a metrics exporter")?;

    #[cfg(feature = "jemalloc")]
    spawn_allocator_metrics_loop();

    Ok(())
}
