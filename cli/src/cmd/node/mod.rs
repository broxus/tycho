use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tycho_core::global_config::GlobalConfig;
use tycho_core::node::NodeKeys;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::metrics::init_metrics;
use tycho_util::cli::{resolve_public_ip, signal};

pub use self::control::CmdControl;
use crate::node::{Node, NodeConfig};
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

            let node_keys_path = args.node_keys_path(self.keys.as_ref());
            let node_keys = NodeKeys::load_or_create(node_keys_path)?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            let control_socket = args.control_socket_path(self.control_socket.as_ref());

            Node::new(
                socket_addr,
                node_keys,
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

        node.run(&init_block_id).await?;

        Ok(())
    }
}
