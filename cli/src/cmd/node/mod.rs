use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tycho_core::block_strider::ColdBootType;
use tycho_core::global_config::GlobalConfig;
use tycho_core::node::NodeKeys;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::metrics::init_metrics;
use tycho_util::cli::{resolve_public_ip, signal};

pub use self::control::CmdControl;
pub use self::mempool_control::MempoolServer;
use crate::BaseArgs;
use crate::node::{Node, NodeConfig};

mod control;
mod mempool_control;

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
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// Path to the global config. Default: `$TYCHO_HOME/global-config.json`
    #[clap(short, long)]
    global_config: Option<PathBuf>,

    /// Path to the node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(short, long)]
    keys: Option<PathBuf>,

    /// Path to the control socket. Default: `$TYCHO_HOME/control.sock`
    #[clap(short = 't', long)]
    control_socket: Option<PathBuf>,

    /// Path to the logger config.
    #[clap(short, long)]
    logger_config: Option<PathBuf>,

    /// List of zerostate files to import.
    #[clap(short = 'z', long)]
    import_zerostate: Option<Vec<PathBuf>>,

    /// Path to the work units tuner config.
    #[clap(long)]
    wu_tuner_config: Option<PathBuf>,

    /// Overwrite cold boot type. Default: `latest-persistent`
    #[clap(short = 'b', long)]
    cold_boot: Option<ColdBootType>,

    /// Pass this flag if you used `just gen_network 1` for manual tests
    #[clap(short = 's', long, action)]
    single_node: bool,
}

impl CmdRun {
    fn run(self, args: BaseArgs) -> Result<()> {
        let node_config = NodeConfig::from_file(args.node_config_path(self.config.as_ref()))
            .context("failed to load node config")?
            .with_relative_paths(&args.home);

        node_config
            .threads
            .init_all_and_run(signal::run_or_terminate(self.run_impl(args, node_config)))
    }

    async fn run_impl(self, args: BaseArgs, mut node_config: NodeConfig) -> Result<()> {
        init_logger(&node_config.logger, self.logger_config)?;
        set_abort_with_tracing();

        if let Some(metrics_config) = &node_config.metrics {
            init_metrics(metrics_config)?;
        }

        if self.single_node {
            let too_new_archive_threshold =
                &mut node_config.blockchain_rpc_client.too_new_archive_threshold;
            if *too_new_archive_threshold > 0 {
                tracing::warn!(
                    ?too_new_archive_threshold,
                    "blockchain rpc client setting was ignored due to single-node mode"
                );
                *too_new_archive_threshold = 0;
            }
        }

        let mut node = {
            let global_config =
                GlobalConfig::from_file(args.global_config_path(self.global_config.as_ref()))
                    .context("failed to load global config")?;

            let node_keys_path = args.node_keys_path(self.keys.as_ref());
            let node_keys = NodeKeys::load_or_create(node_keys_path)?;

            let public_ip = resolve_public_ip(node_config.public_ip).await?;
            let socket_addr = SocketAddr::new(public_ip, node_config.port);

            let control_socket = args.control_socket_path(self.control_socket.as_ref());

            let wu_tuner_config_path = args.wu_tuner_config_path(self.wu_tuner_config.as_ref());

            Node::new(
                socket_addr,
                node_keys,
                node_config,
                global_config,
                control_socket,
                wu_tuner_config_path,
                self.single_node,
            )
            .await?
        };

        if let Some(cold_boot_type) = self.cold_boot {
            node.overwrite_cold_boot_type(cold_boot_type);
        }

        if self.single_node {
            if let Some(cold_boot_type) = self.cold_boot
                && cold_boot_type != ColdBootType::Genesis
            {
                tracing::warn!(
                    ?cold_boot_type,
                    "cold boot type settings was ignored due to single-node mode"
                );
            }
            node.overwrite_cold_boot_type(ColdBootType::Genesis);
        } else {
            node.wait_for_neighbours().await;
        }

        let init_block_id = node
            .boot(self.import_zerostate)
            .await
            .context("failed to init node")?;

        tracing::info!(%init_block_id, "node initialized");

        node.run(&init_block_id, self.single_node).await?;

        Ok(())
    }
}
