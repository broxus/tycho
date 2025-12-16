use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::backend::KubeCtl;
use crate::config::PodConfig;

#[derive(Subcommand)]
pub enum NodeCommand {
    /// Get pod logs
    Logs(NodeLogsCommand),
    /// Exec shell in pod
    Shell(NodeShellCommand),
}

impl NodeCommand {
    pub fn run(self) -> Result<()> {
        match self {
            NodeCommand::Logs(a) => {
                KubeCtl::logs(&PodConfig::name(a.node_index), a.follow)?;
            }
            NodeCommand::Shell(a) => {
                let ctrl_cmd = a.ctrl.then(|| a.remaining.join(" "));
                KubeCtl::shell(&PodConfig::name(a.node_index), ctrl_cmd)?;
            }
        }
        Ok(())
    }
}

#[derive(Parser)]
pub struct NodeLogsCommand {
    #[clap(short, long)]
    #[clap(default_value_t = 0)]
    node_index: usize,
    #[clap(short, long, action)]
    follow: bool,
}

#[derive(Parser)]
pub struct NodeShellCommand {
    #[clap(short, long)]
    #[clap(default_value_t = 0)]
    node_index: usize,

    /// Everything passed after `--ctrl` is forwarded to mempool control server client,
    /// shorthand for "/app/tycho node mempool ${remaining}"
    #[arg(long)]
    ctrl: bool,

    remaining: Vec<String>,
}
