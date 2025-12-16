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
                let args = a.ctrl.then_some(a.remaining);
                KubeCtl::shell(&PodConfig::name(a.node_index), args)?;
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

    /// Everything passed after `--ctrl` is forwarded as argv to `/app/tycho node mempool ...`
    #[arg(long)]
    ctrl: bool,

    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    remaining: Vec<String>,
}
