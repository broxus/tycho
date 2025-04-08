use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::{PodConfig, SimulatorConfig};
use crate::helm::HelmRunner;

#[derive(Subcommand)]
pub enum NodeCommand {
    Add(AddCommand),
    Start(StartCommand),
    Stop,
    Logs(NodeLogsCommand),
    Exec(NodeExecCommand),
    Status,
}

impl NodeCommand {
    pub fn run(self, config: &SimulatorConfig) -> Result<()> {
        // todo: update to actual cluster type

        match self {
            NodeCommand::Add(_) => {
                panic!("unimplemented: add node");
            }
            NodeCommand::Start(a) => {
                HelmRunner::upgrade_install(config, a.debug, None)?;
            }
            NodeCommand::Stop => {
                HelmRunner::uninstall(config)?;
            }
            NodeCommand::Logs(a) => {
                HelmRunner::logs(&PodConfig::name(a.node_index), a.follow)?;
            }
            NodeCommand::Exec(a) => {
                let pod_name = PodConfig::name(a.node_index);
                HelmRunner::exec_command(&pod_name, a.stdin, a.tty, &a.cmd, &a.args)?;
            }
            NodeCommand::Status => {
                HelmRunner::ps()?;
            }
        }
        Ok(())
    }
}

#[derive(Parser)]
pub struct AddCommand {
    #[clap(short, long)]
    pub delay: Option<u16>,
    #[clap(short, long)]
    pub loss: Option<u16>,
}

#[derive(Parser)]
pub struct StartCommand {
    #[clap(short, long, action)]
    pub debug: bool,
}

#[derive(Parser)]
pub struct NodeLogsCommand {
    #[clap(short, long)]
    #[clap(default_value = "0")]
    node_index: usize,
    #[clap(short, long)]
    follow: bool,
}

#[derive(Parser)]
pub struct NodeExecCommand {
    #[clap(short, long)]
    node_index: usize,
    #[clap(short = 'i', long, action)]
    stdin: bool,
    #[clap(short, long, action)]
    tty: bool,
    cmd: String,
    args: Vec<String>,
}
