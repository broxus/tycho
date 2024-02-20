#![allow(clippy::unused_self)]

use std::process::Command;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::compose::ComposeRunner;
use crate::simulator::Simulator;

mod compose;
mod config;
mod node;
mod simulator;

static DEFAULT_SUBNET: &str = "172.30.0.0/24";

fn main() -> Result<()> {
    let args: Cli = Cli::parse();
    test_prerequisites()?;
    match args.command {
        Commands::Prepare(a) => a.run(),
        Commands::Clean(a) => a.run(),
        Commands::Build(a) => a.run(),
        Commands::Node(a) => a.run(),
    }
}

fn test_prerequisites() -> Result<()> {
    Command::new("docker")
        .arg("--version")
        .output()
        .context("docker not found")?;
    Command::new("docker")
        .arg("compose")
        .arg("--version")
        .output()
        .context("docker-compose not found")?;
    Command::new("cargo").arg("--version").output()?;
    Ok(())
}

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts the network simulation
    Prepare(PrepareCommand),
    /// Cleans up the network simulation setup. By default logs are not removed
    Clean(CleanCommand),
    ///  Builds docker image
    Build(BuildCommand),
    /// Node management
    #[clap(subcommand)]
    Node(NodeCommand),
}

#[derive(Parser)]
struct PrepareCommand {
    /// Number of nodes to start
    #[clap(short, long)]
    #[clap(default_value = "5")]
    nodes: usize,
}

impl PrepareCommand {
    fn run(self) -> Result<()> {
        CleanCommand { logs: true }.run()?;

        let config = config::ServiceConfig::new(DEFAULT_SUBNET.to_string())?;
        let mut sim = Simulator::new(config)?;
        sim.prepare(self.nodes)?;
        Ok(())
    }
}

#[derive(Parser)]
struct CleanCommand {
    #[clap(short, long)]
    #[clap(default_value = "false")]
    logs: bool,
}

impl CleanCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new(DEFAULT_SUBNET.to_string())?;
        let compose = ComposeRunner::load_from_fs(&config)?;
        compose.down()?;
        if self.logs {
            std::fs::remove_dir_all(config.logs_dir())?;
        }
        std::fs::remove_file(config.global_config_path())?;
        std::fs::remove_file(config.compose_path())?;
        std::fs::remove_dir_all(config.entrypoints())?;

        Ok(())
    }
}

#[derive(Parser)]
struct StatusCommand;

impl StatusCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new(DEFAULT_SUBNET.to_string())?;
        let compose = ComposeRunner::load_from_fs(&config)?;

        compose.execute_compose_command(&["ps"])
    }
}

#[derive(Parser)]
struct BuildCommand;

impl BuildCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new(DEFAULT_SUBNET.to_string())?;
        println!("Building docker image");

        Command::new("docker")
            .arg("build")
            .arg("-t")
            .arg("tycho-network")
            .arg("-f")
            .arg(config.project_root.join("network.Dockerfile"))
            .arg(config.project_root)
            .env("DOCKER_BUILDKIT", "1")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()?
            .wait()?;

        Ok(())
    }
}

#[derive(Subcommand)]
enum NodeCommand {
    Add(AddCommand),
    Start(NodeStartCommand),
    Stop(NodeStopCommand),
    Logs(NodeLogsCommand),
    Exec(NodeExecCommand),
    Status(StatusCommand),
}

impl NodeCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new(DEFAULT_SUBNET.to_string())?;
        let compose = ComposeRunner::load_from_fs(&config)?;

        match self {
            NodeCommand::Add(a) => a.run(),
            NodeCommand::Start(a) => a.run(compose),
            NodeCommand::Stop(a) => a.run(compose),
            NodeCommand::Logs(a) => a.run(compose),
            NodeCommand::Exec(a) => a.run(compose),
            NodeCommand::Status(a) => a.run(),
        }
    }
}

#[derive(Parser)]
struct AddCommand;

impl AddCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new(DEFAULT_SUBNET.to_string())?;
        let mut sim = Simulator::new(config)?;
        let next_node_index = sim.next_node_index();
        sim.add_node(next_node_index)?;
        sim.finalize()?;

        println!("Added node-{}", next_node_index);
        Ok(())
    }
}

#[derive(Parser)]
struct NodeStartCommand {
    #[clap(short, long)]
    node_index: Option<usize>,
}

impl NodeStartCommand {
    fn run(self, sim: ComposeRunner) -> Result<()> {
        sim.start_node(self.node_index)
    }
}

#[derive(Parser)]
struct NodeStopCommand {
    #[clap(short, long)]
    node_index: Option<usize>,
}

impl NodeStopCommand {
    fn run(self, compose: ComposeRunner) -> Result<()> {
        compose.stop_node(self.node_index)
    }
}

#[derive(Parser)]
struct NodeLogsCommand {
    #[clap(short, long)]
    node_index: Option<usize>,
    #[clap(short, long)]
    follow: bool,
}

impl NodeLogsCommand {
    fn run(self, compose: ComposeRunner) -> Result<()> {
        compose.logs(self.follow, self.node_index)
    }
}

#[derive(Parser)]
struct NodeExecCommand {
    #[clap(short, long)]
    node_index: usize,
    cmd: String,
    args: Vec<String>,
}

impl NodeExecCommand {
    fn run(self, compose: ComposeRunner) -> Result<()> {
        compose.exec_command(self.node_index, &self.cmd, self.args)
    }
}
