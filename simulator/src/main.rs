#![allow(clippy::unused_self, clippy::print_stdout, clippy::print_stderr)]

use std::process::Command;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::helm::{ClusterType, HelmRunner};
use crate::simulator::Simulator;

mod config;
mod node;
mod simulator;

mod helm;

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
    Command::new("docker")
        .arg("buildx")
        .arg("--version")
        .output()?;
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

    #[clap(short, long)]
    #[clap(default_value = "k3s")]
    cluster_type: ClusterType,
}

impl PrepareCommand {
    fn run(self) -> Result<()> {
        CleanCommand {
            cluster_type: self.cluster_type,
        }
        .run()
        .ok();

        let config = config::ServiceConfig::new()?;
        let mut sim = Simulator::new(config, self.cluster_type)?;
        sim.prepare(self.nodes)?;
        Ok(())
    }
}

#[derive(Parser)]
struct CleanCommand {
    #[clap(short, long)]
    #[clap(default_value = "k3s")]
    cluster_type: ClusterType,
}

impl CleanCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new()?;
        let helm = HelmRunner::new(config.clone(), ClusterType::K3S);
        helm.stop_node("tycho")?;

        std::fs::remove_file(config.global_config_path())?;
        std::fs::remove_file(config.helm_template_output())?;

        Ok(())
    }
}

#[derive(Parser)]
struct StatusCommand;

impl StatusCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new()?;
        let compose = HelmRunner::new(config.clone(), ClusterType::K3S);

        compose.ps()?;
        Ok(())
    }
}

#[derive(Parser)]
struct BuildCommand {
    #[clap(short, long)]
    #[clap(default_value = "k3s")]
    cluster_type: ClusterType,
}

impl BuildCommand {
    fn run(self) -> Result<()> {
        let config = config::ServiceConfig::new()?;
        println!("Building docker image");

        Command::new("docker")
            .arg("build")
            .arg("-t")
            .arg("tycho-network")
            .arg("-f")
            .arg(&config.project_root.join("network.Dockerfile"))
            .arg(&config.project_root)
            .env("DOCKER_BUILDKIT", "1")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()?
            .wait()?;
        let helm = HelmRunner::new(config.clone(), ClusterType::K3S);
        helm.upload_image("tycho-network")?;

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
        let config = config::ServiceConfig::new()?;
        // todo: update to actual cluster type
        let compose = HelmRunner::new(config.clone(), ClusterType::K3S);

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
struct AddCommand {
    #[clap(short, long)]
    pub delay: Option<u16>,
    #[clap(short, long)]
    pub loss: Option<u16>,
}

impl AddCommand {
    fn run(self) -> Result<()> {
        todo!()
    }
}

#[derive(Parser)]
struct NodeStartCommand {
    node_index: Option<usize>,
}

impl NodeStartCommand {
    fn run(self, sim: HelmRunner) -> Result<()> {
        sim.start_node("tycho", "tycho", None)?;

        Ok(())
    }
}

#[derive(Parser)]
struct NodeStopCommand {
    #[clap(short, long)]
    node_index: Option<usize>,
}

impl NodeStopCommand {
    fn run(self, compose: HelmRunner) -> Result<()> {
        compose.stop_node("tycho")?;
        Ok(())
    }
}

#[derive(Parser)]
struct NodeLogsCommand {
    #[clap(short, long)]
    #[clap(default_value = "0")]
    node_index: usize,
    #[clap(short, long)]
    follow: bool,
}

impl NodeLogsCommand {
    fn run(self, compose: HelmRunner) -> Result<()> {
        compose.logs(&format!("tycho-{}", self.node_index), self.follow)?;

        Ok(())
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
    fn run(self, compose: HelmRunner) -> Result<()> {
        compose.exec_command(&format!("tycho-{}", self.node_index), &self.args)?;
        Ok(())
    }
}
