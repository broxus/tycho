#![allow(clippy::unused_self, clippy::print_stdout, clippy::print_stderr)]

use std::process::Command;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::commands::{BuildCommand, CleanCommand, NodeCommand, PrepareCommand};
use crate::config::SimulatorConfig;

mod commands;
mod config;
mod helm;

fn main() -> Result<()> {
    let args: Cli = Cli::parse();
    test_prerequisites()?;

    let config = SimulatorConfig::new().context("create config")?;

    match args.command {
        Commands::Prepare(cmd) => {
            let clean = CleanCommand {
                cluster_type: cmd.cluster_type,
            };
            clean.run(&config);
            cmd.run(&config)
        }
        Commands::Clean(cmd) => {
            if cmd.run(&config) {
                Ok(())
            } else {
                Err(anyhow::anyhow!("there were errors during clean"))
            }
        }
        Commands::Build(cmd) => cmd.run(&config),
        Commands::Node(cmd) => cmd.run(&config),
    }
}

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Setup config files for network simulation
    Prepare(PrepareCommand),
    /// Cleans up the network simulation setup. By default logs are not removed
    Clean(CleanCommand),
    /// Builds docker image
    Build(BuildCommand),
    /// Node management
    #[clap(subcommand)]
    Node(NodeCommand),
}

fn test_prerequisites() -> Result<()> {
    Command::new("git")
        .arg("--version")
        .output()
        .context("git not found, will not determine project root")?;
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
    Command::new("helm").arg("version").output()?;
    Command::new("cargo").arg("--version").output()?;
    Ok(())
}
