#![allow(clippy::print_stdout)]

use std::process::Command;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::backend::Helm;
use crate::commands::{BuildCommand, CleanCommand, NodeCommand, PrepareCommand, StartCommand};
use crate::config::SimulatorConfig;

mod backend;
mod commands;
mod config;

fn main() -> Result<()> {
    let args: Cli = Cli::parse();

    test_prerequisites()?;
    let config = SimulatorConfig::new()?;

    match args.command {
        Commands::Build(cmd) => cmd.run(&config)?,
        Commands::Clean => {
            if !CleanCommand::run(&config) {
                anyhow::bail!("there were errors during clean");
            }
        }
        Commands::Node(cmd) => cmd.run()?,
        Commands::Prepare => {
            CleanCommand::run(&config);
            PrepareCommand::run(&config)?;
        }
        Commands::Start(cmd) => cmd.run(&config)?,
        Commands::Stop => Helm::uninstall(&config)?,
    }

    Ok(())
}

/// Network simulator
#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Setup config files for network simulation
    Prepare,
    /// Helm uninstalls `tycho` chart and deletes its `values` file
    Clean,
    /// Builds docker image
    #[clap(subcommand)]
    Build(BuildCommand),
    /// Helm installs `tycho` chart
    Start(StartCommand),
    /// Helm uninstalls `tycho` chart
    Stop,
    /// Node management
    #[clap(subcommand)]
    Node(NodeCommand),
}

fn test_prerequisites() -> Result<()> {
    Command::new("git")
        .arg("--version")
        .output()
        .context("git not found")?;
    Command::new("kubectl")
        .arg("version")
        .output()
        .context("kubectl not found")?;
    Command::new("helm")
        .arg("version")
        .output()
        .context("helm not found")?;
    Ok(())
}
