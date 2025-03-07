use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::tasks::gen_network::GenNetworkArgs;

mod tasks;

/// Tycho development task runner
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Generate Protocol Buffers definitions for tycho-rpc crate
    GenProto,
    GenNetwork(GenNetworkArgs),
}

fn main() -> Result<()> {
    std::env::set_var("RUST_BACKTRACE", "1");

    let cli = Cli::parse();

    match cli.command {
        Commands::GenProto => tasks::gen_proto::run().context("Failed to generate protocol files"),
        Commands::GenNetwork(args) => {
            tasks::gen_network::run(args).context("Failed to generate network")
        }
    }
}
