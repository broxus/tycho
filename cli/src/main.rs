#![allow(clippy::print_stdout, clippy::print_stderr, clippy::exit)] // it's a CLI tool

use std::process::ExitCode;
use std::sync::OnceLock;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod tools {
    pub mod gen_account;
    pub mod gen_dht;
    pub mod gen_key;
    pub mod gen_zerostate;
}

mod node;
mod util;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> ExitCode {
    if std::env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    match App::parse().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Tycho Node
#[derive(Parser)]
#[clap(name = "tycho")]
#[clap(version = version_string())]
#[clap(subcommand_required = true, arg_required_else_help = true)]
struct App {
    #[clap(subcommand)]
    cmd: Cmd,
}

impl App {
    fn run(self) -> Result<()> {
        self.cmd.run()
    }
}

#[derive(Subcommand)]
enum Cmd {
    #[clap(subcommand)]
    Node(NodeCmd),
    #[clap(subcommand)]
    Tool(ToolCmd),
}

impl Cmd {
    fn run(self) -> Result<()> {
        match self {
            Cmd::Node(cmd) => cmd.run(),
            Cmd::Tool(cmd) => cmd.run(),
        }
    }
}

/// Node commands
#[derive(Subcommand)]
enum NodeCmd {
    Run(node::CmdRun),
    Ping(node::control::CmdPing),
}

impl NodeCmd {
    fn run(self) -> Result<()> {
        match self {
            NodeCmd::Run(cmd) => cmd.run(),
            NodeCmd::Ping(cmd) => cmd.run(),
        }
    }
}

/// A collection of tools
#[derive(Subcommand)]
#[allow(clippy::enum_variant_names)]
enum ToolCmd {
    GenDht(tools::gen_dht::Cmd),
    GenKey(tools::gen_key::Cmd),
    GenZerostate(tools::gen_zerostate::Cmd),
    GenAccount(tools::gen_account::Cmd),
}

impl ToolCmd {
    fn run(self) -> Result<()> {
        match self {
            ToolCmd::GenDht(cmd) => cmd.run(),
            ToolCmd::GenKey(cmd) => cmd.run(),
            ToolCmd::GenZerostate(cmd) => cmd.run(),
            ToolCmd::GenAccount(cmd) => cmd.run(),
        }
    }
}

fn version_string() -> &'static str {
    static STRING: OnceLock<String> = OnceLock::new();
    STRING.get_or_init(|| {
        format!("(release {TYCHO_VERSION}) (build {TYCHO_BUILD}) (rustc {RUSTC_VERSION})")
    })
}

static TYCHO_VERSION: &str = env!("TYCHO_VERSION");
static TYCHO_BUILD: &str = env!("TYCHO_BUILD");
static RUSTC_VERSION: &str = env!("TYCHO_RUSTC_VERSION");
