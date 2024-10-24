#![allow(clippy::print_stdout, clippy::print_stderr, clippy::exit)] // it's a CLI tool

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::OnceLock;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

#[cfg(feature = "debug")]
mod debug;
mod init;
mod node;
mod tools;
mod util;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> ExitCode {
    if std::env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        // Disable backtraces in libraries by default
        std::env::set_var("RUST_LIB_BACKTRACE", "0");
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

    #[clap(flatten)]
    args: BaseArgs,
}

impl App {
    fn run(self) -> Result<()> {
        self.cmd.run(self.args)
    }
}

#[derive(Subcommand)]
enum Cmd {
    Init(init::Cmd),
    Node(node::Cmd),
    Tool(tools::Cmd),
    #[cfg(feature = "debug")]
    Debug(debug::Cmd),
}

impl Cmd {
    fn run(self, args: BaseArgs) -> Result<()> {
        match self {
            Cmd::Init(cmd) => cmd.run(args),
            Cmd::Node(cmd) => cmd.run(args),
            Cmd::Tool(cmd) => cmd.run(),
            #[cfg(feature = "debug")]
            Cmd::Debug(cmd) => cmd.run(),
        }
    }
}

#[derive(Args)]
pub struct BaseArgs {
    /// Directory for config and keys.
    #[clap(long, value_parser, default_value_os = default_home_dir().as_os_str())]
    home: PathBuf,
}

fn version_string() -> &'static str {
    static STRING: OnceLock<String> = OnceLock::new();
    STRING.get_or_init(|| {
        format!("(release {TYCHO_VERSION}) (build {TYCHO_BUILD}) (rustc {RUSTC_VERSION})")
    })
}

fn default_home_dir() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        if let Ok(dir) = std::env::var("TYCHO_HOME") {
            return dir.into();
        }

        if let Some(mut dir) = dirs::home_dir() {
            dir.push(".tycho");
            return dir;
        }

        PathBuf::default()
    })
}

static TYCHO_VERSION: &str = env!("TYCHO_VERSION");
static TYCHO_BUILD: &str = env!("TYCHO_BUILD");
static RUSTC_VERSION: &str = env!("TYCHO_RUSTC_VERSION");
