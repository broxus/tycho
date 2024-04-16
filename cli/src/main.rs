use std::sync::OnceLock;

use clap::{Parser, Subcommand};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    if std::env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    App::parse().run();
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
    fn run(self) {}
}

#[derive(Subcommand)]
enum Cmd {
    Init(InitCmd),
    Run(RunCmd),
}

/// Initialize a node environment
#[derive(Parser)]
struct InitCmd {}

/// Run a node
#[derive(Parser)]
struct RunCmd {}

fn version_string() -> &'static str {
    static STRING: OnceLock<String> = OnceLock::new();
    STRING.get_or_init(|| {
        format!("(release {TYCHO_VERSION}) (build {TYCHO_BUILD}) (rustc {RUSTC_VERSION})")
    })
}

static TYCHO_VERSION: &str = env!("TYCHO_VERSION");
static TYCHO_BUILD: &str = env!("TYCHO_BUILD");
static RUSTC_VERSION: &str = env!("TYCHO_RUSTC_VERSION");
