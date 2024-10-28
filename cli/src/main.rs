#![allow(clippy::print_stdout, clippy::print_stderr, clippy::exit)] // it's a CLI tool

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tycho_control::ControlClient;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;

mod cmd {
    #[cfg(feature = "debug")]
    pub mod debug;
    pub mod elect;
    pub mod init;
    pub mod node;
    pub mod tools;
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
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        // Disable backtraces in libraries by default
        std::env::set_var("RUST_LIB_BACKTRACE", "0");
    }

    match App::parse().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err:?}");
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
    Init(cmd::init::Cmd),
    Node(cmd::node::Cmd),
    Tool(cmd::tools::Cmd),
    Elect(cmd::elect::Cmd),
    #[cfg(feature = "debug")]
    Debug(cmd::debug::Cmd),
}

impl Cmd {
    fn run(self, args: BaseArgs) -> Result<()> {
        match self {
            Cmd::Init(cmd) => cmd.run(args),
            Cmd::Node(cmd) => cmd.run(args),
            Cmd::Tool(cmd) => cmd.run(),
            Cmd::Elect(cmd) => cmd.run(args),
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

impl BaseArgs {
    pub fn create_home_dir(&self) -> Result<&Self> {
        crate::util::create_dir_all(&self.home)?;
        Ok(self)
    }

    pub fn node_config_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite
            .cloned()
            .unwrap_or_else(|| self.home.join("config.json"))
    }

    pub fn node_keys_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite
            .cloned()
            .unwrap_or_else(|| self.home.join("node_keys.json"))
    }

    pub fn validator_keys_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite
            .cloned()
            .unwrap_or_else(|| self.home.join("validator_keys.json"))
    }

    pub fn global_config_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite
            .cloned()
            .unwrap_or_else(|| self.home.join("global-config.json"))
    }

    pub fn control_socket_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        match overwrite {
            Some(path) => path.clone(),
            None => match std::env::var("TYCHO_CONTROL_SOCK") {
                Ok(sock) => PathBuf::from(sock),
                Err(_) => self.home.join("control.sock"),
            },
        }
    }
}

#[derive(Clone, Args)]
struct ControlArgs {
    /// Path to the control socket. Default: `$TYCHO_HOME/control.sock`
    #[clap(long)]
    control_socket: Option<PathBuf>,
}

impl ControlArgs {
    fn rt<F, FT>(&self, args: BaseArgs, f: F) -> Result<()>
    where
        F: FnOnce(ControlClient) -> FT + Send + 'static,
        FT: Future<Output = Result<()>> + Send,
    {
        tracing_subscriber::fmt::init();

        let sock = args.control_socket_path(self.control_socket.as_ref());

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let run_fut = JoinTask::new(async move {
                    let client = ControlClient::connect(sock)
                        .await
                        .context("failed to connect to control server")?;
                    f(client).await
                });
                let stop_fut = signal::any_signal(signal::TERMINATION_SIGNALS);
                tokio::select! {
                    res = run_fut => res,
                    signal = stop_fut => match signal {
                        Ok(signal) => {
                            tracing::info!(?signal, "received termination signal");
                            Ok(())
                        }
                        Err(e) => Err(e.into()),
                    }
                }
            })
    }
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
