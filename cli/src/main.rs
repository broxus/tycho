#![allow(clippy::print_stdout, clippy::print_stderr, clippy::exit)]
#[doc(hidden)]
#[allow(dead_code)]
mod __async_profile_guard__ {
    use std::time::{Duration, Instant};
    const THRESHOLD_MS: u64 = 10u64;
    pub struct Guard {
        name: &'static str,
        file: &'static str,
        from_line: u32,
        current_start: Option<Instant>,
        consecutive_hits: u32,
    }
    impl Guard {
        pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
            Guard {
                name,
                file,
                from_line: line,
                current_start: Some(Instant::now()),
                consecutive_hits: 0,
            }
        }
        pub fn checkpoint(&mut self, new_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = new_line
                    );
                    let wraparound = new_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail)"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
        pub fn end_section(&mut self, to_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = to_line
                    );
                    let wraparound = to_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (loop wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
        }
        pub fn start_section(&mut self, new_line: u32) {
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(start) = self.current_start {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{line}-{line}", file = self.file, line = self.from_line
                    );
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, span = %
                        span, hits = self.consecutive_hits, wraparound = false,
                        "long poll"
                    );
                }
            }
        }
    }
}
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::OnceLock;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
mod cmd {
    #[cfg(feature = "debug")]
    pub mod debug;
    pub mod elect;
    pub mod init;
    pub mod node;
    pub mod tools;
    pub mod util;
}
mod node;
mod util;
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
fn main() -> ExitCode {
    if std::env::var("RUST_BACKTRACE").is_err() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        unsafe { std::env::set_var("RUST_LIB_BACKTRACE", "0") };
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
    Util(cmd::util::Cmd),
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
            Cmd::Util(cmd) => cmd.run(),
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
        util::create_dir_all(&self.home)?;
        Ok(self)
    }
    pub fn node_config_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite.cloned().unwrap_or_else(|| self.home.join("config.json"))
    }
    pub fn node_keys_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite.cloned().unwrap_or_else(|| self.home.join("node_keys.json"))
    }
    pub fn elections_config_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite.cloned().unwrap_or_else(|| self.home.join("elections.json"))
    }
    pub fn global_config_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite.cloned().unwrap_or_else(|| self.home.join("global-config.json"))
    }
    pub fn control_socket_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        match overwrite {
            Some(path) => path.clone(),
            None => {
                match std::env::var("TYCHO_CONTROL_SOCK") {
                    Ok(sock) => PathBuf::from(sock),
                    Err(_) => self.home.join("control.sock"),
                }
            }
        }
    }
    pub fn wu_tuner_config_path(&self, overwrite: Option<&PathBuf>) -> PathBuf {
        overwrite.cloned().unwrap_or_else(|| self.home.join("wu-tuner-config.json"))
    }
}
fn version_string() -> &'static str {
    static STRING: OnceLock<String> = OnceLock::new();
    STRING
        .get_or_init(|| {
            format!(
                "(release {TYCHO_VERSION}) (build {TYCHO_BUILD}) (rustc {RUSTC_VERSION})"
            )
        })
}
fn default_home_dir() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        if std::env::var("CI").is_ok() {
            return PathBuf::from("/tmp/.tycho-ci");
        }
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
