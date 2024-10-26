use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_crypto::ed25519;
use rand::Rng;
use tycho_core::global_config::GlobalConfig;

use crate::node::NodeConfig;
use crate::util::create_dir_all;
use crate::BaseArgs;

const TYCHO_SERVICE: &str = "tycho.service";

macro_rules! node_service {
    () => {
        r#"[Unit]
Description=Tycho Node
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory={work_dir}
ExecStart={tycho_bin} node run --mempool-start-round 0
Environment=RUST_BACKTRACE=1,RUST_LIB_BACKTRACE=0

[Install]
WantedBy=multi-user.target
"#
    };
}

/// Create a node environment or reinitialize an existing one.
#[derive(Parser)]
#[command(subcommand_negates_reqs = true)]
pub struct Cmd {
    /// Path to the `tycho` binary.
    #[clap(short, long, value_parser, default_value_os = default_binary_path().as_os_str())]
    binary: PathBuf,

    /// Whether to init as a validator.
    #[clap(long)]
    validator: bool,

    /// Whether to create a systemd services.
    #[clap(long)]
    systemd: bool,

    /// Path or URL of the global config.
    #[clap(short, long, required = true)]
    global_config: Option<String>,

    #[clap(subcommand)]
    cmd: Option<SubCmd>,
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(self.run_impl(args))
    }

    async fn run_impl(self, args: BaseArgs) -> Result<()> {
        if let Some(cmd) = self.cmd {
            return match cmd {
                SubCmd::Config(cmd) => cmd.run(args),
                SubCmd::Systemd(cmd) => cmd.run(args),
            };
        }

        args.create_home_dir()?;

        // Create node keys if not exists
        let node_keys_path = args.node_keys_path(None);
        if !node_keys_path.exists() {
            let secret = ed25519::SecretKey::from_bytes(rand::thread_rng().gen());
            let public = ed25519::PublicKey::from(&secret);
            std::fs::write(
                node_keys_path,
                serde_json::to_string_pretty(&serde_json::json!({
                    "public": hex::encode(public.as_bytes()),
                    "secret": hex::encode(secret.as_bytes()),
                }))?,
            )
            .context("failed to save node keys")?;
        }

        // Create config if not exists
        let config_path = args.node_config_path(None);
        if !config_path.exists() {
            NodeConfig::default()
                .save_to_file(config_path)
                .context("failed to save node config")?;
        }

        // Download global config
        let global_config = 'config: {
            let path = self.global_config.unwrap();
            if let Ok(meta) = Path::new(&path).metadata() {
                if meta.is_file() {
                    break 'config std::fs::read(path)?.into();
                }
            }

            reqwest::get(path)
                .await
                .with_context(|| format!("failed to fetch a global config"))?
                .bytes()
                .await
                .context("failed to receive a global config")?
        };

        serde_json::from_slice::<GlobalConfig>(&global_config).context("invalid global config")?;

        std::fs::write(args.global_config_path(None), global_config)
            .context("failed to save global config")?;

        // Create systemd services if requested
        if self.systemd {
            let systemd_dir = create_user_systemd_dir()?;

            let tycho_service_file = systemd_dir.join(TYCHO_SERVICE);
            if !tycho_service_file.exists() {
                let tycho_service = format!(
                    node_service!(),
                    work_dir = args.home.display(),
                    tycho_bin = self.binary.display(),
                );
                std::fs::write(&tycho_service_file, tycho_service)
                    .with_context(|| format!("failed to save {}", tycho_service_file.display()))?
            }
        }

        Ok(())
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Config(CmdInitConfig),
    Systemd(CmdSystemd),
}

/// Generate a default node config.
#[derive(Parser)]
struct CmdInitConfig {
    /// Custom path to the output file. Default: `$TYCHO_HOME/config.json`.
    output: Option<PathBuf>,

    /// Overwrite the existing config.
    #[clap(short, long)]
    force: bool,
}

impl CmdInitConfig {
    fn run(self, args: BaseArgs) -> Result<()> {
        let config_path = args.node_config_path(self.output.as_ref());
        if config_path.exists() && !self.force {
            anyhow::bail!("config file already exists, use --force to overwrite");
        }

        if self.output.is_none() {
            args.create_home_dir()?;
        }

        NodeConfig::default()
            .save_to_file(config_path)
            .context("failed to save node config")
    }
}

/// Generate systemd services.
#[derive(Parser)]
struct CmdSystemd {
    /// Path to the `tycho` binary.
    #[clap(short, long, value_parser, default_value_os = default_binary_path().as_os_str())]
    binary: PathBuf,

    /// Overwrite the existing config.
    #[clap(short, long)]
    force: bool,
}

impl CmdSystemd {
    fn run(self, args: BaseArgs) -> Result<()> {
        let systemd_dir = create_user_systemd_dir()?;

        let tycho_service_file = systemd_dir.join(TYCHO_SERVICE);
        if !tycho_service_file.exists() {
            let tycho_service = format!(
                node_service!(),
                work_dir = args.home.display(),
                tycho_bin = self.binary.display(),
            );
            std::fs::write(&tycho_service_file, tycho_service)
                .with_context(|| format!("failed to save {}", tycho_service_file.display()))?
        }

        Ok(())
    }
}

fn default_binary_path() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        if let Ok(path) = std::env::current_exe() {
            return path;
        }

        PathBuf::default()
    })
}

fn create_user_systemd_dir() -> Result<PathBuf> {
    let Some(user_home) = dirs::home_dir() else {
        anyhow::bail!("failed to get user home directory");
    };

    let systemd_dir = user_home.join(".config/systemd/user");
    create_dir_all(&systemd_dir)?;

    Ok(systemd_dir)
}
