use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_types::cell::HashBytes;
use everscale_types::models::StdAddr;
use rand::Rng;
use serde::Serialize;
use tycho_core::global_config::GlobalConfig;
use tycho_util::FastHashMap;

use crate::node::{ElectionsConfig, NodeConfig, NodeKeys, SimpleElectionsConfig};
use crate::util::{create_dir_all, print_json, FpTokens};
use crate::BaseArgs;

const TYCHO_SERVICE: &str = "tycho";
macro_rules! node_service {
    () => {
        r#"[Unit]
Description=Tycho Node
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory={work_dir}
ExecStart={tycho_bin} node run
Environment=RUST_BACKTRACE=1,RUST_LIB_BACKTRACE=0

[Install]
WantedBy=multi-user.target
"#
    };
}

const TYCHO_ELECT_SERVICE: &str = "tycho-elect";
macro_rules! elect_service {
    () => {
        r#"[Unit]
Description=Tycho Node Elections
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory={work_dir}
ExecStart={tycho_bin} elect run
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
    #[clap(long, requires = "stake")]
    validator: bool,

    /// Validator stake per round.
    #[clap(long)]
    stake: Option<FpTokens>,

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
                SubCmd::Validator(cmd) => cmd.run(args),
                SubCmd::Systemd(cmd) => cmd.run(args),
            };
        }

        args.create_home_dir()?;

        // Create node keys if not exists
        let node_keys_path = args.node_keys_path(None);
        let (node_keys_updated, node_keys_public) =
            prepare_node_keys(&node_keys_path).context("failed to prepare node keys")?;

        // Create validator keys if not exists
        let mut elections_config = None;
        if self.validator {
            let stake = self.stake.unwrap();

            let path = args.elections_config_path(None);
            let (updated, simple) = prepare_elections_config(&path, stake)
                .context("failed to prepare elections config")?;
            let public = simple.public_key();

            elections_config = Some(ElectionsConfigInfo {
                wallet: simple.wallet_address,
                public: HashBytes(public.to_bytes()),
                stake,
                path,
                updated,
            });
        }

        // Create config if not exists
        let config_path = args.node_config_path(None);
        let config_updated = !config_path.exists();
        if config_path.exists() {
            NodeConfig::from_file(&config_path).context("invalid node config")?;
        } else {
            NodeConfig::default()
                .save_to_file(&config_path)
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

            reqwest::get(&path)
                .await
                .with_context(|| format!("failed to fetch a global config. Path: {path}"))?
                .bytes()
                .await
                .context("failed to receive a global config")?
        };

        serde_json::from_slice::<GlobalConfig>(&global_config).context("invalid global config")?;

        let global_config_path = args.global_config_path(None);
        std::fs::write(&global_config_path, global_config)
            .context("failed to save global config")?;

        // Create systemd services if requested
        let systemd_services = self
            .systemd
            .then(|| prepare_systemd_services(&args.home, &self.binary))
            .transpose()
            .context("failed to prepare systemd services")?;

        // Print
        print_json(serde_json::json!({
            "node_keys": {
                "public": node_keys_public,
                "path": node_keys_path,
                "updated": node_keys_updated,
            },
            "elections": elections_config,
            "node_config": {
                "path": config_path,
                "updated": config_updated,
            },
            "global_config": {
                "path": global_config_path,
                "updated": true,
            },
            "systemd": systemd_services,
        }))
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Config(CmdInitConfig),
    Validator(CmdInitValidator),
    Systemd(CmdInitSystemd),
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
            .save_to_file(&config_path)
            .context("failed to save node config")?;

        // Print
        print_json(serde_json::json!({
            "node_config": {
                "path": config_path,
                "updated": true,
            },
        }))
    }
}

/// Generate validator keys and wallet.
#[derive(Parser)]
struct CmdInitValidator {
    /// Validator stake per round.
    #[clap(long)]
    stake: FpTokens,
}

impl CmdInitValidator {
    fn run(self, args: BaseArgs) -> Result<()> {
        let path = args.elections_config_path(None);
        args.create_home_dir()?;

        let (updated, simple) = prepare_elections_config(&path, self.stake)
            .context("failed to prepare elections config")?;
        let public = simple.public_key();

        print_json(serde_json::json!({
            "elections_config": ElectionsConfigInfo {
                wallet: simple.wallet_address,
                public: HashBytes(public.to_bytes()),
                stake: self.stake,
                path,
                updated,
            },
        }))
    }
}

/// Generate systemd services.
#[derive(Parser)]
struct CmdInitSystemd {
    /// Path to the `tycho` binary.
    #[clap(short, long, value_parser, default_value_os = default_binary_path().as_os_str())]
    binary: PathBuf,
}

impl CmdInitSystemd {
    fn run(self, args: BaseArgs) -> Result<()> {
        let service_info = prepare_systemd_services(&args.home, &self.binary)
            .context("failed to prepare systemd services")?;

        print_json(serde_json::json!({
            "systemd": service_info,
        }))
    }
}

#[derive(Debug, Serialize)]
struct ElectionsConfigInfo {
    wallet: StdAddr,
    public: HashBytes,
    stake: FpTokens,
    path: PathBuf,
    updated: bool,
}

#[derive(Debug, Serialize)]
struct SystemdServiceInfo {
    path: PathBuf,
    updated: bool,
}

fn prepare_systemd_services(
    home_dir: &Path,
    binary: &Path,
) -> Result<FastHashMap<&'static str, SystemdServiceInfo>> {
    let systemd_dir = create_user_systemd_dir()?;

    let service_file = |name: &str| format!("{name}.service");

    let mut service_info = FastHashMap::default();

    let node_service_path = systemd_dir.join(service_file(TYCHO_SERVICE));
    let node_service_updated = !node_service_path.exists();
    if !node_service_path.exists() {
        let node_service = format!(
            node_service!(),
            work_dir = home_dir.display(),
            tycho_bin = binary.display(),
        );
        std::fs::write(&node_service_path, node_service)
            .with_context(|| format!("failed to save {}", node_service_path.display()))?;
    }
    service_info.insert(TYCHO_SERVICE, SystemdServiceInfo {
        updated: node_service_updated,
        path: node_service_path,
    });

    let elect_service_path = systemd_dir.join(service_file(TYCHO_ELECT_SERVICE));
    let elect_service_updated = !elect_service_path.exists();
    if !elect_service_path.exists() {
        let elect_service = format!(
            elect_service!(),
            work_dir = home_dir.display(),
            tycho_bin = binary.display(),
        );
        std::fs::write(&elect_service_path, elect_service)
            .with_context(|| format!("failed to save {}", elect_service_path.display()))?;
    }
    service_info.insert(TYCHO_ELECT_SERVICE, SystemdServiceInfo {
        updated: elect_service_updated,
        path: elect_service_path,
    });

    Ok(service_info)
}

fn prepare_node_keys<P: AsRef<Path>>(path: P) -> Result<(bool, ed25519::PublicKey)> {
    if path.as_ref().exists() {
        let keys = NodeKeys::from_file(path)?;
        Ok((false, ed25519::PublicKey::from(&keys.as_secret())))
    } else {
        let keys = rand::random::<NodeKeys>();
        keys.save_to_file(path)?;

        Ok((true, ed25519::PublicKey::from(&keys.as_secret())))
    }
}

fn prepare_elections_config<P: AsRef<Path>>(
    path: P,
    stake: FpTokens,
) -> Result<(bool, SimpleElectionsConfig)> {
    let mut updated = false;
    let mut simple = if path.as_ref().exists() {
        match tycho_util::serde_helpers::load_json_from_file::<ElectionsConfig, _>(&path)? {
            ElectionsConfig::Simple(config) => config,
        }
    } else {
        updated = true;
        SimpleElectionsConfig::from_key(&generate_key(), Some(stake), None)
    };

    updated |= simple.stake != Some(stake);
    simple.stake = Some(stake);

    let data = serde_json::to_string_pretty(&ElectionsConfig::Simple(simple.clone()))?;
    std::fs::write(path, data)?;

    Ok((updated, simple))
}

fn generate_key() -> ed25519::SecretKey {
    ed25519::SecretKey::from_bytes(rand::thread_rng().gen())
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
