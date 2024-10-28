use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_types::cell::HashBytes;
use everscale_types::models::StdAddr;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tycho_core::global_config::GlobalConfig;
use tycho_util::FastHashMap;

use crate::node::{NodeConfig, NodeKeys};
use crate::util::{create_dir_all, print_json};
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
                SubCmd::Validator(cmd) => cmd.run(args),
                SubCmd::Systemd(cmd) => cmd.run(args),
            };
        }

        args.create_home_dir()?;

        // Create node keys if not exists
        let node_keys_path = args.node_keys_path(None);
        let node_keys_updated = !node_keys_path.exists();
        let node_keys_public =
            prepare_keys(&node_keys_path).context("failed to prepare node keys")?;

        // Create validator keys if not exists
        let mut validator_keys = None;
        if self.validator {
            let path = args.validator_keys_path(None);
            let updated = !path.exists();
            let (public, wallet) =
                prepare_keys_with_wallet(&path).context("failed to prepare validator keys")?;
            validator_keys = Some(ValidatorKeysInfo {
                wallet,
                public: HashBytes(public.to_bytes()),
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

            reqwest::get(path)
                .await
                .with_context(|| format!("failed to fetch a global config"))?
                .bytes()
                .await
                .context("failed to receive a global config")?
        };

        serde_json::from_slice::<GlobalConfig>(&global_config).context("invalid global config")?;

        let global_config_path = args.global_config_path(None);
        std::fs::write(&global_config_path, global_config)
            .context("failed to save global config")?;

        // Create systemd services if requested
        let mut systemd_services = None;
        if self.systemd {
            let systemd_dir = create_user_systemd_dir()?;

            let mut service_info = FastHashMap::default();

            let tycho_service_path = systemd_dir.join(format!("{TYCHO_SERVICE}.service"));
            let tycho_service_updated = !tycho_service_path.exists();
            if !tycho_service_path.exists() {
                let tycho_service = format!(
                    node_service!(),
                    work_dir = args.home.display(),
                    tycho_bin = self.binary.display(),
                );
                std::fs::write(&tycho_service_path, tycho_service)
                    .with_context(|| format!("failed to save {}", tycho_service_path.display()))?
            }

            service_info.insert(TYCHO_SERVICE, SystemdServiceInfo {
                updated: tycho_service_updated,
                path: tycho_service_path,
            });

            systemd_services = Some(service_info);
        }

        // Print
        print_json(serde_json::json!({
            "node_keys": {
                "public": node_keys_public,
                "path": node_keys_path,
                "updated": node_keys_updated,
            },
            "validator_keys": validator_keys,
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
    /// Overwrite existing keys (MAKE SURE TO MAKE A BACKUP FIRST).
    #[clap(long)]
    force: bool,
}

impl CmdInitValidator {
    fn run(self, args: BaseArgs) -> Result<()> {
        let path = args.validator_keys_path(None);
        if path.exists() && !self.force {
            anyhow::bail!("validator keys file already exists, use --force to overwrite");
        }

        args.create_home_dir()?;

        let (public, wallet) = save_keys_with_wallet(&generate_key(), &path)
            .context("failed to save validator keys")?;

        // Print
        print_json(serde_json::json!({
            "validator_keys": ValidatorKeysInfo {
                wallet,
                public: HashBytes(public.to_bytes()),
                path,
                updated: true,
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

    /// Overwrite the existing config.
    #[clap(short, long)]
    force: bool,
}

impl CmdInitSystemd {
    fn run(self, args: BaseArgs) -> Result<()> {
        let systemd_dir = create_user_systemd_dir()?;

        let tycho_service_path = systemd_dir.join(TYCHO_SERVICE);
        let tycho_service_updated = !tycho_service_path.exists();
        if !tycho_service_path.exists() {
            let tycho_service = format!(
                node_service!(),
                work_dir = args.home.display(),
                tycho_bin = self.binary.display(),
            );
            std::fs::write(&tycho_service_path, tycho_service)
                .with_context(|| format!("failed to save {}", tycho_service_path.display()))?
        }

        let mut service_info = FastHashMap::default();
        service_info.insert(TYCHO_SERVICE, SystemdServiceInfo {
            updated: tycho_service_updated,
            path: tycho_service_path,
        });

        // Print
        print_json(serde_json::json!({
            "systemd": service_info,
        }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ValidatorKeysInfo {
    wallet: StdAddr,
    public: HashBytes,
    path: PathBuf,
    updated: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SystemdServiceInfo {
    path: PathBuf,
    updated: bool,
}

fn prepare_keys<P: AsRef<Path>>(path: P) -> Result<ed25519::PublicKey> {
    if path.as_ref().exists() {
        let keys = NodeKeys::from_file(path)?;
        Ok(ed25519::PublicKey::from(&keys.as_secret()))
    } else {
        save_keys(&generate_key(), path)
    }
}

fn save_keys<P: AsRef<Path>>(secret: &ed25519::SecretKey, path: P) -> Result<ed25519::PublicKey> {
    let public = ed25519::PublicKey::from(secret);
    std::fs::write(
        path,
        serde_json::to_string_pretty(&serde_json::json!({
            "public": hex::encode(public.as_bytes()),
            "secret": hex::encode(secret.as_bytes()),
        }))?,
    )?;
    Ok(public)
}

fn prepare_keys_with_wallet<P: AsRef<Path>>(path: P) -> Result<(ed25519::PublicKey, StdAddr)> {
    #[derive(Debug, Clone, Deserialize)]
    struct NodeKeysPartial {
        secret: HashBytes,
        #[serde(default)]
        public: Option<HashBytes>,
        #[serde(default)]
        wallet: Option<StdAddr>,
    }

    if path.as_ref().exists() {
        let partial = tycho_util::serde_helpers::load_json_from_file::<NodeKeysPartial, _>(path)?;
        let secret = ed25519::SecretKey::from_bytes(*partial.secret.as_array());

        let public = ed25519::PublicKey::from(&secret);
        if let Some(stored_public) = partial.public {
            anyhow::ensure!(
                stored_public.as_array() == public.as_bytes(),
                "public key mismatch (stored: {stored_public}, expected: {public})",
            );
        }

        let wallet = validator_wallet(&public);
        if let Some(stored_wallet) = partial.wallet {
            anyhow::ensure!(
                stored_wallet == wallet,
                "wallet address mismatch (stored: {stored_wallet}, expected: {wallet})",
            );
        }

        Ok((public, wallet))
    } else {
        save_keys_with_wallet(&generate_key(), path)
    }
}

fn save_keys_with_wallet<P: AsRef<Path>>(
    secret: &ed25519::SecretKey,
    path: P,
) -> Result<(ed25519::PublicKey, StdAddr)> {
    let public = ed25519::PublicKey::from(secret);
    let wallet = validator_wallet(&public);
    std::fs::write(
        path,
        serde_json::to_string_pretty(&serde_json::json!({
            "public": hex::encode(public.as_bytes()),
            "secret": hex::encode(secret.as_bytes()),
            "wallet": wallet,
        }))?,
    )?;
    Ok((public, wallet))
}

fn generate_key() -> ed25519::SecretKey {
    ed25519::SecretKey::from_bytes(rand::thread_rng().gen())
}

fn validator_wallet(public: &ed25519::PublicKey) -> StdAddr {
    crate::util::wallet::compute_address(-1, public)
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
