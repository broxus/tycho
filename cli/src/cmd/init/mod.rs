use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use clap::Parser;

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
ExecStart={tycho_bin} node run \
    --mempool-start-round 0 \
    --keys keys.json \
    --config config.json \
    --global-config global-config.json \
    --logger-config logger.json
Environment=RUST_BACKTRACE=1,RUST_LIB_BACKTRACE=0

[Install]
WantedBy=multi-user.target
"#
    };
}

/// Create a node environment or reinitialize an existing one.
#[derive(Parser)]
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
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        args.create_home_dir()?;

        if self.systemd {
            let Some(user_home) = dirs::home_dir() else {
                anyhow::bail!("failed to get user home directory");
            };

            let systemd_dir = user_home.join(".config/systemd/user");
            create_dir_all(&systemd_dir)?;

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

fn default_binary_path() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        if let Ok(path) = std::env::current_exe() {
            return path;
        }

        PathBuf::default()
    })
}
