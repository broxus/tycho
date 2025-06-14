use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tycho_util::serde_helpers::load_json_from_file;

use crate::backend::{Helm, K3sDocker};
use crate::config::{BuilderValues, ClusterType, SimulatorConfig};

#[derive(Subcommand)]
pub enum BuildCommand {
    /// Docker builds image and k3s imports its
    Local,
    /// Helm installs `tycho_builder` chart
    Install(InstallCommand),
    /// Helm removes `tycho_builder` chart; `values` file is left intact
    Remove,
}

impl BuildCommand {
    pub fn run(self, config: &SimulatorConfig) -> Result<()> {
        match self {
            Self::Local => {
                if config.cluster_type == ClusterType::K3S {
                    let builder_values: BuilderValues =
                        load_json_from_file(&config.project_root.simulator.helm.builder.values)
                            .context("could not read `builder` chart values")?;
                    let tycho_image = builder_values.tycho_image;
                    let image_name = format!("{}:{}", tycho_image.repository, tycho_image.tag);
                    K3sDocker::build_upload(config, &image_name)?;
                } else {
                    anyhow::bail!("current kubectl config is set to {:?}", config.cluster_type);
                }
            }
            Self::Install(cmd) => Helm::upgrade_install(config, Helm::BUILDER_CHART, cmd.quiet)?,
            Self::Remove => Helm::uninstall(config)?,
        }
        Ok(())
    }
}

#[derive(Parser)]
pub struct InstallCommand {
    /// Quiet is the opposite to debug output, enabled by default
    #[clap(short, long, action)]
    pub quiet: bool,
}
