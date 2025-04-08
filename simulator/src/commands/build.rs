use std::process::Command;

use anyhow::Result;
use clap::Parser;

use crate::config::SimulatorConfig;
use crate::helm::{ClusterType, HelmRunner};

#[derive(Parser)]
pub struct BuildCommand {
    #[clap(short, long)]
    cluster_type: Option<ClusterType>,
}

impl BuildCommand {
    pub fn run(self, config: &SimulatorConfig) -> Result<()> {
        println!("Building docker image");

        Command::new("docker")
            .arg("build")
            .arg("-t")
            .arg(&config.pod.image_name)
            .arg("-f")
            .arg(&config.project_root.dockerfile)
            .arg(&config.project_root.dir)
            .env("DOCKER_BUILDKIT", "1")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()?
            .wait()?;
        HelmRunner::upload_image(config, self.cluster_type.unwrap_or_default())?;

        Ok(())
    }
}
