use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

pub struct ServiceConfig {
    pub project_root: PathBuf,
    pub scratch_dir: PathBuf,
    pub network_subnet: String,
    pub node_port: u16,
}

impl ServiceConfig {
    pub fn new(network_subnet: String) -> Result<Self> {
        let project_root = Command::new("git")
            .arg("rev-parse")
            .arg("--show-toplevel")
            .output()?
            .stdout;
        // won't work on windows but we don't care
        let project_root = PathBuf::from(
            String::from_utf8(project_root)
                .context("Invalid project root")?
                .trim(),
        );
        let scratch_dir = project_root.join(".scratch");
        Ok(Self {
            project_root,
            scratch_dir,
            network_subnet,
            node_port: 25565,
        })
    }

    pub fn global_config_path(&self) -> PathBuf {
        self.scratch_dir.join("global-config.json")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.scratch_dir.join("logs")
    }

    pub fn compose_path(&self) -> PathBuf {
        self.scratch_dir.join("docker-compose.json")
    }

    pub fn entrypoints(&self) -> PathBuf {
        self.scratch_dir.join("entrypoints")
    }
}
