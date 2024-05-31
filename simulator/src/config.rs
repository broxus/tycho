use std::path::PathBuf;

use anyhow::Result;
use tycho_util::project_root;

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub project_root: PathBuf,
    pub scratch_dir: PathBuf,
    pub node_port: u16,
}

impl ServiceConfig {
    pub fn new() -> Result<Self> {
        let project_root = project_root()?;
        let scratch_dir = project_root.join(".scratch");
        Ok(Self {
            project_root,
            scratch_dir,
            node_port: 30310,
        })
    }

    pub fn global_config_path(&self) -> PathBuf {
        self.scratch_dir.join("global-config.json")
    }

    pub fn helm_template_output(&self) -> PathBuf {
        self.scratch_dir.join("helm")
    }

    pub fn helm_template_path(&self) -> PathBuf {
        self.project_root
            .join("simulator")
            .join("helm")
            .join("tycho")
    }
}
