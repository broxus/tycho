use serde::{Deserialize, Serialize};

use crate::backend::KubeCtl;
use crate::config::project_root::ProjectRoot;

#[derive(Debug)]
pub struct SimulatorConfig {
    pub helm_release: String,
    pub project_root: ProjectRoot,
    pub cluster_type: ClusterType,
    pub pod: PodConfig,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClusterType {
    K3S,
    Gke,
}

#[derive(Debug)]
pub struct PodConfig {
    pub node_port: u16,
    pub metrics_port: u16,
    pub db_path: String,
}

impl PodConfig {
    pub fn name(pod_index: usize) -> String {
        format!("tycho-{pod_index}")
    }
}

impl SimulatorConfig {
    pub fn new() -> anyhow::Result<Self> {
        let project_root = ProjectRoot::new()?;
        let cluster_type = KubeCtl::get_cluster_type()?;

        println!("kubectl context is {cluster_type:?}\n");

        Ok(Self {
            helm_release: "tycho-simulated".to_string(),
            project_root,
            cluster_type,
            pod: PodConfig {
                node_port: 30310,
                metrics_port: 9090,
                db_path: "/app/db".to_string(),
            },
        })
    }
}
