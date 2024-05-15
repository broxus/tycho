use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::ServiceConfig;
use crate::helm::{ClusterType, HelmConfig};
use crate::node::Node;

pub(crate) struct Simulator {
    config: ServiceConfig,
    helm_config: HelmConfig,
    global_config: GlobalConfig,
}

impl Simulator {
    pub fn new(config: ServiceConfig, _cluster_ty: ClusterType) -> Result<Self> {
        // todo add support for different cluster types
        let helm_config = HelmConfig::new(&config);
        let global_config = GlobalConfig::new(&config)?;

        Ok(Self {
            config,
            helm_config,
            global_config,
        })
    }

    pub fn prepare(&mut self, nodes: usize) -> Result<()> {
        for node_index in 0..nodes {
            self.add_node(node_index)?;
        }

        self.finalize()?;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<()> {
        self.helm_config
            .finalize(serde_json::to_string(&self.global_config)?)?;
        Ok(())
    }

    // updates the next_node_ip and adds a new node to the network
    pub fn add_node(&mut self, node_index: usize) -> Result<()> {
        let mut node = Node::init_from_cli(node_index, self.config.node_port)
            .with_context(|| format!("failed to init node-{node_index}"))?;
        self.global_config
            .bootstrap_peers
            .push(node.dht_value.take());
        self.helm_config.add_node(&node.key);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobalConfig {
    bootstrap_peers: Vec<serde_json::Value>,
}

impl GlobalConfig {
    pub fn new(config: &ServiceConfig) -> Result<Self> {
        let global_config_path = config.global_config_path();
        if std::fs::metadata(&global_config_path).is_ok() {
            return serde_json::from_reader(&mut std::fs::File::open(global_config_path)?)
                .context("Failed to read global config");
        }

        Ok(Self {
            bootstrap_peers: Vec::new(),
        })
    }
}
