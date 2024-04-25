use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::compose::{ComposeRunner, Service};
use crate::config::ServiceConfig;
use crate::node::{Node, NodeOptions};

pub(crate) struct Simulator {
    config: ServiceConfig,
    compose: ComposeRunner,
    global_config: GlobalConfig,
    next_node_ip: Ipv4Addr,
}

impl Simulator {
    pub fn new(config: ServiceConfig) -> Result<Self> {
        let compose = ComposeRunner::new(&config)?;
        let global_config = GlobalConfig::new(&config)?;
        let next_node_ip: Ipv4Addr = config
            .network_subnet
            .split('/')
            .next()
            .context("no subnet")?
            .parse()
            .context("invalid ip")?;
        let next_node_ip = increment_ip(next_node_ip, global_config.bootstrap_peers.len() + 1);

        Ok(Self {
            config,
            compose,
            global_config,
            next_node_ip,
        })
    }

    pub fn prepare(&mut self, nodes: usize) -> Result<()> {
        for node_index in 0..nodes {
            self.add_node(node_index, None, None)?;
        }

        self.add_grafana()?;
        self.add_prometheus()?;

        self.finalize()?;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<()> {
        let global_config_path = self.config.global_config_path();
        std::fs::write(
            global_config_path,
            serde_json::to_string(&self.global_config)?,
        )?;
        self.compose.finalize()?;
        Ok(())
    }


    // updates the next_node_ip and adds a new node to the network
    pub fn add_node(
        &mut self,
        node_index: usize,
        delay: Option<u16>,
        loss: Option<u16>,
    ) -> Result<()> {
        let node_ip = increment_ip(self.next_node_ip, 1);

        let options = match (delay, loss) {
            (Some(delay), Some(loss)) => Some(NodeOptions {
                delay,
                packet_loss: loss,
            }),
            (Some(delay), None) => Some(NodeOptions {
                delay,
                packet_loss: 0,
            }),
            (None, Some(loss)) => Some(NodeOptions {
                delay: 0,
                packet_loss: loss,
            }),
            (None, None) => None,
        };

        let mut node = Node::init_from_cli(node_ip, self.config.node_port, node_index, options)
            .with_context(|| format!("failed to init node-{node_index}"))?;
        let service = node.as_service(&self.config)?;

        self.global_config
            .bootstrap_peers
            .push(node.dht_value.take());
        self.compose
            .add_service(format!("node-{}", node_index), service)?;

        let logs_dir = node.logs_dir(&self.config);
        println!("Creating {:?}", logs_dir);
        std::fs::create_dir_all(&logs_dir)?;

        self.write_run_data(node)?;
        self.next_node_ip = node_ip;
        Ok(())
    }

    pub fn add_grafana(&mut self) -> Result<()> {
        self.compose.add_grafana()
    }

    pub fn add_prometheus(&mut self) -> Result<()> {
        self.compose.add_prometheus()
    }

    pub fn next_node_index(&self) -> usize {
        self.global_config.bootstrap_peers.len()
    }

    fn write_run_data(&self, node: Node) -> Result<()> {
        let entrypoint_data = generate_entrypoint(node.run_command());
        let entrypoint_path = node.entrypoint_path(&self.config);
        let options_path = node.options_path(&self.config);

        println!("Writing entrypoint to {:?}", entrypoint_path);

        std::fs::create_dir_all(self.config.entrypoints())?;
        std::fs::create_dir_all(self.config.options())?;

        std::fs::write(&entrypoint_path, entrypoint_data)
            .context("Failed to write entrypoint data")?;
        std::fs::set_permissions(entrypoint_path, std::fs::Permissions::from_mode(0o755))
            .context("Failed to set entrypoint permissions")?;

        println!("Writing persistent options json file");
        let data = match node.options {
            Some(options) => serde_json::to_string(&options)?,
            None => serde_json::to_string(&NodeOptions::default())?,
        };

        std::fs::write(&options_path, data).context("Failed to write node options")?;
        std::fs::set_permissions(options_path, std::fs::Permissions::from_mode(0o755))
            .context("Failed to set node options permissions")?;

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

fn generate_entrypoint(params: String) -> String {
    format!(
        r#"#!/bin/bash
export RUST_LOG="info,tycho_network=trace"
export TYCHO_PERSISTENT_LOGS=true
cd /app
/app/network-node {params}
"#
    )
}

fn increment_ip(ip: Ipv4Addr, by: usize) -> Ipv4Addr {
    let mut octets = ip.octets();
    octets[3] += by as u8;
    Ipv4Addr::from(octets)
}
