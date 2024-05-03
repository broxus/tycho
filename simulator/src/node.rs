use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::compose::{Service, ServiceNetwork};
use crate::config::ServiceConfig;

pub struct Node {
    pub index: usize,
    pub ip: Ipv4Addr,
    pub port: u16,
    pub dht_value: serde_json::Value,
    pub key: String,
    pub options: Option<NodeOptions>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct NodeOptions {
    pub delay: u16,
    pub packet_loss: u16,
}

impl Node {
    pub fn init_from_cli(
        ip: Ipv4Addr,
        port: u16,
        index: usize,
        options: Option<NodeOptions>,
    ) -> Result<Self> {
        let private_key = hex::encode(rand::random::<[u8; 32]>());
        let output = Command::new("cargo")
            .arg("run")
            .arg("--example")
            .arg("network-node")
            .arg("--")
            .arg("gen-dht")
            .arg(format!("{ip}:{port}"))
            .arg("--key")
            .arg(&private_key)
            .output()
            .with_context(|| "failed to execute process")?;
        if !output.status.success() {
            anyhow::bail!(
                "failed to execute process: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let dht_value = serde_json::from_slice(&output.stdout)?;
        Ok(Self {
            index,
            ip,
            dht_value,
            port,
            key: private_key,
            options,
        })
    }

    pub fn as_service(&self, service_config: &ServiceConfig) -> Result<Service> {
        let volumes = vec![
            format!(
                "./entrypoints/node-{}_entrypoint.sh:/entrypoints/entrypoint.sh",
                self.index
            ),
            format!(
                "{}:/app/global-config.json",
                service_config.global_config_path().to_string_lossy()
            ),
            format!(
                "./options/node-{}_options.json:/options/options.json",
                self.index
            ),
            format!(
                "{}:/app/logs:rw",
                self.logs_dir(service_config).to_string_lossy()
            ),
        ];

        let entrypoint = "/entrypoints/entrypoint.sh".to_string();

        Ok(Service {
            entrypoint,
            image: "tycho-network".to_string(),
            networks: HashMap::from([(
                "default".to_string(),
                ServiceNetwork {
                    ipv4_address: Some(self.ip.to_string()),
                },
            )]),
            stop_grace_period: "1s".to_string(),
            stop_signal: "KILL".to_string(),
            volumes,
            privileged: true,
        })
    }

    pub fn logs_dir(&self, service_config: &ServiceConfig) -> PathBuf {
        service_config
            .logs_dir()
            .join(format!("node-{}", self.index))
    }

    pub fn entrypoint_path(&self, service_config: &ServiceConfig) -> PathBuf {
        service_config
            .entrypoints()
            .join(format!("node-{}_entrypoint.sh", self.index))
    }

    pub fn options_path(&self, service_config: &ServiceConfig) -> PathBuf {
        service_config
            .options()
            .join(format!("node-{}_options.json", self.index))
    }

    pub fn run_command(&self) -> String {
        format!(
            "run {ip}:{node_port} --key {key} --global-config /app/global-config.json",
            ip = self.ip,
            node_port = self.port,
            key = self.key
        )
    }
}
