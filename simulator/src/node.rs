use std::process::Command;

use anyhow::{Context, Result};

pub struct Node {
    pub index: usize,
    pub hostname: String,
    pub port: u16,
    pub dht_value: serde_json::Value,
    pub key: String,
}

impl Node {
    pub fn init_from_cli(index: usize, port: u16) -> Result<Self> {
        let private_key = hex::encode(rand::random::<[u8; 32]>());
        let hostname = format!("tycho-{index}.tycho.default.svc.cluster.local:{port}");

        let output = Command::new("cargo")
            .arg("run")
            .arg("--example")
            .arg("network-node")
            .arg("--")
            .arg("gen-dht")
            .arg(&hostname)
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
            hostname,
            port,
            dht_value,
            key: private_key,
        })
    }
}
