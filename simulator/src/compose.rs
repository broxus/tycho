use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::ServiceConfig;

pub struct ComposeRunner {
    compose_path: PathBuf,
    compose: DockerCompose,
}

impl ComposeRunner {
    pub fn new(service_config: &ServiceConfig) -> Result<Self> {
        let compose_path = service_config.compose_path();
        if std::fs::metadata(&compose_path).is_ok() {
            return Self::load_from_fs(service_config);
        }

        Ok(Self {
            compose_path,
            compose: DockerCompose::new(service_config.network_subnet.clone())?,
        })
    }

    pub fn load_from_fs(config: &ServiceConfig) -> Result<Self> {
        let compose_path = config.compose_path();
        let compose = std::fs::read_to_string(&compose_path)
            .with_context(|| format!("Failed to read {:?}", &compose_path))?;
        let compose: DockerCompose = serde_json::from_str(&compose)
            .with_context(|| format!("Failed to parse {:?}", &compose_path))?;
        if compose.services.is_empty() {
            anyhow::bail!("No services found in {:?}", &compose_path);
        }

        Ok(Self {
            compose_path,
            compose,
        })
    }

    pub fn add_service(&mut self, name: String, service: Service) {
        self.compose.services.insert(name, service);
    }

    pub fn finalize(&self) -> Result<()> {
        let file = std::fs::File::create(&self.compose_path)?;
        println!("Writing to {:?}", &self.compose_path);
        serde_json::to_writer_pretty(file, &self.compose)?;

        Ok(())
    }

    /// Executes a Docker Compose command with the given arguments.
    pub fn execute_compose_command<T>(&self, args: &[T]) -> Result<()>
    where
        T: AsRef<OsStr>,
    {
        let mut command = Command::new("docker");
        let mut command = command.arg("compose").arg("-f").arg(&self.compose_path);

        for arg in args {
            command = command.arg(arg);
        }

        command
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .stdin(Stdio::inherit())
            .spawn()
            .context("Failed to spawn Docker Compose command")?
            .wait()
            .context("Failed to wait on Docker Compose command")?;

        Ok(())
    }

    pub fn logs(&self, follow: bool, node: Option<usize>) -> Result<()> {
        let mut args = vec!["logs".to_string()];

        if follow {
            args.push("-f".to_string());
        }

        if let Some(node_index) = node {
            args.push(format!("node-{}", node_index));
        }

        self.execute_compose_command(&args)
    }

    pub fn stop_node(&self, node_index: Option<usize>) -> Result<()> {
        let mut args = vec!["stop".to_string()];
        if let Some(node_index) = node_index {
            args.push(format!("node-{}", node_index));
        }
        self.execute_compose_command(&args)
    }

    pub fn start_node(&self, node_index: Option<usize>) -> Result<()> {
        let mut args = vec!["up".to_string()];
        if let Some(node_index) = node_index {
            args.push(format!("node-{}", node_index));
        }
        args.push("-d".to_string());

        self.execute_compose_command(&args)
    }

    pub fn exec_command(&self, node_index: usize, cmd: &str, args: Vec<String>) -> Result<()> {
        let service_name = format!("node-{}", node_index);
        let mut docker_compose_command = vec!["exec".to_string(), service_name, cmd.to_string()];
        docker_compose_command.extend(args);
        self.execute_compose_command(&docker_compose_command)?;

        Ok(())
    }

    pub fn down(&self) -> Result<()> {
        self.execute_compose_command(&["down", "--remove-orphans"])?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct DockerCompose {
    version: String,
    services: HashMap<String, Service>,
    networks: HashMap<String, Network>,
}

impl DockerCompose {
    pub fn new(subnet: String) -> Result<Self> {
        let mut compose = Self::default();
        compose
            .networks
            .insert("default".to_string(), Network::new(subnet));
        Ok(compose)
    }
}

impl Default for DockerCompose {
    fn default() -> Self {
        Self {
            version: "3.8".to_string(),
            services: HashMap::new(),
            networks: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Service {
    pub entrypoint: String,
    pub image: String,
    pub networks: HashMap<String, ServiceNetwork>,
    #[serde(rename = "stop_grace_period")]
    pub stop_grace_period: String,
    #[serde(rename = "stop_signal")]
    pub stop_signal: String,
    pub volumes: Vec<String>,
    pub privileged: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServiceNetwork {
    #[serde(rename = "ipv4_address")]
    pub ipv4_address: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Network {
    ipam: Ipam,
}

impl Network {
    fn new(subnet: String) -> Self {
        Self {
            ipam: Ipam {
                config: vec![Subnet { subnet }],
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Ipam {
    config: Vec<Subnet>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Subnet {
    subnet: String,
}
