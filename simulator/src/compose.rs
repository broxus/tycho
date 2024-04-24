use std::collections::HashMap;
use std::ffi::OsStr;
use std::{str};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::str::FromStr;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::ServiceConfig;
use crate::node::NodeOptions;

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
    pub fn execute_compose_command<T>(&self, args: &[T]) -> Result<Output>
    where
        T: AsRef<OsStr>,
    {
        let mut command = Command::new("docker");
        let mut command = command.arg("compose").arg("-f").arg(&self.compose_path);

        for arg in args {
            command = command.arg(arg);
        }

        let result = command
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .stdin(Stdio::inherit())
            .spawn()
            .context("Failed to spawn Docker Compose command")?
            .wait_with_output()
            .context("Failed to wait on Docker Compose command")?;

        Ok(result)
    }

    pub fn logs(&self, follow: bool, node: Option<usize>) -> Result<()> {
        let mut args = vec!["logs".to_string()];

        if follow {
            args.push("-f".to_string());
        }

        if let Some(node_index) = node {
            args.push(format!("node-{}", node_index));
        }

        self.execute_compose_command(&args)?;
        Ok(())
    }

    pub fn stop_node(&self, node_index: Option<usize>) -> Result<()> {
        let mut args = vec!["stop".to_string()];
        if let Some(node_index) = node_index {
            args.push(format!("node-{}", node_index));
        }
        self.execute_compose_command(&args)?;
        Ok(())
    }

    pub fn start_node(&self, node_index: Option<usize>) -> Result<()> {
        let mut args = vec!["up".to_string()];
        if let Some(node_index) = node_index {
            args.push(format!("node-{}", node_index));
        }
        args.push("-d".to_string());

        self.execute_compose_command(&args)?;

        {
            for i in self.get_running_nodes_list()? {
                let index = usize::from_str(&i[5..6])?;
                let info = self.node_info(index)?;
                if info.delay > 0 {
                    self.set_delay(index, info.delay)?;
                }
                if info.packet_loss > 0 {
                    self.set_packet_loss(index, info.packet_loss)?;
                }
            }
        }

        Ok(())
    }

    pub fn get_running_nodes_list(&self) -> Result<Vec<String>> {
        let docker_compose_command = vec!["config".to_string(), "--services".to_string()];
        let output = self.execute_compose_command(&docker_compose_command)?;
        let x = String::from_utf8(output.stdout)?.split("\n").map(|x| x.to_string()).collect();
        Ok(x)
    }

    pub fn node_info(&self, node_index: usize) -> Result<NodeOptions> {
        let command = "cat";
        let output = self.exec_command(node_index, command, vec!["/options/options.json".to_string()])?;
        let node_options = serde_json::from_slice(output.stdout.as_slice())?;
        Ok(node_options)
    }

    pub fn set_delay(&self, node_index: usize, delay: u16) -> Result<()> {
        println!("Setting delay {delay}ms for node {node_index}");
        let command = "sh";
        let args = format!("tc qdisc add dev eth0 root netem delay {delay}ms");
        self.exec_command(node_index, command, vec!["-c".to_string(), format!("{args}")])?;
        Ok(())
    }

    pub fn set_packet_loss(&self, node_index: usize, loss: u16) -> Result<()> {
        println!("Setting packet loss {loss}% for node {node_index}");
        let command = "sh";
        let args = format!("tc qdisc change dev eth0 root netem loss {loss}%");
        self.exec_command(node_index, command, vec!["-c".to_string(), format!("{args}")])?;
        Ok(())
    }

    pub fn exec_command(&self, node_index: usize, cmd: &str, args: Vec<String>) -> Result<Output> {
        let service_name = format!("node-{}", node_index);
        let mut docker_compose_command = vec!["exec".to_string(), service_name, cmd.to_string()];
        docker_compose_command.extend(args);
        let output = self.execute_compose_command(&docker_compose_command)?;
        Ok(output)
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
