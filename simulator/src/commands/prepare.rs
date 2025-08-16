use std::process::Command;

use anyhow::{Context, Result};
use base64::Engine;
use clap::Parser;
use tycho_util::serde_helpers::load_json_from_file;

use crate::backend::Helm;
use crate::config::{BuilderValues, HelmValues, SharedConfigs, SimulatorConfig};

#[derive(Parser)]
pub struct PrepareCommand;

impl PrepareCommand {
    pub fn run(config: &SimulatorConfig) -> Result<()> {
        // create builder values only if a file does not exist
        let builder_values = &config.project_root.simulator.helm.builder.values;
        let builder_values: BuilderValues =
            if std::fs::exists(builder_values).context("`builder` chart values")? {
                println!("loading existing `builder` chart values");
                load_json_from_file(builder_values).context("read `builder` chart values")?
            } else {
                println!("creating new `builder` chart values");
                let default = BuilderValues::default();
                std::fs::write(builder_values, serde_json::to_string_pretty(&default)?)
                    .context("write `builder` chart values")?;
                default
            };
        let global_conf_data = prepare_global_config(config)?;
        let zerostate = load_zerostate(config)?;
        let logger = serde_json::to_string(
            &load_json_from_file::<serde_json::Value, _>(&config.project_root.logger)
                .context("load logger.json")?,
        )?;

        let pod_milli_cpu: u16 = 1400; // TODO estimate based on node count
        let threads = u8::try_from(pod_milli_cpu.div_ceil(1000)).context("too many threads")?;

        let node_config = prepare_node_config(config, threads)?;

        let shared_configs = SharedConfigs {
            rust_log: "info,tycho_network=trace".to_string(),
            global_config: global_conf_data.global_config,
            config: node_config,
            zerostate,
            logger,
        };

        let consensus_config = match global_conf_data.consensus_config {
            Some(mempool_config) => mempool_config,
            None => {
                let zerostate_json: serde_json::Value =
                    load_json_from_file(&config.project_root.temp.zerostate_json)?;
                serde_path_to_error::deserialize(&zerostate_json["params"]["29"])?
            }
        };

        let pod_disk_gb = {
            let max_rounds = (consensus_config.commit_history_rounds
                + consensus_config.deduplicate_rounds
                + consensus_config.max_consensus_lag_rounds
                + consensus_config.sync_support_rounds) as u128;
            let kb_per_round = (global_conf_data.node_secrets.len()
                * consensus_config.payload_batch_bytes) as u128;
            // 6/5 is +20% provisioning
            let raw = (max_rounds * kb_per_round * 6 / 5).div_ceil(1024 * 1024 * 1024);
            u8::try_from(raw).context("too many disk GBs")?
        };

        let helm_values = HelmValues::new(
            config.cluster_type,
            &config.pod,
            &builder_values,
            shared_configs,
            global_conf_data.node_secrets,
            pod_disk_gb,
            pod_milli_cpu,
        );

        println!("writing `tycho` chart values");
        // write the values file
        // yaml is a superset of json, so we can use serde_json to write yaml
        std::fs::write(
            &config.project_root.simulator.helm.tycho.values,
            serde_json::to_string_pretty(&helm_values)?,
        )?;

        Helm::lint(config)?;

        println!("finished prepare");
        Ok(())
    }
}

struct GlobalConfData {
    global_config: String,
    consensus_config: Option<ConsensusConfigPart>,
    node_secrets: Vec<String>,
}

fn prepare_global_config(config: &SimulatorConfig) -> Result<GlobalConfData> {
    let mut global_config: serde_json::Value =
        load_json_from_file(&config.project_root.temp.global_config).context("global config")?;

    let mempool_config = global_config
        .get("mempool")
        .map(serde_path_to_error::deserialize)
        .transpose()?;

    let bootstrap_peers_field: &mut serde_json::Value = global_config
        .get_mut("bootstrap_peers")
        .context("bootstrap_peers field not found in global config")?;

    let bootstrap_peer_count = bootstrap_peers_field
        .as_array()
        .context("bootstrap_peers field in global config is not a json array")?
        .len();

    let mut bootstrap_peers = Vec::with_capacity(bootstrap_peer_count);
    let mut node_secrets = Vec::with_capacity(bootstrap_peer_count);

    for node_index in 0..bootstrap_peer_count {
        let node = make_node(config, node_index)?;
        bootstrap_peers.push(node.dht_value);
        node_secrets.push(node.secret_key);
    }

    *bootstrap_peers_field = serde_json::json!(bootstrap_peers);

    Ok(GlobalConfData {
        global_config: serde_json::to_string(&global_config)?,
        consensus_config: mempool_config,
        node_secrets,
    })
}

/// NOTE keys file index is shifted by +1 compared with pod name index
fn make_node(config: &SimulatorConfig, node_index: usize) -> Result<Node> {
    let keypair: serde_json::Value =
        load_json_from_file(config.project_root.temp.keys(node_index + 1))
            .with_context(|| format!("keys for node {node_index}"))?;

    let secret = keypair
        .get("secret")
        .with_context(|| format!("no `secret` field in keys for node {node_index}"))?
        .as_str()
        .with_context(|| format!("`secret` field in keys for node {node_index} is not a string"))?;

    // updates the next_node_ip and adds a new node to the network
    // TODO use port from config?
    Node::init_from_cli(node_index, secret, config.pod.node_port)
        .with_context(|| format!("failed to init node-{node_index}"))
}

fn prepare_node_config(config: &SimulatorConfig, threads: u8) -> Result<String> {
    let mut node_config: serde_json::Value =
        load_json_from_file(&config.project_root.config).context("node config")?;

    let node_port_field: &mut serde_json::Value = node_config
        .get_mut("port")
        .context("port field not found in node config")?;
    *node_port_field = serde_json::json!(&config.pod.node_port);

    node_config["metrics"]["listen_addr"] =
        serde_json::json!(format!("0.0.0.0:{}", config.pod.metrics_port));

    node_config["threads"]["rayon_threads"] = serde_json::json!(threads);
    node_config["threads"]["tokio_workers"] = serde_json::json!(threads);

    let storage_root_dir: &mut serde_json::Value = node_config
        .get_mut("storage")
        .context("`storage` field not found in node config")?
        .as_object_mut()
        .context("`storage` field in node config is not json object")?
        .get_mut("root_dir")
        .context("no `root_dir` field in `storage` object in node config")?;
    *storage_root_dir = serde_json::json!(&config.pod.db_path);

    node_config["core_storage"]["blob_db"]["pre_create_cas_tree"] = serde_json::json!(false);

    Ok(serde_json::to_string(&node_config)?)
}

fn load_zerostate(config: &SimulatorConfig) -> Result<String> {
    let zerostate_path = &config.project_root.temp.zerostate_boc;

    let zerostate_bytes = std::fs::metadata(zerostate_path)
        .and_then(|_| std::fs::read(zerostate_path))
        .context("failed to read zerostate.boc file")?;

    let zerostate_base64 = base64::engine::general_purpose::STANDARD.encode(zerostate_bytes);

    Ok(zerostate_base64)
}

#[derive(serde::Deserialize)]
struct ConsensusConfigPart {
    payload_batch_bytes: usize,
    commit_history_rounds: usize,
    deduplicate_rounds: usize,
    max_consensus_lag_rounds: usize,
    sync_support_rounds: usize,
}

struct Node {
    dht_value: serde_json::Value,
    secret_key: String,
}

impl Node {
    fn init_from_cli(index: usize, secret: &str, node_port: u16) -> Result<Self> {
        let hostname = format!("tycho-{index}.tycho.default.svc.cluster.local:{node_port}");

        let output = Command::new("cargo")
            .arg("run")
            .arg("--package")
            .arg("tycho-network")
            .arg("--example")
            .arg("network-node")
            .arg("--")
            .arg("gen-dht")
            .arg(&hostname)
            .arg("--key")
            .arg(secret)
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
            dht_value,
            secret_key: secret.to_owned(),
        })
    }
}
