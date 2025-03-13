use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result};
use clap::Parser;
use serde::Serialize;
use serde_json::{json, Value};
use tycho_cli_models::account::AccountStateOutput;
use tycho_cli_models::keypair::KeypairOutput;
use tycho_cli_models::FpTokens;

#[derive(Debug, Parser)]
pub(crate) struct GenNetworkArgs {
    #[arg(short, long)]
    force: bool,
    #[arg(long, default_value = ".temp")]
    dir: PathBuf,
    #[arg(long, default_value = "100000")]
    validator_balance: FpTokens,
    #[arg(long, default_value = "30000")]
    validator_stake: FpTokens,
    #[arg(long, default_value = "false")]
    save_logs: bool,
    /// If set add giver to the zerostate
    #[arg(long, requires = "giver_balance")]
    save_giver_keys: Option<PathBuf>,
    #[clap(long)]
    giver_balance: Option<FpTokens>,
    nodes: usize,
}

pub fn run(args: GenNetworkArgs) -> Result<()> {
    let root_dir = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let base_dir = root_dir.join(&args.dir);

    // Handle existing network
    if base_dir.join("zerostate.boc").exists() {
        if !args.force {
            anyhow::bail!("Network exists, use `--force` to overwrite");
        }
        fs::remove_dir_all(&base_dir)?;
    }
    fs::create_dir_all(&base_dir).context("Failed to create base directory")?;

    let binary = Binary::build(root_dir)?;
    let mut global = json!({ "bootstrap_peers": [] });
    let mut zerostate = initial_zerostate(root_dir)?;

    for i in 1..=args.nodes {
        let (node_cfg, elections_cfg, dht) = node_config(&binary, &base_dir, i, &args)?;

        write_json(node_json_path(&base_dir, "config", i), &node_cfg)
            .context("Failed to write node config")?;
        write_json(node_json_path(&base_dir, "elections", i), &elections_cfg)
            .context("Failed to write elections config")?;

        global["bootstrap_peers"].as_array_mut().unwrap().push(dht);

        zerostate["validators"].as_array_mut().unwrap().push(
            read_json(node_json_path(&base_dir, "keys", i))?
                .get("public")
                .context("Missing public key in keys file")?
                .clone(),
        );
    }

    let zerostate_id = binary
        .generate_zerostate(&base_dir, &mut zerostate, &args)
        .context("Failed to generate zerostate")?;

    // Add zerostate ID to global config after generation
    global["zerostate"] = json!(zerostate_id);

    write_json(base_dir.join("global-config.json"), &global)
        .context("Failed to write global config")?;

    Ok(())
}

struct Binary {
    bin: PathBuf,
}

impl Binary {
    fn build(root_dir: &Path) -> Result<Self> {
        eprintln!("Building node");
        let output = Command::new(root_dir.join("./scripts/build-node.sh"))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .output()
            .context("Failed to build node")?;
        if output.status.success() {
            Ok(Self {
                bin: PathBuf::from(String::from_utf8(output.stdout)?.trim()),
            })
        } else {
            Err(anyhow::anyhow!(
                "Node build failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    fn run_inner(&self, args: &[&str]) -> Result<Output> {
        let output = Command::new(&self.bin).args(args).output()?;
        output.status.success().then_some(()).ok_or_else(|| {
            anyhow::anyhow!(
                "Command failed: {}. Exit code: {}",
                String::from_utf8_lossy(&output.stderr),
                output.status,
            )
        })?;
        Ok(output)
    }

    fn run_tool(&self, args: &[&str]) -> Result<Value> {
        let output = self.run_inner(args)?;
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn run_tool_typed<T: serde::de::DeserializeOwned>(&self, args: &[&str]) -> Result<T> {
        let output = self.run_inner(args)?;
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn gen_key(&self) -> Result<KeypairOutput> {
        self.run_tool_typed(&["tool", "gen-key"])
    }

    fn gen_account_wallet(&self, pubkey: &str, balance: &str) -> Result<Value> {
        self.run_tool(&[
            "tool",
            "gen-account",
            "wallet",
            "--pubkey",
            pubkey,
            "--balance",
            balance,
        ])
    }

    fn gen_dht(&self, addr: &str, key: &str) -> Result<Value> {
        self.run_tool(&["tool", "gen-dht", addr, "--key", key])
    }

    fn generate_zerostate(
        &self,
        base: &Path,
        zerostate: &mut Value,
        args: &GenNetworkArgs,
    ) -> Result<Value> {
        if let Some(giver_keys) = &args.save_giver_keys {
            let key_pair = self.gen_key()?;
            write_json(giver_keys, &key_pair)?;

            let giver: AccountStateOutput = self.run_tool_typed(&[
                "tool",
                "gen-account",
                "wallet",
                "--pubkey",
                &key_pair.public,
                "--balance",
                &args.giver_balance.as_ref().unwrap().to_string(),
            ])?;
            zerostate["accounts"]
                .as_object_mut()
                .unwrap()
                .insert(giver.account, Value::String(giver.boc));
        }

        let zerostate_path = base.join("zerostate.json");
        write_json(&zerostate_path, zerostate)?;

        self.run_tool(&[
            "tool",
            "gen-zerostate",
            zerostate_path.to_str().unwrap(),
            "--output",
            base.join("zerostate.boc").to_str().unwrap(),
            "--force",
        ])
    }
}

fn initial_zerostate(root: &Path) -> Result<Value> {
    let mut zs: Value = read_json(root.join("zerostate.json"))?;
    zs["validators"] = json!([]);
    zs["accounts"] = json!({});
    Ok(zs)
}

fn node_config(
    binary: &Binary,
    base: &Path,
    i: usize,
    args: &GenNetworkArgs,
) -> Result<(Value, Value, Value)> {
    let port = 20000 + i;
    let rpc_port = 8000 + i;
    let metrics_port = 10000 + i;

    let node_keys = binary.gen_key()?;
    write_json(node_json_path(base, "keys", i), &node_keys)?;

    let wallet_keys = binary.gen_key()?;
    let wallet =
        binary.gen_account_wallet(&wallet_keys.public, &args.validator_balance.to_string())?;
    let wallet: AccountStateOutput = serde_json::from_value(wallet)?;

    let secret = node_keys.secret;
    let dht = binary.gen_dht(&format!("127.0.0.1:{port}"), &secret)?;

    let mut cfg = read_json(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("config.json"),
    )?;
    let db_path = base.join(format!("db{i}"));
    fs::create_dir_all(&db_path)?;

    cfg["storage"]["root_dir"] = json!(db_path.to_str().unwrap());
    cfg["port"] = json!(port);
    cfg["rpc"]["listen_addr"] = json!(format!("0.0.0.0:{rpc_port}"));
    cfg["metrics"]["listen_addr"] = json!(format!("0.0.0.0:{metrics_port}"));
    cfg["control"]["socket_path"] = json!(base.join(format!("control{i}.sock")).to_str().unwrap());
    cfg["blockchain_rpc_client"]["too_new_archive_threshold"] = json!(1);

    if args.save_logs {
        let logger_config = LoggerConfig {
            outputs: vec![
                LoggerOutput::Stderr,
                LoggerOutput::File(LoggerFileOutput {
                    dir: base.join(format!("logs{i}")),
                    file_prefix: "node".to_string(),
                    human_readable: true,
                    max_files: NonZeroUsize::new(1).unwrap(),
                }),
            ],
        };
        cfg["logger"] = serde_json::to_value(logger_config)?;
    }

    // Create elections config
    let elections = json!({
        "ty": "Simple",
        "wallet_private": secret,
        "wallet_address": format!("-1:{}", wallet.account),
        "stake": &args.validator_stake,
    });

    Ok((cfg, elections, dht))
}

fn node_json_path(base: &Path, prefix: &str, i: usize) -> PathBuf {
    base.join(format!("{prefix}{i}.json"))
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> Result<()> {
    Ok(fs::write(path, serde_json::to_string_pretty(value)?)?)
}

fn read_json(path: impl AsRef<Path>) -> Result<Value> {
    let path = path.as_ref();
    let data = fs::read_to_string(path)
        .with_context(|| format!("Failed to read JSON file: {}", path.display()))?;
    serde_json::from_str(&data).context("Failed to parse JSON")
}

#[derive(Debug, Clone, Serialize)]
pub struct LoggerConfig {
    pub outputs: Vec<LoggerOutput>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum LoggerOutput {
    Stderr,
    File(LoggerFileOutput),
}

#[derive(Debug, Clone, Serialize)]
pub struct LoggerFileOutput {
    pub dir: PathBuf,
    pub human_readable: bool,
    pub file_prefix: String,
    pub max_files: NonZeroUsize,
}
