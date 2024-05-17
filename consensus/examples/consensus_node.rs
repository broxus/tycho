//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_consensus=debug
//! ```

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};
use tycho_consensus::test_utils::drain_anchors;
use tycho_consensus::{Engine, InputBufferStub};
use tycho_network::{DhtConfig, NetworkConfig, PeerId, PeerInfo};
use tycho_util::time::now_sec;

#[tokio::main]
async fn main() -> Result<()> {
    Cli::parse().run().await
}

/// Tycho network node.
#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    async fn run(self) -> Result<()> {
        let enable_persistent_logs = std::env::var("TYCHO_PERSISTENT_LOGS").is_ok();

        let collector = tracing_subscriber::registry().with(
            fmt::Layer::new()
                .with_ansi(std::io::stdout().is_terminal())
                .compact()
                .with_writer(std::io::stdout)
                .with_filter(EnvFilter::from_default_env()),
        );

        if enable_persistent_logs {
            let file_appender = tracing_appender::rolling::hourly("logs", "tycho-consensus");
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

            let collector = collector.with(
                fmt::Layer::new()
                    .with_ansi(false)
                    .compact()
                    .with_writer(non_blocking)
                    .with_filter(EnvFilter::new("trace")), // todo: update with needed crates
            );
            tracing::subscriber::set_global_default(collector)?;
        } else {
            tracing::subscriber::set_global_default(collector)?;
        };

        match self.cmd {
            Cmd::Run(cmd) => cmd.run().await,
            Cmd::GenKey(cmd) => cmd.run(),
            Cmd::GenDht(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    Run(CmdRun),
    GenKey(CmdGenKey),
    GenDht(CmdGenDht),
}

/// run a node
#[derive(Parser)]
struct CmdRun {
    /// local node address
    addr: SocketAddr,

    /// node secret key
    #[clap(long)]
    key: String,

    /// path to the node config
    #[clap(long)]
    config: Option<String>,

    /// path to the global config
    #[clap(long)]
    global_config: String,
}

impl CmdRun {
    async fn run(self) -> Result<()> {
        let node_config = self
            .config
            .map(NodeConfig::from_file)
            .transpose()?
            .unwrap_or_default();
        let global_config = GlobalConfig::from_file(self.global_config)?;

        let secret_key = parse_key(&self.key)?;
        let key_pair = Arc::new(KeyPair::from(&secret_key));

        let (dht_client, overlay) = tycho_consensus::test_utils::from_validator(
            self.addr,
            &secret_key,
            node_config.dht,
            node_config.network,
        );

        let all_peers = global_config
            .bootstrap_peers
            .iter()
            .map(|info| info.id)
            .collect::<Vec<_>>();

        let mut initial_peer_count = 1_usize;
        let local_id = PeerId::from(PublicKey::from(&secret_key));
        for peer in global_config.bootstrap_peers {
            if peer.id != local_id {
                let is_new = dht_client.add_peer(Arc::new(peer))?;
                initial_peer_count += is_new as usize;
            }
        }

        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        let mut engine = Engine::new(
            key_pair.clone(),
            &dht_client,
            &overlay,
            committed_tx,
            InputBufferStub::new(100, 5),
        );
        engine.init_with_genesis(all_peers.as_slice()).await;
        tokio::spawn(drain_anchors(committed_rx));

        tracing::info!(
            local_id = %dht_client.network().peer_id(),
            addr = %self.addr,
            initial_peer_count,
            "node started"
        );

        tokio::spawn(engine.run());

        futures_util::future::pending().await
    }
}

/// generate a key
#[derive(Parser)]
struct CmdGenKey {}

impl CmdGenKey {
    fn run(self) -> Result<()> {
        let secret_key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let public_key = ed25519::PublicKey::from(&secret_key);
        let peer_id = PeerId::from(public_key);

        let data = serde_json::json!({
            "key": hex::encode(secret_key.as_bytes()),
            "peer_id": peer_id.to_string(),
        });
        let output = if std::io::stdin().is_terminal() {
            serde_json::to_string_pretty(&data)
        } else {
            serde_json::to_string(&data)
        }?;
        println!("{output}");
        Ok(())
    }
}

/// generate a dht node info
#[derive(Parser)]
struct CmdGenDht {
    /// local node address
    addr: SocketAddr,

    /// node secret key
    #[clap(long)]
    key: String,

    /// time to live in seconds (default: unlimited)
    #[clap(long)]
    ttl: Option<u32>,
}

impl CmdGenDht {
    fn run(self) -> Result<()> {
        let secret_key = parse_key(&self.key)?;
        let key_pair = KeyPair::from(&secret_key);
        let entry =
            tycho_consensus::test_utils::make_peer_info(&key_pair, self.addr.into(), self.ttl);
        let output = if std::io::stdin().is_terminal() {
            serde_json::to_string_pretty(&entry)
        } else {
            serde_json::to_string(&entry)
        }?;
        println!("{output}");
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct GlobalConfig {
    bootstrap_peers: Vec<PeerInfo>,
}

impl GlobalConfig {
    fn from_file(path: impl AsRef<str>) -> Result<Self> {
        let config: Self = {
            let data = std::fs::read_to_string(path.as_ref())?;
            serde_json::from_str(&data)?
        };

        let now = now_sec();
        for peer in &config.bootstrap_peers {
            anyhow::ensure!(peer.is_valid(now), "invalid peer info for {}", peer.id);
        }

        Ok(config)
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct NodeConfig {
    network: NetworkConfig,
    dht: DhtConfig,
}

impl NodeConfig {
    fn from_file(path: impl AsRef<str>) -> Result<Self> {
        let data = std::fs::read_to_string(path.as_ref())?;
        let config = serde_json::from_str(&data)?;
        Ok(config)
    }
}

fn parse_key(key: &str) -> Result<ed25519::SecretKey> {
    match hex::decode(key)?.try_into() {
        Ok(bytes) => Ok(ed25519::SecretKey::from_bytes(bytes)),
        Err(_) => anyhow::bail!("invalid secret key"),
    }
}
