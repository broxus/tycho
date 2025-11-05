#![allow(clippy::print_stdout)]
//! Run tests with this env:
//! ```text
//! RUST_LOG=info,tycho_network=trace
//! ```
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Result;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};
use tycho_crypto::ed25519;
use tycho_network::{
    Address, DhtClient, DhtConfig, DhtService, Network, NetworkConfig, PeerId, PeerInfo,
    Router,
};
use tycho_util::time::now_sec;
#[tokio::main]
async fn main() -> Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(main)),
        file!(),
        23u32,
    );
    {
        __guard.end_section(24u32);
        let __result = Cli::parse().run().await;
        __guard.start_section(24u32);
        __result
    }
}
/// Tycho network node.
#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}
impl Cli {
    async fn run(self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run)),
            file!(),
            35u32,
        );
        let enable_persistent_logs = std::env::var("TYCHO_PERSISTENT_LOGS").is_ok();
        let collector = tracing_subscriber::registry()
            .with(
                fmt::Layer::new()
                    .with_ansi(std::io::stdout().is_terminal())
                    .compact()
                    .with_writer(std::io::stdout)
                    .with_filter(EnvFilter::from_default_env()),
            );
        if enable_persistent_logs {
            let file_appender = tracing_appender::rolling::hourly(
                "logs",
                "tycho-network",
            );
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
            let collector = collector
                .with(
                    fmt::Layer::new()
                        .with_ansi(false)
                        .compact()
                        .with_writer(non_blocking)
                        .with_filter(EnvFilter::new("trace")),
                );
            tracing::subscriber::set_global_default(collector)?;
        } else {
            tracing::subscriber::set_global_default(collector)?;
        };
        match self.cmd {
            Cmd::Run(cmd) => {
                __guard.end_section(63u32);
                let __result = cmd.run().await;
                __guard.start_section(63u32);
                __result
            }
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
    remote_addr: Address,
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run)),
            file!(),
            99u32,
        );
        let node_config = self
            .config
            .map(NodeConfig::from_file)
            .transpose()?
            .unwrap_or_default();
        let global_config = GlobalConfig::from_file(self.global_config)?;
        let node = Node::new(
            parse_key(&self.key)?,
            self.addr.into(),
            self.remote_addr,
            node_config,
        )?;
        let mut initial_peer_count = 0usize;
        for peer in global_config.bootstrap_peers {
            __guard.checkpoint(115u32);
            let is_new = node.dht.add_peer(Arc::new(peer))?;
            initial_peer_count += is_new as usize;
        }
        tracing::info!(
            local_id = % node.network.peer_id(), addr = % self.addr, initial_peer_count,
            "node started"
        );
        {
            __guard.end_section(127u32);
            let __result = futures_util::future::pending().await;
            __guard.start_section(127u32);
            __result
        }
    }
}
/// generate a key
#[derive(Parser)]
struct CmdGenKey {}
impl CmdGenKey {
    #[allow(clippy::unused_self)]
    fn run(self) -> Result<()> {
        let secret_key = rand::random::<ed25519::SecretKey>();
        let public_key = ed25519::PublicKey::from(&secret_key);
        let peer_id = PeerId::from(public_key);
        let data = serde_json::json!(
            { "key" : hex::encode(secret_key.as_bytes()), "peer_id" : peer_id
            .to_string(), }
        );
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
    /// a list of node addresses
    #[clap(required = true)]
    addr: Vec<Address>,
    /// node secret key
    #[clap(long)]
    key: String,
    /// time to live in seconds (default: unlimited)
    #[clap(long)]
    ttl: Option<u32>,
}
impl CmdGenDht {
    fn run(self) -> Result<()> {
        let entry = Node::make_peer_info(parse_key(&self.key)?, self.addr, self.ttl);
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
            anyhow::ensure!(peer.verify(now), "invalid peer info for {}", peer.id);
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
struct Node {
    network: Network,
    dht: DhtClient,
}
impl Node {
    fn new(
        key: ed25519::SecretKey,
        address: Address,
        remote_addr: Address,
        config: NodeConfig,
    ) -> Result<Self> {
        let keypair = tycho_crypto::ed25519::KeyPair::from(&key);
        let (dht_tasks, dht_service) = DhtService::builder(keypair.public_key.into())
            .with_config(config.dht)
            .build();
        let router = Router::builder().route(dht_service.clone()).build();
        let network = Network::builder()
            .with_config(config.network)
            .with_private_key(key.to_bytes())
            .with_remote_addr(remote_addr)
            .build(address, router)?;
        dht_tasks.spawn(&network);
        let dht = dht_service.make_client(&network);
        Ok(Self { network, dht })
    }
    fn make_peer_info(
        key: ed25519::SecretKey,
        address_list: Vec<Address>,
        ttl: Option<u32>,
    ) -> PeerInfo {
        let keypair = ed25519::KeyPair::from(&key);
        let peer_id = PeerId::from(keypair.public_key);
        let now = now_sec();
        let mut node_info = PeerInfo {
            id: peer_id,
            address_list: address_list.into_boxed_slice(),
            created_at: now,
            expires_at: now.saturating_add(ttl.unwrap_or(u32::MAX)),
            signature: Box::new([0; 64]),
        };
        *node_info.signature = keypair.sign_tl(&node_info);
        node_info
    }
}
fn parse_key(key: &str) -> Result<ed25519::SecretKey> {
    match hex::decode(key)?.try_into() {
        Ok(bytes) => Ok(ed25519::SecretKey::from_bytes(bytes)),
        Err(_) => anyhow::bail!("invalid secret key"),
    }
}
