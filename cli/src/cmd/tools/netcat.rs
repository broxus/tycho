use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tycho_core::node::{NodeBaseConfig, NodeKeys};
use tycho_crypto::ed25519;
use tycho_network::{
    Address, Network, NetworkExt, PeerId, PeerInfo, Request, Response, Routable, Router, Service,
    ServiceRequest,
};
use tycho_util::cli::logger::{LoggerConfig, LoggerOutput, LoggerStderrOutput};
use tycho_util::cli::resolve_public_ip;
use tycho_util::cli::signal::run_or_terminate;
use tycho_util::serde_helpers::load_json_from_file;

use crate::BaseArgs;

/// Netcat subset for debugging network issues.
#[derive(clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        nc_rt(async move {
            match self.cmd {
                SubCmd::Listen(cmd) => cmd.run(args).await,
                SubCmd::Connect(cmd) => cmd.run(args).await,
            }
        })
    }
}

#[derive(clap::Parser)]
enum SubCmd {
    Listen(ListenCmd),
    Connect(ConnectCmd),
}

/// Listen for incoming connections.
#[derive(clap::Parser)]
struct ListenCmd {
    /// Custom UDP port number.
    #[clap(long)]
    port: Option<u16>,

    /// Path to the node config. Default: `$TYCHO_HOME/config.json`
    #[clap(long)]
    config: Option<PathBuf>,

    /// Path to the node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    keys: Option<PathBuf>,
}

impl ListenCmd {
    async fn run(self, args: BaseArgs) -> Result<()> {
        let _network = init(args, self.port, self.config, self.keys).await?;
        futures_util::future::pending().await
    }
}

/// Connect to the remote Tycho netcat instance.
#[derive(clap::Parser)]
struct ConnectCmd {
    /// Remote socket address.
    address: Address,

    /// Remote peer ID.
    id: PeerId,

    /// Custom UDP port number.
    #[clap(long)]
    port: Option<u16>,

    /// Path to the node config. Default: `$TYCHO_HOME/config.json`
    #[clap(long)]
    config: Option<PathBuf>,

    /// Path to the node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    keys: Option<PathBuf>,
}

impl ConnectCmd {
    async fn run(self, args: BaseArgs) -> Result<()> {
        let network = init(args, self.port, self.config, self.keys).await?;

        let peer_info = Arc::new(PeerInfo {
            id: self.id,
            address_list: vec![self.address.clone()].into_boxed_slice(),
            created_at: 0,
            expires_at: u32::MAX,
            signature: Box::new([0; 64]),
        });
        let _handle = network
            .known_peers()
            .insert(peer_info.clone(), false)
            .context("failed to add remote peer")?;

        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;

            tracing::info!("sending ping");

            let created_at = tycho_util::time::now_millis();
            match network
                .query(&peer_info.id, Request::from_tl(proto::Ping { created_at }))
                .await
            {
                Ok(response) => match response.parse_tl::<proto::Pong>() {
                    Ok(proto::Pong { received_at }) => {
                        let ping_ms = received_at as i64 - created_at as i64;

                        tracing::info!(
                            peer_id = %peer_info.id,
                            addr = %self.address,
                            ping_ms,
                            "received pong"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            peer_id = %peer_info.id,
                            addr = %self.address,
                            "invalid response: {e:?}",
                        );
                    }
                },
                Err(e) => {
                    tracing::error!(
                        peer_id = %peer_info.id,
                        addr = %self.address,
                        "failed to send ping: {e:?}",
                    );
                }
            }
        }
    }
}

// === Stuff ===

async fn init(
    args: BaseArgs,
    custom_port: Option<u16>,
    config: Option<PathBuf>,
    keys: Option<PathBuf>,
) -> Result<Network> {
    let mut config =
        load_json_from_file::<NodeBaseConfig, _>(args.node_config_path(config.as_ref()))
            .context("failed to load node config")?
            .with_relative_paths(&args.home);

    if let Some(port) = custom_port {
        config.port = port;
    }

    tycho_util::cli::logger::init_logger(
        &LoggerConfig {
            outputs: vec![LoggerOutput::Stderr(LoggerStderrOutput)],
        },
        None,
    )?;

    make_network(args, config, keys).await
}

async fn make_network(
    args: BaseArgs,
    config: NodeBaseConfig,
    keys: Option<PathBuf>,
) -> Result<Network> {
    let public_ip = resolve_public_ip(config.public_ip).await?;

    let node_keys_path = args.node_keys_path(keys.as_ref());
    let key = NodeKeys::load_or_create(node_keys_path)?.as_secret();
    let local_id: PeerId = ed25519::PublicKey::from(&key).into();

    let router = Router::builder().route(PingService).build();

    let network = Network::builder()
        .with_config(config.network)
        .with_private_key(key.to_bytes())
        .with_remote_addr(SocketAddr::new(public_ip, config.port))
        .build((config.local_ip, config.port), router)
        .unwrap();

    let local_addr = network.local_addr();

    let mut public_addr = network.remote_addr().clone();
    match &mut public_addr {
        Address::Ip(addr) => addr.set_port(local_addr.port()),
        Address::Dns { port, .. } => *port = local_addr.port(),
    }

    tracing::info!(
        %local_id,
        %local_addr,
        %public_addr,
        "initialized network"
    );
    Ok(network)
}

#[derive(Clone)]
struct PingService;

impl Service<ServiceRequest> for PingService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Response>>;
    type OnMessageFuture = futures_util::future::Ready<()>;

    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        futures_util::future::ready(match req.parse_tl() {
            Ok(proto::Ping { created_at }) => {
                let received_at = tycho_util::time::now_millis();
                let ping_ms = received_at as i64 - created_at as i64;

                tracing::info!(
                    peer_id = %req.metadata.peer_id,
                    addr = %req.metadata.remote_address,
                    ping_ms,
                    "received ping"
                );

                Some(Response::from_tl(proto::Pong { received_at }))
            }
            Err(e) => {
                tracing::error!(
                    peer_id = %req.metadata.peer_id,
                    addr = %req.metadata.remote_address,
                    "invalid request: {e:?}",
                );
                None
            }
        })
    }

    fn on_message(&self, _req: ServiceRequest) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }
}

impl Routable for PingService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [proto::Ping::TL_ID]
    }
}

mod proto {
    use tl_proto::{TlRead, TlWrite};

    #[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "netcat.ping",
        scheme_inline = "netcat.ping = netcat.Ping;"
    )]
    pub struct Ping {
        pub created_at: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
    #[tl(
        boxed,
        id = "netcat.pong",
        scheme_inline = "netcat.pong = netcat.Pong;"
    )]
    pub struct Pong {
        pub received_at: u64,
    }
}

fn nc_rt<F>(f: F) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_or_terminate(f))
}
