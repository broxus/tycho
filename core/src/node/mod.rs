use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_crypto::ed25519;
use tycho_network::{DhtClient, DhtService, Network, OverlayService, PeerResolver, Router};
use tycho_storage::Storage;

pub use self::config::NodeBaseConfig;
use crate::block_strider::{
    ArchiveBlockProviderConfig, BlockchainBlockProviderConfig, StarterConfig,
};
use crate::blockchain_rpc::BlockchainRpcClient;
use crate::global_config::{GlobalConfig, ZerostateId};

mod config;

pub struct NodeBase {
    keypair: Arc<ed25519::KeyPair>,

    zerostate: ZerostateId,

    network: Network,
    dht_client: DhtClient,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,

    starter_config: StarterConfig,
    blockchain_block_provider_config: BlockchainBlockProviderConfig,
    archive_block_provider_config: ArchiveBlockProviderConfig,
}

pub struct ConfiguredNetwork {
    pub network: Network,
    pub dht_client: DhtClient,
    pub peer_resolver: PeerResolver,
    pub overlay_service: OverlayService,
}

impl ConfiguredNetwork {
    pub fn new(
        public_addr: SocketAddr,
        secret_key: &ed25519::SecretKey,
        base_config: &NodeBaseConfig,
        global_config: &GlobalConfig,
    ) -> Result<Self> {
        // Setup network
        let keypair = Arc::new(ed25519::KeyPair::from(secret_key));
        let local_id = keypair.public_key.into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id)
            .with_config(base_config.dht.clone())
            .build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_config(base_config.overlay.clone())
            .with_dht_service(dht_service.clone())
            .build();

        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();

        let local_addr = SocketAddr::from((base_config.local_ip, base_config.port));

        let network = Network::builder()
            .with_config(base_config.network.clone())
            .with_private_key(secret_key.to_bytes())
            .with_remote_addr(public_addr)
            .build(local_addr, router)
            .context("failed to build node network")?;

        dht_tasks.spawn(&network);
        overlay_tasks.spawn(&network);

        let dht_client = dht_service.make_client(&network);
        let peer_resolver = dht_service
            .make_peer_resolver()
            .with_config(base_config.peer_resolver.clone())
            .build(&network);

        let mut bootstrap_peers = 0usize;
        for peer in &global_config.bootstrap_peers {
            let is_new = dht_client.add_peer(Arc::new(peer.clone()))?;
            bootstrap_peers += is_new as usize;
        }

        tracing::info!(
            %local_id,
            %local_addr,
            %public_addr,
            bootstrap_peers,
            "initialized network"
        );

        Ok(Self {
            network,
            dht_client,
            peer_resolver,
            overlay_service,
        })
    }
}
