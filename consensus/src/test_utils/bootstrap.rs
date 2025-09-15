use std::num::NonZeroU16;

use tycho_crypto::ed25519::{KeyPair, PublicKey, SecretKey};
use tycho_network::{
    Address, DhtClient, DhtConfig, DhtService, Network, NetworkConfig, OverlayConfig,
    OverlayService, PeerId, PeerInfo, PeerResolver, PeerResolverConfig, Router, ToSocket,
};
use tycho_types::models::{ConsensusConfig, GenesisInfo};
use tycho_util::time::now_sec;

use crate::engine::{MempoolConfigBuilder, MempoolMergedConfig, MempoolNodeConfig};

pub fn default_test_config() -> MempoolMergedConfig {
    let consensus_config = ConsensusConfig {
        clock_skew_millis: (5 * 1000).try_into().unwrap(),
        payload_batch_bytes: (768 * 1024).try_into().unwrap(),
        commit_history_rounds: 20.try_into().unwrap(),
        deduplicate_rounds: 20,
        max_consensus_lag_rounds: 20.try_into().unwrap(),
        payload_buffer_bytes: (50 * 1024 * 1024).try_into().unwrap(),
        broadcast_retry_millis: 150.try_into().unwrap(),
        download_retry_millis: 25.try_into().unwrap(),
        download_peers: 2.try_into().unwrap(),
        min_sign_attempts: 3.try_into().unwrap(),
        download_peer_queries: 1.try_into().unwrap(),
        sync_support_rounds: 15.try_into().unwrap(),
    };

    let node_config = MempoolNodeConfig {
        clean_db_period_rounds: NonZeroU16::new(10).unwrap(),
        ..Default::default()
    };

    let mut builder = MempoolConfigBuilder::new(&node_config);
    builder.set_genesis(GenesisInfo::default());
    builder
        .set_consensus_config(&consensus_config)
        .expect("invalid consensus config");

    builder.build().unwrap()
}

pub fn make_peer_info(keypair: &KeyPair, address_list: Vec<Address>, ttl: Option<u32>) -> PeerInfo {
    let peer_id = PeerId::from(keypair.public_key);

    let now = now_sec();
    let mut peer_info = PeerInfo {
        id: peer_id,
        address_list: address_list.into_boxed_slice(),
        created_at: now,
        expires_at: ttl.unwrap_or(u32::MAX),
        signature: Box::new([0; 64]),
    };
    *peer_info.signature = keypair.sign_tl(&peer_info);
    peer_info
}

pub fn from_validator<T: ToSocket, A: Into<Address>>(
    bind_address: T,
    secret_key: &SecretKey,
    remote_addr: Option<A>,
    dht_config: DhtConfig,
    peer_resolver_config: Option<PeerResolverConfig>,
    overlay_config: Option<OverlayConfig>,
    network_config: NetworkConfig,
) -> (DhtClient, PeerResolver, OverlayService) {
    let local_id = PeerId::from(PublicKey::from(secret_key));

    let (dht_tasks, dht_service) = DhtService::builder(local_id)
        .with_config(dht_config)
        .build();

    let mut overlay_service_builder =
        OverlayService::builder(local_id).with_dht_service(dht_service.clone());

    if let Some(overlay_config) = overlay_config {
        overlay_service_builder = overlay_service_builder.with_config(overlay_config);
    }
    let (overlay_tasks, overlay_service) = overlay_service_builder.build();

    let router = Router::builder()
        .route(dht_service.clone())
        .route(overlay_service.clone())
        .build();

    let mut network_builder = Network::builder()
        .with_config(network_config)
        .with_private_key(secret_key.to_bytes());
    if let Some(remote_addr) = remote_addr {
        network_builder = network_builder.with_remote_addr(remote_addr);
    }

    let network = network_builder.build(bind_address, router).unwrap();

    let mut peer_resolver_builder = dht_service.make_peer_resolver();
    if let Some(peer_resolver_config) = peer_resolver_config {
        peer_resolver_builder = peer_resolver_builder.with_config(peer_resolver_config);
    }
    let peer_resolver = peer_resolver_builder.build(&network);

    dht_tasks.spawn(&network);
    overlay_tasks.spawn(&network);

    (
        dht_service.make_client(&network),
        peer_resolver,
        overlay_service,
    )
}
