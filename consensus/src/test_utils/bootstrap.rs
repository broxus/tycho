use everscale_crypto::ed25519::{KeyPair, PublicKey, SecretKey};
use tycho_network::{
    Address, DhtClient, DhtConfig, DhtService, Network, NetworkConfig, OverlayService, PeerId,
    PeerInfo, Router, ToSocket,
};
use tycho_util::time::now_sec;

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
    *peer_info.signature = keypair.sign(&peer_info);
    peer_info
}

// TODO receive configured services from general node,
//  move current setup to tests as it provides acceptable timing
// This dependencies should be passed from validator module to init mempool
pub fn from_validator<T: ToSocket>(
    bind_address: T,
    secret_key: &SecretKey,
    remote_addr: Option<Address>,
    dht_config: DhtConfig,
    network_config: NetworkConfig,
) -> (DhtClient, OverlayService) {
    let local_id = PeerId::from(PublicKey::from(secret_key));

    let (dht_tasks, dht_service) = DhtService::builder(local_id)
        .with_config(dht_config)
        .build();

    let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
        .with_dht_service(dht_service.clone())
        .build();

    let router = Router::builder()
        .route(dht_service.clone())
        .route(overlay_service.clone())
        .build();

    let mut network_builder = Network::builder()
        .with_config(network_config)
        .with_private_key(secret_key.to_bytes())
        .with_service_name("mempool-test-network-service");
    if let Some(remote_addr) = remote_addr {
        network_builder = network_builder.with_remote_addr(remote_addr);
    }

    let network = network_builder.build(bind_address, router).unwrap();

    dht_tasks.spawn(&network);
    overlay_tasks.spawn(&network);

    (dht_service.make_client(&network), overlay_service)
}