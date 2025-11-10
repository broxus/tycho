use std::net::Ipv4Addr;
use std::time::Duration;

use tycho_collator::validator::ValidatorNetworkContext;
use tycho_crypto::ed25519;
use tycho_network::{DhtConfig, DhtService, Network, OverlayService, PeerId, Router};
use tycho_types::models::BlockId;

#[allow(dead_code)]
pub fn make_validator_network(
    secret_key: &ed25519::SecretKey,
    zerostate_id: &BlockId,
) -> ValidatorNetworkContext {
    let public_key = ed25519::PublicKey::from(secret_key);
    let local_id = PeerId::from(public_key);

    let (_, overlay_service) = OverlayService::builder(local_id).build();

    let (_, dht_service) = DhtService::builder(local_id)
        .with_config(DhtConfig {
            local_info_announce_period: Duration::from_secs(1),
            local_info_announce_period_max_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            routing_table_refresh_period_max_jitter: Duration::from_secs(1),
            ..Default::default()
        })
        .build();

    let router = Router::builder()
        .route(overlay_service.clone())
        .route(dht_service.clone())
        .build();

    let network = Network::builder()
        .with_private_key(secret_key.to_bytes())
        .build((Ipv4Addr::LOCALHOST, 0), router)
        .unwrap();

    let peer_resolver = dht_service.make_peer_resolver().build(&network);

    ValidatorNetworkContext {
        network,
        peer_resolver,
        overlays: overlay_service,
        zerostate_id: *zerostate_id,
    }
}
