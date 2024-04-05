use std::net::Ipv4Addr;
use std::time::Duration;

use everscale_crypto::ed25519;

use tycho_network::{DhtConfig, DhtService, Network, OverlayService, PeerId, Router};

use crate::types::NodeNetwork;

pub fn try_init_test_tracing(level_filter: tracing_subscriber::filter::LevelFilter) {
    use std::io::IsTerminal;
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .from_env_lossy(),
        )
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        //.with_thread_ids(true)
        .compact()
        .try_init()
        .ok();
}

pub fn create_node_network() -> NodeNetwork {
    let random_secret_key = ed25519::SecretKey::generate(&mut rand::thread_rng());
    let keypair = ed25519::KeyPair::from(&random_secret_key);
    let local_id = PeerId::from(keypair.public_key);
    let (_, overlay_service) = OverlayService::builder(local_id).build();

    let router = Router::builder().route(overlay_service.clone()).build();
    let network = Network::builder()
        .with_private_key(random_secret_key.to_bytes())
        .with_service_name("test-service")
        .build((Ipv4Addr::LOCALHOST, 0), router)
        .unwrap();

    let (_, dht_service) = DhtService::builder(local_id)
        .with_config(DhtConfig {
            local_info_announce_period: Duration::from_secs(1),
            max_local_info_announce_period_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            max_routing_table_refresh_period_jitter: Duration::from_secs(1),
            ..Default::default()
        })
        .build();

    let dht_client = dht_service.make_client(&network);
    let peer_resolver = dht_service.make_peer_resolver().build(&network);

    NodeNetwork {
        overlay_service,
        peer_resolver,
        dht_client,
    }
}
