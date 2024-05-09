use std::net::ToSocketAddrs;
use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, PublicKey, SecretKey};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use tycho_network::{
    Address, DhtClient, DhtConfig, DhtService, Network, NetworkConfig, OverlayService, PeerId,
    PeerInfo, Router, ToSocket,
};
use tycho_util::time::now_sec;

use crate::engine::MempoolConfig;
use crate::models::{Link, Location, Point, PointBody, UnixTime};

const GENESIS_SECRET_KEY_BYTES: [u8; 32] = [0xAE; 32];
const GENESIS_MILLIS: u64 = 1713225727398;

pub fn genesis() -> Point {
    let genesis_keys = KeyPair::from(&SecretKey::from_bytes(GENESIS_SECRET_KEY_BYTES));

    PointBody {
        location: Location {
            round: MempoolConfig::GENESIS_ROUND,
            author: genesis_keys.public_key.into(),
        },
        time: UnixTime::from_millis(GENESIS_MILLIS),
        payload: vec![],
        proof: None,
        includes: Default::default(),
        witness: Default::default(),
        anchor_trigger: Link::ToSelf,
        anchor_proof: Link::ToSelf,
    }
    .wrap(&genesis_keys)
}

pub fn make_peer_info(key: &SecretKey, address: Address, ttl: Option<u32>) -> PeerInfo {
    let keypair = KeyPair::from(key);
    let peer_id = PeerId::from(keypair.public_key);

    let now = now_sec();
    let mut peer_info = PeerInfo {
        id: peer_id,
        address_list: vec![address.clone()].into_boxed_slice(),
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
    socket_addr: T,
    secret_key: &SecretKey,
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

    let network = Network::builder()
        .with_config(network_config)
        .with_private_key(secret_key.to_bytes())
        .with_service_name("mempool-test-network-service")
        .build(socket_addr, router)
        .unwrap();

    dht_tasks.spawn(&network);
    overlay_tasks.spawn(&network);

    (dht_service.make_client(&network), overlay_service)
}

pub fn drain_anchors(
    mut committed: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            _ = committed
                .recv()
                .await
                .expect("committed anchor reader must be alive");
        }
    })
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use parking_lot::deadlock;
    use tokio::sync::mpsc;
    use tokio::task::JoinSet;

    use crate::engine::Engine;

    use super::*;

    async fn make_network(node_count: usize) -> Vec<Engine> {
        let keys = (0..node_count)
            .map(|_| SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();

        let all_peers = keys
            .iter()
            .map(|s| PeerId::from(KeyPair::from(s).public_key))
            .collect::<Vec<_>>();

        let from_validators = keys
            .iter()
            .map(|secret| {
                from_validator(
                    (Ipv4Addr::LOCALHOST, 0),
                    secret,
                    DhtConfig {
                        local_info_announce_period: Duration::from_secs(1),
                        local_info_announce_period_max_jitter: Duration::from_secs(1),
                        routing_table_refresh_period: Duration::from_secs(1),
                        routing_table_refresh_period_max_jitter: Duration::from_secs(1),
                        ..Default::default()
                    },
                    NetworkConfig::default(),
                )
            })
            .collect::<Vec<_>>();

        let peer_info = std::iter::zip(&keys, &from_validators)
            .map(|(key, (dht_client, _))| {
                Arc::new(make_peer_info(
                    key,
                    dht_client.network().local_addr().into(),
                    None,
                ))
            })
            .collect::<Vec<_>>();

        for (dht_client, _) in from_validators.iter() {
            for info in &peer_info {
                if info.id == dht_client.network().peer_id() {
                    continue;
                }
                dht_client
                    .add_peer(info.clone())
                    .expect("add peer to dht client");
            }
        }
        let mut engines = vec![];
        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
        for (secret_key, (dht_client, overlay_service)) in keys.iter().zip(from_validators.iter()) {
            let engine = Engine::new(
                secret_key,
                &dht_client,
                &overlay_service,
                &all_peers,
                committed_tx.clone(),
            )
            .await;
            tracing::info!("created engine {}", dht_client.network().peer_id());
            engines.push(engine);
        }
        drain_anchors(committed_rx);

        engines
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_works() -> Result<(), ()> {
        // tracing_subscriber::fmt::try_init().ok();
        // tracing::info!("engine_works");
        tycho_util::test::init_logger("engine_works", "info,tycho_consensus=debug");

        check_parking_lot();
        heart_beat();
        let mut js = JoinSet::new();
        for engine in make_network(4).await {
            js.spawn(engine.run());
        }
        while let Some(res) = js.join_next().await {
            res.unwrap();
        }
        Ok(())
    }

    pub fn check_parking_lot() {
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(10));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            tracing::error!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                tracing::error!("Deadlock #{}", i);
                for t in threads {
                    tracing::error!("Thread Id {:#?}", t.thread_id());
                    tracing::error!("{:#?}", t.backtrace());
                }
            }
        });
    }

    pub fn heart_beat() {
        // Create a background thread which checks for deadlocks every 10s
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(1));
            tracing::info!("heart beat");
        });
    }
}
