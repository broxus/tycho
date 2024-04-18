use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use everscale_crypto::ed25519::{KeyPair, PublicKey, SecretKey};

use tycho_network::{DhtClient, DhtConfig, DhtService, Network, OverlayService, PeerId, Router};

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

// TODO receive configured services from general node,
//  move current setup to tests as it provides acceptable timing
// This dependencies should be passed from validator module to init mempool
fn from_validator<T: ToSocketAddrs>(
    socket_addr: T,
    secret_key: &SecretKey,
) -> (DhtClient, OverlayService) {
    let local_id = PeerId::from(PublicKey::from(secret_key));

    let (dht_tasks, dht_service) = DhtService::builder(local_id)
        .with_config(DhtConfig {
            local_info_announce_period: Duration::from_secs(1),
            local_info_announce_period_max_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            routing_table_refresh_period_max_jitter: Duration::from_secs(1),
            ..Default::default()
        })
        .build();

    let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
        .with_dht_service(dht_service.clone())
        .build();

    let router = Router::builder()
        .route(dht_service.clone())
        .route(overlay_service.clone())
        .build();

    let network = Network::builder()
        .with_private_key(secret_key.to_bytes())
        .with_service_name("mempool-test-network-service")
        .build(socket_addr, router)
        .unwrap();

    dht_tasks.spawn(&network);
    overlay_tasks.spawn(&network);

    (dht_service.make_client(&network), overlay_service)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::thread;
    use std::time::Duration;

    use parking_lot::deadlock;
    use tokio::task::JoinSet;

    use tycho_network::{Address, PeerInfo};
    use tycho_util::time::now_sec;

    use crate::engine::Engine;

    use super::*;

    fn make_peer_info(key: &SecretKey, address: Address) -> PeerInfo {
        let keypair = KeyPair::from(key);
        let peer_id = PeerId::from(keypair.public_key);

        let now = now_sec();
        let mut peer_info = PeerInfo {
            id: peer_id,
            address_list: vec![address.clone()].into_boxed_slice(),
            created_at: now,
            expires_at: u32::MAX,
            signature: Box::new([0; 64]),
        };
        *peer_info.signature = keypair.sign(&peer_info);
        peer_info
    }

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
            .map(|secret| from_validator((Ipv4Addr::LOCALHOST, 0), secret))
            .collect::<Vec<_>>();

        let peer_info = std::iter::zip(&keys, &from_validators)
            .map(|(key, (dht_client, _))| {
                Arc::new(make_peer_info(
                    key,
                    dht_client.network().local_addr().into(),
                ))
            })
            .collect::<Vec<_>>();

        for (dht_client, _) in from_validators.iter() {
            for info in &peer_info {
                if info.id == dht_client.network().peer_id() {
                    continue;
                }
                assert!(dht_client.add_peer(info.clone()).unwrap(), "peer added");
            }
        }
        let mut engines = vec![];
        for (secret_key, (dht_client, overlay_service)) in keys.iter().zip(from_validators.iter()) {
            let engine = Engine::new(secret_key, &dht_client, &overlay_service, &all_peers).await;
            tracing::info!("created engine {}", dht_client.network().peer_id());
            engines.push(engine);
        }

        engines
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_works() -> Result<(), ()> {
        // tracing_subscriber::fmt::try_init().ok();
        // tracing::info!("engine_works");
        tycho_util::test::init_logger("engine_works");

        check_parking_lot();
        heart_beat();
        let mut js = JoinSet::new();
        for engine in make_network(3).await {
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
