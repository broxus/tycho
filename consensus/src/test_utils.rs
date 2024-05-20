use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, PublicKey, SecretKey};
use tokio::sync::mpsc::UnboundedReceiver;
use tycho_network::{
    Address, DhtClient, DhtConfig, DhtService, Network, NetworkConfig, OverlayService, PeerId,
    PeerInfo, Router, ToSocket,
};
use tycho_util::time::now_sec;

use crate::engine::MempoolConfig;
use crate::models::{Link, Location, Point, PointBody, UnixTime};

const GENESIS_SECRET_KEY_BYTES: [u8; 32] = [0xAE; 32];
const GENESIS_MILLIS: u64 = 1713225727398;

pub fn genesis() -> Arc<Point> {
    let genesis_keys = KeyPair::from(&SecretKey::from_bytes(GENESIS_SECRET_KEY_BYTES));

    Point::new(&genesis_keys, PointBody {
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
    })
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

pub async fn drain_anchors(mut committed: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>) {
    loop {
        _ = committed
            .recv()
            .await
            .expect("committed anchor reader must be alive");
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::deadlock;
    use tokio::sync::mpsc;

    use super::*;
    use crate::engine::{Engine, InputBufferStub};

    #[global_allocator]
    static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

    fn make_network(
        node_count: usize,
        workers_per_node: usize,
    ) -> Vec<std::thread::JoinHandle<()>> {
        let keys = (0..node_count)
            .map(|_| SecretKey::generate(&mut rand::thread_rng()))
            .map(|secret| (secret, Arc::new(KeyPair::from(&secret))))
            .collect::<Vec<_>>();

        let all_peers = keys
            .iter()
            .map(|(_, kp)| PeerId::from(kp.public_key))
            .collect::<Vec<_>>();

        let addresses = keys
            .iter()
            .map(|_| {
                std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                    .expect("bind udp socket")
                    .local_addr()
                    .expect("local address")
                    .into()
            })
            .collect::<Vec<Address>>();

        let peer_info = keys
            .iter()
            .zip(addresses.iter())
            .map(|((_, key_pair), addr)| {
                Arc::new(make_peer_info(key_pair, vec![addr.clone()], None))
            })
            .collect::<Vec<_>>();

        let mut handles = vec![];
        for (((secret_key, key_pair), address), peer_id) in keys
            .into_iter()
            .zip(addresses.into_iter())
            .zip(peer_info.iter().map(|p| p.id))
        {
            let all_peers = all_peers.clone();
            let peer_info = peer_info.clone();
            let handle = std::thread::spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(workers_per_node)
                    .thread_name(format!("tokio-runtime-{peer_id:.4?}"))
                    .build()
                    .expect("new tokio runtime")
                    .block_on(async move {
                        let (dht_client, overlay_service) = from_validator(
                            address,
                            &secret_key,
                            DhtConfig {
                                local_info_announce_period: Duration::from_secs(1),
                                local_info_announce_period_max_jitter: Duration::from_secs(1),
                                routing_table_refresh_period: Duration::from_secs(1),
                                routing_table_refresh_period_max_jitter: Duration::from_secs(1),
                                ..Default::default()
                            },
                            NetworkConfig::default(),
                        );
                        for info in &peer_info {
                            if info.id != dht_client.network().peer_id() {
                                dht_client
                                    .add_peer(info.clone())
                                    .expect("add peer to dht client");
                            }
                        }

                        let (committed_tx, committed_rx) = mpsc::unbounded_channel();
                        tokio::spawn(drain_anchors(committed_rx));
                        let mut engine = Engine::new(
                            key_pair,
                            &dht_client,
                            &overlay_service,
                            committed_tx.clone(),
                            InputBufferStub::new(100, 3),
                        );
                        engine.init_with_genesis(all_peers.as_slice()).await;
                        tracing::info!("created engine {}", dht_client.network().peer_id());
                        engine.run().await;
                    });
            });
            handles.push(handle);
        }
        handles
    }

    #[test]
    fn engine_works() -> Result<(), anyhow::Error> {
        // tracing_subscriber::fmt::try_init().ok();
        // tracing::info!("engine_works");
        tycho_util::test::init_logger(
            "engine_works",
            "info,tycho_consensus=info,tycho_network=info",
        );

        // check_parking_lot();
        heart_beat();
        let handles = make_network(21, 2);
        for handle in handles {
            handle.join().unwrap();
        }
        Ok(())
    }

    pub fn check_parking_lot() {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
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
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(1));
            tracing::info!("heart beat");
        });
    }
}
