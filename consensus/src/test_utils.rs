use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use everscale_crypto::ed25519::{KeyPair, PublicKey, SecretKey};
use tokio::sync::mpsc;

use tycho_network::{DhtClient, DhtConfig, DhtService, Network, OverlayService, PeerId, Router};

use crate::intercom::{BroadcastFilter, Dispatcher, PeerSchedule, PeerScheduleUpdater, Responder};
use crate::models::{Link, Location, Point, PointBody, Round, UnixTime};

const GENESIS_SECRET_KEY_BYTES: [u8; 32] = [0xAE; 32];
const GENESIS_MILLIS: u64 = 0;
const GENESIS_ROUND: u32 = 0;

pub fn genesis() -> Point {
    let genesis_keys = KeyPair::from(&SecretKey::from_bytes(GENESIS_SECRET_KEY_BYTES));

    PointBody {
        location: Location {
            round: Round(GENESIS_ROUND),
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

pub async fn bootstrap<T: ToSocketAddrs>(
    secret_key: &SecretKey,
    dht_client: &DhtClient,
    overlay_service: &OverlayService,
    peers: &Vec<PeerId>,
) {
    let peer_schedule = Arc::new(PeerSchedule::new(Arc::new(KeyPair::from(secret_key))));

    let (bcast_tx, bcast_rx) = mpsc::unbounded_channel();

    let broadcast_filter = BroadcastFilter::new(peer_schedule.clone(), bcast_tx);

    let (sig_requests, sig_responses) = mpsc::unbounded_channel();

    let dispatcher = Dispatcher::new(
        &dht_client,
        &overlay_service,
        peers,
        Responder::new(broadcast_filter.clone(), sig_requests),
    );

    let genesis = Arc::new(crate::test_utils::genesis());
    // finished epoch
    peer_schedule.set_next_peers(&vec![genesis.body.location.author]);
    peer_schedule.set_next_start(genesis.body.location.round);
    peer_schedule.rotate();
    // current epoch
    peer_schedule.set_next_start(genesis.body.location.round.next());
    peer_schedule.set_next_peers(peers);
    peer_schedule.rotate();
    // start updater only after peers are populated into schedule
    PeerScheduleUpdater::run(dispatcher.overlay.clone(), peer_schedule.clone());

    // tOdO define if the last round is finished based on peer schedule
    //   move out from bcaster & signer ? where to get our last point from ?

    // tOdO в конце каждого раунда берем точку с триггером
    //  и комиттим
    //  * either own point contains Trigger
    //  * or search through last round to find the latest trigger
    //   * * can U do so without scan of a round ???
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
            max_local_info_announce_period_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            max_routing_table_refresh_period_jitter: Duration::from_secs(1),
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

    (dht_service.make_client(network.clone()), overlay_service)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use futures_util::stream::FuturesUnordered;
    use tokio::task::JoinSet;

    use tycho_network::{Address, PeerInfo};
    use tycho_util::time::now_sec;

    use crate::engine::Engine;
    use crate::engine::EngineTestExt;
    use crate::intercom::DispatcherTestExt;

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

        let mut engines = vec![];
        for (secret_key, (dht_client, overlay_service)) in keys.iter().zip(from_validators.iter()) {
            let engine = Engine::new(secret_key, &dht_client, &overlay_service, &all_peers).await;
            engines.push(engine);
        }

        let peer_info = std::iter::zip(&keys, &engines)
            .map(|(key, engine)| {
                Arc::new(make_peer_info(
                    key,
                    engine.dispatcher().network().local_addr().into(),
                ))
            })
            .collect::<Vec<_>>();

        if let Some((dht_client, _)) = from_validators.first() {
            for info in &peer_info {
                if info.id == dht_client.network().peer_id() {
                    continue;
                }
                dht_client.add_peer(info.clone()).unwrap();
            }
        }
        engines
    }

    #[tokio::test]
    async fn engine_works() -> Result<(), ()> {
        tracing_subscriber::fmt::try_init().ok();
        tracing::info!("engine_works");

        let mut js = JoinSet::new();
        let engines = make_network(3)
            .await
            .into_iter()
            .map(|engine| js.spawn(engine.run()))
            .collect::<FuturesUnordered<_>>();
        while let Some(res) = js.join_next().await {
            res.unwrap();
        }
        Ok(())
    }
}
