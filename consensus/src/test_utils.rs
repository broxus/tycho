use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::Arc;

use everscale_crypto::ed25519::{KeyPair, PublicKey, SecretKey};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use tycho_network::{
    Address, DhtClient, DhtConfig, DhtService, Network, NetworkConfig, OverlayService, PeerId,
    PeerInfo, Router, ToSocket,
};
use tycho_util::time::now_sec;
use tycho_util::FastHashMap;

use crate::effects::AltFormat;
use crate::engine::MempoolConfig;
use crate::models::{Link, Location, Point, PointBody, PointId, Round, UnixTime};

const GENESIS_SECRET_KEY_BYTES: [u8; 32] = [0xAE; 32];
const GENESIS_MILLIS: u64 = 1713225727398;

// TODO this must be passed via config file
pub fn genesis_point_id() -> PointId {
    genesis().id()
}

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
        anchor_time: UnixTime::from_millis(GENESIS_MILLIS),
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

type PeerToAnchor = FastHashMap<PeerId, PointId>;
pub async fn check_anchors(
    mut committed: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>,
    peer_id: PeerId,
    anchors_hashmap: Arc<Mutex<FastHashMap<Round, PeerToAnchor>>>,
    known_round_refs: Arc<Mutex<FastHashMap<Round, Vec<PointId>>>>,
    nodes_count: usize,
) {
    loop {
        let (anchor, refs) = committed
            .recv()
            .await
            .expect("committed anchor reader must be alive");
        let anchor_id = anchor.id();

        let anchor_round = anchor.body.location.round;
        let mut guard = anchors_hashmap.lock().await;

        // get last previous anchor round and check if we don't have previous
        guard.iter().for_each(|(key, value)| {
            if key < &anchor_round {
                tracing::info!(
                    "Checking consistency of prev round {:?} for node {}",
                    &key,
                    peer_id.alt()
                );
                if value.get(&peer_id).is_none() {
                    panic!(
                        "Missing anchor for node {} at {:?} but received newer anchor {:?}",
                        peer_id.alt(),
                        key,
                        &anchor_round
                    );
                }
            }
        });

        match guard.entry(anchor_round) {
            Occupied(mut round_peers_entry) => {
                let round_peers = round_peers_entry.get_mut();
                if round_peers.len() >= nodes_count {
                    panic!("We have more anchors than nodes at round: {anchor_round:?}");
                }

                if let Some(point) = round_peers.get(&peer_id) {
                    panic!(
                        "We have already anchor {:?} for round: {anchor_round:?} and node {}",
                        point.location,
                        peer_id.alt()
                    );
                }

                round_peers.insert(peer_id, anchor_id);
            }
            Vacant(entry) => {
                let mut peer_map = FastHashMap::default();
                peer_map.insert(peer_id, anchor_id);
                entry.insert(peer_map);
            }
        }

        let mut refs_guard = known_round_refs.lock().await;
        match refs_guard.get(&anchor_round) {
            Some(round) => {
                if round.len() != refs.len() {
                    panic!("Commited points size differs for round: {anchor_round:?}");
                }

                for (rr, rf) in round.iter().zip(refs.iter()) {
                    if rr.digest != rf.digest {
                        panic!(
                            "Points are not equal or order is different for round {anchor_round:?}"
                        )
                    }
                }
            }
            None => {
                let point_refs = refs.iter().map(|x| x.id()).collect::<Vec<_>>();
                refs_guard.insert(anchor_round, point_refs);
            }
        }

        guard.retain(|key, value| {
            if value.len() == nodes_count {
                refs_guard.remove(key);
                false
            } else {
                true
            }
        });

        tracing::debug!("Anchor hashmap len: {}", guard.len());
        tracing::debug!("Refs hashmap ken: {}", refs_guard.len());

        drop(guard);
        drop(refs_guard);
    }
}

pub async fn drain_anchors(mut committed: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>) {
    loop {
        _ = committed
            .recv()
            .await
            .expect("committed anchor reader must be alive");
    }
}
