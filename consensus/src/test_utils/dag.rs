use std::array;
use std::collections::BTreeMap;
use std::convert::identity;
use std::sync::Arc;

use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use futures_util::FutureExt;
use rand::prelude::SliceRandom;
use rand::{thread_rng, RngCore};
use tokio::sync::oneshot;
use tycho_network::{DhtService, Network, OverlayService, PeerId, Router};
use tycho_util::FastHashMap;

use crate::dag::{AnchorStage, Dag, DagRound, Verifier};
use crate::effects::{ChainedRoundsContext, Effects, EngineContext, MempoolStore, ValidateContext};
use crate::engine::MempoolConfig;
use crate::intercom::{Dispatcher, Downloader, PeerSchedule, Responder};
use crate::models::{
    AnchorStageRole, Digest, Link, PeerCount, Point, PointData, PointId, PointInfo, Round,
    Signature, Through, UnixTime,
};
use crate::test_utils;

pub fn make_dag<const PEER_COUNT: usize>(
    peers: &[(PeerId, KeyPair); PEER_COUNT],
    genesis: &Point,
    store: &MempoolStore,
) -> (Dag, PeerSchedule, Downloader) {
    let local_id = PeerId::from(peers[0].1.public_key);

    let (_, dht_service) = DhtService::builder(local_id).build();

    let (_, overlay_service) = OverlayService::builder(local_id)
        .with_dht_service(dht_service.clone())
        .build();

    let network_builder = Network::builder()
        .with_random_private_key()
        .with_service_name("mempool-stub-network-service");

    let network = network_builder
        .build("0.0.0.0:0", Router::builder().build())
        .expect("network with unused stub socket");

    let (dispatcher, overlay) = Dispatcher::new(
        &dht_service.make_client(&network),
        &overlay_service,
        Responder::default(),
    );

    let peer_schedule = PeerSchedule::new(Arc::new(peers[0].1), overlay);

    let stub_downloader = Downloader::new(&dispatcher, &peer_schedule);

    {
        let mut guard = peer_schedule.write();
        let peer_schedule = peer_schedule.clone();
        guard.set_next_start(MempoolConfig::genesis_round(), &peer_schedule);
        guard.set_next_peers(
            &[test_utils::genesis_point_id().author],
            &peer_schedule,
            false,
        );
        guard.rotate(&peer_schedule);
        // current epoch
        guard.set_next_start(genesis.round().next(), &peer_schedule);
        // start updater only after peers are populated into schedule
        guard.set_next_peers(
            &peers.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            &peer_schedule,
            true,
        );
        guard.rotate(&peer_schedule);
    }
    let genesis_round = DagRound::genesis(genesis, &peer_schedule);
    let next_dag_round = genesis_round.next(&peer_schedule);

    let _ = genesis_round.insert_exact_sign(genesis, next_dag_round.key_pair(), store);

    let mut dag = Dag::default();

    dag.init(genesis_round.clone(), genesis_round.next(&peer_schedule));

    (dag, peer_schedule, stub_downloader)
}

#[allow(clippy::too_many_arguments)] // ok in test
pub async fn populate_dag<const PEER_COUNT: usize>(
    peers: &[(PeerId, KeyPair); PEER_COUNT],
    peer_schedule: &PeerSchedule,
    downloader: &Downloader,
    store: &MempoolStore,
    effects: &Effects<ChainedRoundsContext>,
    genesis: &Point,
    msg_count: usize,
    msg_bytes: usize,
    dag_rounds: &mut BTreeMap<Round, DagRound>,
) {
    let mut prev_points = BTreeMap::default();
    let mut last_candidate = PointInfo::from(genesis);
    let mut last_proof = genesis.id();
    let mut last_trigger = genesis.id();
    prev_points.insert(genesis.data().author, genesis.digest().clone());
    for dag_round in dag_rounds.values().skip(1) {
        if let Some(AnchorStage {
            role: AnchorStageRole::Proof,
            leader,
            ..
        }) = dag_round.anchor_stage()
        {
            let prev_round = dag_round.prev().upgrade().expect("strong dag round ref");
            last_candidate = prev_round
                .view(leader, |loc| {
                    loc.versions()
                        .first_key_value()
                        .and_then(|(_, candidate)| candidate.clone().now_or_never())
                        .and_then(|candidate| candidate.into_valid())
                        .expect("ready and valid anchor candidate dag point future")
                        .info
                })
                .expect("populated anchor candidate");
        }

        let mut points = FastHashMap::default();
        for idx in 0..PEER_COUNT {
            let point = point::<PEER_COUNT>(
                dag_round.round(),
                idx,
                peers,
                &prev_points,
                dag_round.anchor_stage(),
                &last_candidate,
                &last_proof,
                &last_trigger,
                msg_count,
                msg_bytes,
            );
            points.insert(point.data().author, point);
        }
        match dag_round.anchor_stage() {
            Some(AnchorStage {
                role: AnchorStageRole::Proof,
                leader,
                ..
            }) => {
                last_proof = points
                    .get(leader)
                    .expect("anchor proof in passed includes")
                    .id();
            }
            Some(AnchorStage {
                role: AnchorStageRole::Trigger,
                leader,
                ..
            }) => {
                last_trigger = points
                    .get(leader)
                    .expect("anchor trigger in passed includes")
                    .id();
            }
            None => {}
        }
        let effects = Effects::<EngineContext>::new(effects, dag_round.round());
        for point in points.values() {
            Verifier::verify(point, peer_schedule).expect("well-formed point");
            let (_, certified_tx) = oneshot::channel();
            let effects = Effects::<ValidateContext>::new(&effects, point);
            Verifier::validate(
                point.clone(),
                dag_round.downgrade(),
                downloader.clone(),
                store.clone(),
                certified_tx,
                effects,
            )
            .await
            .trusted()
            .expect("trusted point");
        }

        for point in points.values() {
            _ = dag_round.insert_exact_sign(point, Some(&peers[0].1), store);
        }
        prev_points = points
            .into_iter()
            .map(|(author, point)| (author, point.digest().clone()))
            .collect();
    }
}

#[allow(clippy::too_many_arguments)] // ok in test
pub fn point<const PEER_COUNT: usize>(
    round: Round,
    idx: usize,
    peers: &[(PeerId, KeyPair); PEER_COUNT],
    prev_points: &BTreeMap<PeerId, Digest>,
    anchor_stage: Option<&AnchorStage>,
    last_candidate: &PointInfo,
    last_proof: &PointId,
    last_trigger: &PointId,
    msg_count: usize,
    msg_bytes: usize,
) -> Point {
    assert!(idx < PEER_COUNT, "peer index out of range");
    assert!(
        prev_points.len() == 1 || prev_points.len() == PEER_COUNT,
        "unexpected point count"
    );
    let peer_count = PeerCount::try_from(PEER_COUNT).expect("enough peers in non-genesis round");

    let (prev_digest, evidence) = match prev_points.get(&peers[idx].0) {
        Some(prev_point) => {
            let mut evidence = BTreeMap::default();
            for i in &rand_arr::<PEER_COUNT>()[..(peer_count.majority_of_others() + 1)] {
                if *i == idx {
                    continue;
                }
                evidence.insert(peers[*i].0, Signature::new(&peers[*i].1, prev_point));
            }
            (Some(prev_point.clone()), Some(evidence))
        }
        None => (None, None),
    };

    let mut payload = Vec::with_capacity(msg_count);
    for _ in 0..msg_count {
        let mut data = vec![0; msg_bytes];
        thread_rng().fill_bytes(data.as_mut_slice());
        payload.push(Bytes::from(data));
    }

    let anchor_proof = point_anchor_link(
        round,
        peers[idx].0,
        anchor_stage,
        last_proof,
        AnchorStageRole::Proof,
    );

    let anchor_trigger = point_anchor_link(
        round,
        peers[idx].0,
        anchor_stage,
        last_trigger,
        AnchorStageRole::Trigger,
    );

    Point::new(&peers[idx].1, round, evidence, payload, PointData {
        author: peers[idx].0,
        time: last_candidate.data().time.max(UnixTime::now()),
        prev_digest,
        includes: prev_points.clone(),
        witness: Default::default(),
        anchor_trigger,
        anchor_proof,
        anchor_time: last_candidate.data().time,
    })
}

fn point_anchor_link(
    round: Round,
    peer: PeerId,
    anchor_stage: Option<&AnchorStage>,
    last_same_stage_point: &PointId,
    role: AnchorStageRole,
) -> Link {
    match anchor_stage {
        Some(stage) if stage.role == role && stage.leader == peer => Link::ToSelf,
        _ => {
            if last_same_stage_point.round == round.prev() {
                Link::Direct(Through::Includes(last_same_stage_point.author))
            } else {
                Link::Indirect {
                    to: last_same_stage_point.clone(),
                    path: Through::Includes(peer),
                }
            }
        }
    }
}

fn rand_arr<const N: usize>() -> [usize; N] {
    let mut arr: [usize; N] = array::from_fn(identity);
    arr.shuffle(&mut thread_rng());
    arr
}
