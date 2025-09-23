use std::array;
use std::convert::identity;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::FutureExt;
use rand::RngCore;
use rand::prelude::SliceRandom;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{Network, OverlayId, PeerId, PrivateOverlay, Router};
use tycho_util::FastHashMap;

use crate::dag::{AnchorStage, DagRound, ValidateResult, Verifier};
use crate::effects::{Ctx, EngineCtx, MempoolRayon, RoundCtx, TaskTracker, ValidateCtx};
use crate::engine::MempoolConfig;
use crate::engine::round_watch::{Consensus, RoundWatch};
use crate::intercom::{Dispatcher, Downloader, InitPeers, PeerSchedule, Responder};
use crate::models::{
    AnchorStageRole, Cert, Digest, Link, PeerCount, Point, PointData, PointId, Round, Signature,
    Through, UnixTime,
};
use crate::storage::MempoolStore;

pub fn make_engine_parts<const PEER_COUNT: usize>(
    peers: &[(PeerId, Arc<KeyPair>); PEER_COUNT],
    local_keys: Arc<KeyPair>,
) -> (PeerSchedule, Downloader, Point, EngineCtx) {
    let network = Network::builder()
        .with_random_private_key()
        .build("0.0.0.0:0", Router::builder().build())
        .expect("network with unused stub socket");

    let mempool_rayon = MempoolRayon::new(NonZeroUsize::MIN).unwrap();

    let merged_conf = crate::test_utils::default_test_config();
    let conf = &merged_conf.conf;

    let private_overlay =
        PrivateOverlay::builder(*OverlayId::wrap(&[0; 32])).build(Responder::new(&mempool_rayon));

    let dispatcher = Dispatcher::new(&mempool_rayon, &network, &private_overlay);

    let task_tracker = TaskTracker::default();

    let genesis = merged_conf.genesis();

    let engine_ctx = EngineCtx::new(conf.genesis_round, conf, &task_tracker);

    // any peer id will be ok, network is not used
    let peer_schedule = PeerSchedule::new(local_keys, private_overlay, &task_tracker);
    let init_peers = InitPeers::new(peers.iter().map(|(id, _)| *id).collect());
    peer_schedule.init(&merged_conf, &init_peers);

    let stub_consensus_round = RoundWatch::<Consensus>::default();
    let stub_downloader = Downloader::new(
        &dispatcher,
        &peer_schedule,
        stub_consensus_round.receiver(),
        conf,
    );

    (peer_schedule, stub_downloader, genesis, engine_ctx)
}

#[allow(clippy::too_many_arguments, reason = "ok in test")]
pub async fn populate_points<const PEER_COUNT: usize>(
    dag_round: &DagRound,
    peers: &[(PeerId, Arc<KeyPair>); PEER_COUNT],
    local_keys: &Arc<KeyPair>,
    peer_schedule: &PeerSchedule,
    downloader: &Downloader,
    store: &MempoolStore,
    round_ctx: &RoundCtx,
    msg_count: usize,
    msg_bytes: usize,
) {
    let prev_dag_round = dag_round.prev().upgrade().expect("prev DAG round exists");
    let prev_points = prev_dag_round
        .select(|(_, loc)| {
            loc.versions
                .values()
                .map(|a| a.clone().now_or_never().expect("must be ready"))
                .map(|p| p.expect("shutdown"))
                .map(|p| p.valid().expect("must be valid").info().clone())
                .next()
        })
        .collect::<Vec<_>>();
    let last_proof = prev_points
        .iter()
        .map(|point| point.anchor_id(AnchorStageRole::Proof))
        .max_by_key(|anchor_id| anchor_id.round)
        .expect("last proof must exist");
    let last_trigger = prev_points
        .iter()
        .map(|point| point.anchor_id(AnchorStageRole::Trigger))
        .max_by_key(|anchor_id| anchor_id.round)
        .expect("last trigger must exist");
    let max_prev_time = prev_points
        .iter()
        .map(|point| point.time())
        .max()
        .expect("prev time must exist");
    let max_anchor_time = prev_points
        .iter()
        .map(|point| point.anchor_time())
        .max()
        .expect("prev anchor_time must exist");
    let includes = prev_points
        .iter()
        .map(|point| (*point.author(), *point.digest()))
        .collect::<FastHashMap<_, _>>();

    let mut points = FastHashMap::default();
    for idx in 0..PEER_COUNT {
        let point = point::<PEER_COUNT>(
            dag_round.round(),
            idx,
            peers,
            &includes,
            max_prev_time,
            max_anchor_time,
            dag_round.anchor_stage(),
            &last_proof,
            &last_trigger,
            msg_count,
            msg_bytes,
            round_ctx.conf(),
        );
        points.insert(*point.info().author(), point);
    }

    for point in points.values() {
        Point::parse(point.serialized().to_vec())
            .expect("point tl serde is broken")
            .expect("point integrity check is broken");
        Verifier::verify(point.info(), peer_schedule, round_ctx.conf()).expect("well-formed point");
        let validate_ctx = ValidateCtx::new(round_ctx, point.info());
        let validated = Verifier::validate(
            point.info().clone(),
            dag_round.downgrade(),
            downloader.clone(),
            store.clone(),
            Cert::default(),
            validate_ctx,
        )
        .await;
        assert!(
            matches!(validated, Ok(ValidateResult::Valid)),
            "expected valid point, got {validated:?}"
        );
    }

    for point in points.values() {
        dag_round
            .add_local(point, Some(local_keys), store, round_ctx)
            .await
            .expect("cancelled");
    }
}

#[allow(clippy::too_many_arguments, reason = "ok in test")]
fn point<const PEER_COUNT: usize>(
    round: Round,
    idx: usize,
    peers: &[(PeerId, Arc<KeyPair>); PEER_COUNT],
    includes: &FastHashMap<PeerId, Digest>,
    max_prev_time: UnixTime,
    max_anchor_time: UnixTime,
    anchor_stage: Option<&AnchorStage>,
    last_proof: &PointId,
    last_trigger: &PointId,
    msg_count: usize,
    msg_bytes: usize,
    conf: &MempoolConfig,
) -> Point {
    assert!(idx < PEER_COUNT, "peer index out of range");
    assert!(
        includes.len() == 1 || includes.len() == PEER_COUNT,
        "unexpected point count"
    );
    let peer_count = PeerCount::try_from(PEER_COUNT).expect("enough peers in non-genesis round");

    let evidence = match includes.get(&peers[idx].0) {
        Some(prev_digest) => {
            let mut evidence = FastHashMap::default();
            for i in &rand_arr::<PEER_COUNT>()[..(peer_count.majority_of_others() + 1)] {
                if *i == idx {
                    continue;
                }
                evidence.insert(peers[*i].0, Signature::new(&peers[*i].1, prev_digest));
            }
            evidence
        }
        None => FastHashMap::default(),
    };

    let mut payload = Vec::with_capacity(msg_count);
    for _ in 0..msg_count {
        let mut data = vec![0; msg_bytes];
        rand::rng().fill_bytes(data.as_mut_slice());
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

    let anchor_time = if anchor_proof == Link::ToSelf {
        max_prev_time
    } else {
        max_anchor_time
    };

    Point::new(
        &peers[idx].1,
        peers[idx].0,
        round,
        &payload,
        PointData {
            time: max_prev_time.next(),
            includes: includes.clone(),
            witness: Default::default(),
            evidence,
            anchor_trigger,
            anchor_proof,
            anchor_time,
        },
        conf,
    )
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
                    to: *last_same_stage_point,
                    path: Through::Includes(peer),
                }
            }
        }
    }
}

fn rand_arr<const N: usize>() -> [usize; N] {
    let mut arr: [usize; N] = array::from_fn(identity);
    arr.shuffle(&mut rand::rng());
    arr
}
