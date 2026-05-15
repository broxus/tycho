use std::array;
use std::convert::identity;
use std::sync::Arc;

use ahash::HashMapExt;
use futures_util::FutureExt;
use rand::prelude::SliceRandom;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{Network, OverlayId, PeerId, PrivateOverlay, Router};
use tycho_util::FastHashMap;

use crate::dag::{DagRound, LastOwnPoint, Producer, ValidateResult, Verifier};
use crate::effects::{Ctx, EngineCtx, MempoolRayon, RoundCtx, TaskTracker, ValidateCtx};
use crate::engine::{InputBuffer, MempoolConfig};
use crate::intercom::{Dispatcher, Downloader, InitPeers, PeerSchedule, Responder};
use crate::models::{Cert, PeerCount, Point, PointInfo, Round, Signature};
use crate::moderator::Moderator;
use crate::storage::MempoolStore;

pub fn make_engine_parts<const PEER_COUNT: usize>(
    peers: &[(PeerId, Arc<KeyPair>); PEER_COUNT],
    local_keys: Arc<KeyPair>,
) -> (PeerSchedule, Downloader, Point, EngineCtx) {
    let network = Network::builder()
        .with_random_private_key()
        .build("0.0.0.0:0", Router::builder().build())
        .expect("network with unused stub socket");

    let private_overlay =
        PrivateOverlay::builder(*OverlayId::wrap(&[0; 32])).build(Responder::default());

    let task_tracker = TaskTracker::default();

    let merged_conf = crate::test_utils::default_test_config();
    let conf = &merged_conf.conf;
    let genesis = merged_conf.genesis();

    let dispatcher = Dispatcher::new(&network, &private_overlay, &Moderator::new_stub(), conf);

    let rayon = MempoolRayon::new(merged_conf.node_config().rayon_threads)
        .expect("failed to create mempool rayon thread pool");

    let engine_ctx = EngineCtx::new(conf.genesis_round, conf, &task_tracker, &rayon);

    // any peer id will be ok, network is not used
    let peer_schedule = PeerSchedule::new(local_keys, private_overlay);
    let init_peers = InitPeers::new(peers.iter().map(|(id, _)| *id).collect());
    peer_schedule.init(&merged_conf, &init_peers);

    let stub_downloader = Downloader::new(&dispatcher, &peer_schedule);

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
    input_buffer: &InputBuffer,
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

    let mut points = FastHashMap::default();
    for idx in 0..PEER_COUNT {
        let point = point::<PEER_COUNT>(
            dag_round.round(),
            idx,
            peers,
            &prev_points,
            dag_round.leader(),
            input_buffer,
            round_ctx.conf(),
        );
        points.insert(*point.info().author(), point);
    }

    for point in points.values() {
        Point::parse(point.serialized().to_vec())
            .expect("point tl serde is broken")
            .expect("point integrity check is broken")
            .expect("structure check is broken");
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
            "expected valid point, got {validated:?} for {:#?}; leader {:?}",
            point.info(),
            dag_round.leader(),
        );
    }

    for point in points.values() {
        dag_round
            .add_local(
                point,
                Some(local_keys.clone()),
                downloader.clone(),
                store.clone(),
                round_ctx,
            )
            .await
            .expect("cancelled");
    }
}

#[allow(clippy::too_many_arguments, reason = "ok in test")]
fn point<const PEER_COUNT: usize>(
    round: Round,
    idx: usize,
    peers: &[(PeerId, Arc<KeyPair>); PEER_COUNT],
    includes: &[PointInfo],
    round_leader: Option<&PeerId>,
    input_buffer: &InputBuffer,
    conf: &MempoolConfig,
) -> Point {
    assert!(idx < PEER_COUNT, "peer index out of range");
    assert!(
        includes.len() == 1 || includes.len() == PEER_COUNT,
        "unexpected point count"
    );
    let peer_count = PeerCount::try_from(PEER_COUNT).expect("enough peers in non-genesis round");

    let author = peers[idx].0;

    let prev_info = includes.iter().find(|info| info.author() == author);

    let last_own_point = prev_info.map(|info| LastOwnPoint {
        digest: *info.digest(),
        includes: info.includes().clone(),
        round: info.round(),
        signers: peer_count,
        evidence: {
            let mut evidence = FastHashMap::with_capacity(PEER_COUNT);
            for i in &rand_arr::<PEER_COUNT>()[..(peer_count.majority_of_others() + 1)] {
                if *i == idx {
                    continue;
                }
                evidence.insert(peers[*i].0, Signature::new(&peers[*i].1, info.digest()));
            }
            evidence
        },
    });

    Producer::create(
        last_own_point.as_ref(),
        input_buffer,
        &peers[idx].1,
        round,
        round_leader,
        includes.to_vec(),
        Default::default(),
        conf,
    )
    .expect("failed to create point")
}

fn rand_arr<const N: usize>() -> [usize; N] {
    let mut arr: [usize; N] = array::from_fn(identity);
    arr.shuffle(&mut rand::rng());
    arr
}
