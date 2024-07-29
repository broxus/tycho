use futures_util::FutureExt;
use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::intercom::dto::PointByIdResponse;
use crate::models::PointId;

pub struct Uploader;

impl Uploader {
    pub fn find(
        peer_id: &PeerId,
        point_id: &PointId,
        top_dag_round: &DagRound,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> PointByIdResponse {
        if point_id.location.round > top_dag_round.round() {
            PointByIdResponse::TryLater
        } else {
            match top_dag_round.scan(point_id.location.round) {
                Some(dag_round) => {
                    Self::from_dag(peer_id, point_id, top_dag_round, &dag_round, effects)
                }
                None => Self::from_store(peer_id, point_id, store, effects),
                // Fixme return serialized as bytes from DB!
            }
        }
    }

    fn from_dag(
        peer_id: &PeerId,
        point_id: &PointId,
        top_dag_round: &DagRound,
        dag_round: &DagRound,
        effects: &Effects<EngineContext>,
    ) -> PointByIdResponse {
        let found = dag_round
            .view(&point_id.location.author, |loc| {
                loc.versions().get(&point_id.digest).cloned()
            })
            .flatten();
        let task_found = found.is_some();
        let ready = found.and_then(|shared| shared.now_or_never());

        let level = if let Some(dag_point) = ready.as_ref() {
            if dag_point.trusted().is_some() {
                // just fine
                tracing::Level::TRACE
            } else {
                // either some misbehaving author produced truly invalid point,
                // or we trail some point marked as `Invalid` through our history
                // and don't trust newer consensus signatures
                tracing::Level::ERROR
            }
        } else if point_id.location.round >= top_dag_round.round().prev() {
            // peer is from future round, we lag behind consensus and can see it from other logs
            tracing::Level::TRACE
        } else {
            // we have the round, but not the point - it is very weakly included into global DAG
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent: effects.span(),
            level,
            from = display("cache"),
            task_found = task_found,
            ready = ready.as_ref().map(|dag_point| display(dag_point.alt())),
            peer = display(peer_id.alt()),
            author = display(point_id.location.author.alt()),
            round = point_id.location.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        match ready {
            Some(dag_point) => {
                PointByIdResponse::Defined(dag_point.into_trusted().map(|valid| valid.point))
            }
            None if task_found => PointByIdResponse::TryLater,
            None => PointByIdResponse::Defined(None),
        }
    }

    fn from_store(
        peer_id: &PeerId,
        point_id: &PointId,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> PointByIdResponse {
        let flags = store.get_flags(point_id.location.round, &point_id.digest);
        let point_opt = if flags.is_valid || flags.is_certified {
            store.get_point(point_id.location.round, &point_id.digest)
        } else {
            None
        };
        tracing::debug!(
            parent: effects.span(),
            from = display("store"),
            found = point_opt.is_some(),
            valid = flags.is_valid,
            certified = flags.is_certified,
            peer = display(peer_id.alt()),
            author = display(point_id.location.author.alt()),
            round = point_id.location.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        PointByIdResponse::Defined(point_opt)
    }
}
