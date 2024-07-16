use futures_util::FutureExt;
use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext};
use crate::intercom::dto::PointByIdResponse;
use crate::models::PointId;

pub struct Uploader;

impl Uploader {
    pub fn find(
        peer_id: &PeerId,
        point_id: &PointId,
        top_dag_round: &DagRound,
        effects: &Effects<EngineContext>,
    ) -> PointByIdResponse {
        let dag_round = if point_id.location.round > top_dag_round.round() {
            None
        } else {
            top_dag_round.scan(point_id.location.round)
        };
        let found = dag_round.as_ref().and_then(|r| {
            r.view(&point_id.location.author, |loc| {
                loc.versions().get(&point_id.digest).cloned()
            })
            .flatten()
        });
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
        } else if dag_round.is_some() {
            // we have the round, but not the point - it is very weakly included into global DAG
            tracing::Level::DEBUG
        } else {
            // dag round not found - we detected that peer lags more than `DAG_DEPTH` from us
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent: effects.span(),
            level,
            round_found = dag_round.is_some(),
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
}
