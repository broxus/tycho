use futures_util::FutureExt;
use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext, MempoolStore};
use crate::intercom::dto::PointByIdResponse;
use crate::models::PointId;

pub struct Uploader;

enum SearchStatus {
    TryLater,
    None,
    Found,
}

impl Uploader {
    pub fn find(
        peer_id: &PeerId,
        point_id: &PointId,
        top_dag_round: &DagRound,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> PointByIdResponse {
        if point_id.round > top_dag_round.round() {
            // TODO add logs
            return PointByIdResponse::TryLater;
        };
        let status = top_dag_round
            .scan(point_id.round)
            .and_then(|dag_round| Self::from_dag(peer_id, point_id, &dag_round, effects))
            .or(Self::from_store(peer_id, point_id, store, effects));
        match status {
            Some(SearchStatus::None) | None => PointByIdResponse::Defined(None),
            // Fixme return serialized as bytes from DB!
            // TODO add error logs if not found in DB while must have been
            Some(SearchStatus::Found) => {
                PointByIdResponse::Defined(store.get_point(point_id.round, &point_id.digest))
            }
            Some(SearchStatus::TryLater) => PointByIdResponse::TryLater,
        }
    }

    fn from_dag(
        peer_id: &PeerId,
        point_id: &PointId,
        dag_round: &DagRound,
        effects: &Effects<EngineContext>,
    ) -> Option<SearchStatus> {
        let found = dag_round
            .view(&point_id.author, |loc| {
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
        } else {
            // we have the round, but not the point - it is very weakly included into global DAG
            tracing::Level::DEBUG
        };
        dyn_event!(
            parent: effects.span(),
            level,
            from = display("dag"),
            task_found = task_found,
            ready = ready.as_ref().map(|dag_point| display(dag_point.alt())),
            peer = display(peer_id.alt()),
            author = display(point_id.author.alt()),
            round = point_id.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        match ready {
            Some(dag_point) => {
                if dag_point.trusted().is_some() {
                    Some(SearchStatus::Found)
                } else {
                    Some(SearchStatus::None)
                }
            }
            None if task_found => Some(SearchStatus::TryLater),
            None => None,
        }
    }

    fn from_store(
        peer_id: &PeerId,
        point_id: &PointId,
        store: &MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> Option<SearchStatus> {
        let flags = store.get_flags(point_id.round, &point_id.digest);
        let result = if flags.is_trusted || flags.is_certified {
            Some(SearchStatus::Found)
        } else {
            None
        };
        tracing::debug!(
            parent: effects.span(),
            from = display("store"),
            trusted = flags.is_trusted,
            certified = flags.is_certified,
            peer = display(peer_id.alt()),
            author = display(point_id.author.alt()),
            round = point_id.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        result
    }
}
