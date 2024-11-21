use futures_util::FutureExt;
use tycho_network::PeerId;
use weedb::rocksdb::DBPinnableSlice;

use crate::dag::{DagHead, DagRound};
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
    pub fn find<'a>(
        peer_id: &PeerId,
        point_id: &PointId,
        head: &DagHead,
        store: &'a MempoolStore,
        effects: &Effects<EngineContext>,
    ) -> PointByIdResponse<DBPinnableSlice<'a>> {
        // TODO add logs
        let status = if point_id.round > head.next().round() {
            Some(SearchStatus::TryLater)
        } else if point_id.round >= head.last_back_bottom() {
            // may be in mem, but not guaranteed
            head.next()
                .scan(point_id.round)
                .and_then(|dag_round| Self::from_dag(peer_id, point_id, &dag_round, effects))
                .or(Self::from_store(peer_id, point_id, store, effects))
        } else {
            Self::from_store(peer_id, point_id, store, effects)
        };
        match status {
            Some(SearchStatus::None) | None => PointByIdResponse::DefinedNone,
            // Fixme return serialized as bytes from DB!
            // TODO add error logs if not found in DB while must have been
            Some(SearchStatus::Found) => {
                match store.get_point_raw(point_id.round, point_id.digest) {
                    None => PointByIdResponse::DefinedNone,
                    Some(slice) => PointByIdResponse::Defined(slice),
                }
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
                loc.versions.get(&point_id.digest).cloned()
            })
            .flatten();
        let task_found = found.is_some();
        let ready = found.and_then(|shared| shared.now_or_never());

        let level = if let Some(dag_point) = ready.as_ref() {
            if dag_point.valid().is_some() {
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
                if dag_point.valid().is_some() {
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
        let status = store.get_status(point_id.round, &point_id.digest)?;
        let result = if status.is_valid || status.is_trusted || status.is_certified {
            Some(SearchStatus::Found)
        } else {
            None
        };
        tracing::debug!(
            parent: effects.span(),
            from = display("store"),
            trusted = status.is_trusted,
            certified = status.is_certified,
            peer = display(peer_id.alt()),
            author = display(point_id.author.alt()),
            round = point_id.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        result
    }
}
