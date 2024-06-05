use futures_util::FutureExt;
use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::dyn_event;
use crate::effects::{AltFormat, CurrentRoundContext, Effects};
use crate::intercom::dto::PointByIdResponse;
use crate::models::PointId;

pub struct Uploader;

impl Uploader {
    pub fn find(
        peer_id: &PeerId,
        point_id: &PointId,
        top_dag_round: &DagRound,
        effects: &Effects<CurrentRoundContext>,
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
        let ready = found
            .and_then(|shared| shared.now_or_never())
            .map(|(dag_point, _)| dag_point);
        let level = if let Some(dag_point) = ready.as_ref() {
            if dag_point.trusted().is_some() {
                tracing::Level::TRACE
            } else {
                tracing::Level::ERROR
            }
        } else if dag_round.is_some() {
            tracing::Level::DEBUG
        } else if point_id.location.round > top_dag_round.round() {
            tracing::Level::TRACE
        } else {
            tracing::Level::WARN
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
        PointByIdResponse(
            ready
                .and_then(|dag_point| dag_point.into_trusted())
                .map(|valid| valid.point),
        )
    }
}
