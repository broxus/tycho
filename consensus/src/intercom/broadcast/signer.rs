use std::cmp;

use tycho_network::PeerId;

use crate::dag::DagHead;
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext};
use crate::intercom::dto::{SignatureRejectedReason, SignatureResponse};
use crate::intercom::BroadcastFilter;
use crate::models::Round;

pub struct Signer;
impl Signer {
    pub fn signature_response(
        author: &PeerId,
        round: Round,
        head: &DagHead,
        broadcast_filter: &BroadcastFilter,
        effects: &Effects<EngineContext>,
    ) -> SignatureResponse {
        let response = Self::make_signature_response(round, author, head, broadcast_filter);
        let level = match response {
            SignatureResponse::Rejected(_) => tracing::Level::WARN,
            _ => tracing::Level::TRACE,
        };
        dyn_event!(
            parent: effects.span(),
            level,
            author = display(author.alt()),
            round = round.0,
            response = display(response.alt()),
            "signature request from"
        );
        response
    }

    fn make_signature_response(
        round: Round,
        author: &PeerId,
        head: &DagHead,
        broadcast_filter: &BroadcastFilter,
    ) -> SignatureResponse {
        if round >= head.next().round() {
            // first check BroadcastFilter, then DAG for top_dag_round exactly
            if broadcast_filter.has_point(round, author) {
                // hold fast nodes from moving forward
                // notice the engine is initiated with Round(0)), thus avoid panic
                return SignatureResponse::TryLater;
            } else if round > head.next().round() {
                // such points may be in BroadcastFilter only
                return SignatureResponse::NoPoint;
            };
        } else if round < head.prev().round() {
            // lagged too far from consensus and us, will sign only 1 round behind current;
            return SignatureResponse::Rejected(SignatureRejectedReason::TooOldRound);
        };

        let current_round = head.current().round();

        let point_dag_round = match round.cmp(&current_round) {
            cmp::Ordering::Greater => head.next(),
            cmp::Ordering::Equal => head.current(),
            cmp::Ordering::Less => head.prev(),
        };

        // TODO do not state().clone() - mutating closure on location is easily used;
        //  need to remove inner locks from InclusionState and leave it guarded by DashMap;
        //  also sign points during their validation, see comments in DagLocation::add_validate()
        let Some(state) = point_dag_round.view(author, |loc| loc.state().clone()) else {
            // retry broadcast
            return SignatureResponse::NoPoint;
        };
        if round > current_round {
            // point was moved from broadcast filter to dag, do not respond with `NoPoint`
            return SignatureResponse::TryLater;
        }
        if let Some(signable) = state.signable() {
            if round == current_round {
                signable.sign(current_round, head.includes_keys());
            } else {
                // only previous to current round
                signable.sign(current_round, head.witness_keys());
            };
        }
        match state.signed() {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.with.clone()),
            Some(Err(())) => SignatureResponse::Rejected(SignatureRejectedReason::CannotSign),
            None => SignatureResponse::TryLater,
        }
    }
}
