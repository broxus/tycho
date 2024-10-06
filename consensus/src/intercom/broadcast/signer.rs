use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::dyn_event;
use crate::effects::{AltFormat, Effects, EngineContext};
use crate::intercom::dto::{SignatureRejectedReason, SignatureResponse};
use crate::intercom::BroadcastFilter;
use crate::models::Round;

pub struct Signer;
impl Signer {
    pub fn signature_response(
        round: Round,
        author: &PeerId,
        top_dag_round: &DagRound,
        broadcast_filter: &BroadcastFilter,
        effects: &Effects<EngineContext>,
    ) -> SignatureResponse {
        let response =
            Self::make_signature_response(round, author, top_dag_round, broadcast_filter, effects);
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
    /// use [`Self::make_signature_response`]
    fn make_signature_response(
        round: Round,
        author: &PeerId,
        next_dag_round: &DagRound,
        broadcast_filter: &BroadcastFilter,
        effects: &Effects<EngineContext>,
    ) -> SignatureResponse {
        if round >= next_dag_round.round() {
            // first check BroadcastFilter, then DAG for top_dag_round exactly
            if broadcast_filter.has_point(round, author) {
                // hold fast nodes from moving forward
                // notice the engine is initiated with Round(0)), thus avoid panic
                return SignatureResponse::TryLater;
            } else if round > next_dag_round.round() {
                // such points may be in BroadcastFilter only
                return SignatureResponse::NoPoint;
            };
        };

        // notice the first genesis is at with Round(1), thus avoid panic
        let engine_round = next_dag_round.round().prev();
        if round.next() < engine_round {
            // lagged too far from consensus and us, will sign only 1 round behind current;
            return SignatureResponse::Rejected(SignatureRejectedReason::TooOldRound);
        }

        let Some(point_dag_round) = next_dag_round.scan(round) else {
            // may happen on init from genesis, when DAG is not yet populated with points,
            // but a misbehaving node requests a point older than genesis, which cannot be signed
            return SignatureResponse::Rejected(SignatureRejectedReason::NoDagRound);
        };
        // TODO do not state().clone() - mutating closure on location is easily used;
        //  need to remove inner locks from InclusionState and leave it guarded by DashMap;
        //  also sign points during their validation, see comments in DagLocation::add_validate()
        let Some(state) = point_dag_round.view(author, |loc| loc.state().clone()) else {
            // retry broadcast
            return SignatureResponse::NoPoint;
        };
        if round == next_dag_round.round() {
            // point was moved from broadcast filter to dag, do not respond with `NoPoint`
            return SignatureResponse::TryLater;
        }
        if let Some(signable) = state.signable() {
            let is_witness = round.next() == engine_round;
            let current_dag_round_bind = if is_witness {
                next_dag_round.prev().upgrade()
            } else {
                None
            };
            let key_pair = if is_witness {
                // points at previous local dag round are witness for next round point
                current_dag_round_bind
                    .as_ref()
                    .and_then(|for_witness| for_witness.key_pair())
            } else if round == engine_round {
                // points at current local dag round are includes for next round point
                next_dag_round.key_pair()
            } else {
                let _guard = effects.span().enter();
                panic!(
                    "requests for round {} must be filtered out earlier in this method; author {}",
                    round.0,
                    author.alt()
                )
            };
            signable.sign(engine_round, key_pair);
        }
        match state.signed() {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.with.clone()),
            Some(Err(())) => SignatureResponse::Rejected(SignatureRejectedReason::CannotSign),
            None => SignatureResponse::TryLater,
        }
    }
}
