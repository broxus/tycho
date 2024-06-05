use tycho_network::PeerId;

use crate::dag::DagRound;
use crate::effects::{AltFormat, CurrentRoundContext, Effects};
use crate::intercom::dto::SignatureResponse;
use crate::models::Round;
use crate::{dyn_event, MempoolConfig};

pub struct Signer;
impl Signer {
    pub fn signature_response(
        round: Round,
        author: &PeerId,
        next_dag_round: &DagRound,
        effects: &Effects<CurrentRoundContext>,
    ) -> SignatureResponse {
        let response = Self::make_signature_response(round, author, next_dag_round);
        let level = if response == SignatureResponse::Rejected {
            tracing::Level::WARN
        } else {
            tracing::Level::TRACE
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
    ) -> SignatureResponse {
        if round >= next_dag_round.round() {
            // hold fast nodes from moving forward
            // notice the engine is initiated with Round(0)), thus avoid panic
            return SignatureResponse::TryLater;
        };
        // notice the first genesis is at with Round(1), thus avoid panic
        let engine_round = next_dag_round.round().prev();
        if round.next() < engine_round {
            // lagged too far from consensus and us, will sign only 1 round behind current;
            return SignatureResponse::Rejected;
        }

        let Some(point_dag_round) = next_dag_round.scan(round) else {
            // may happen on init from genesis, when DAG is not yet populated with points,
            // but a misbehaving node requests a point older than genesis, which cannot be signed
            return SignatureResponse::Rejected;
        };
        // TODO do not state().clone() - mutating closure on location is easily used;
        //  need to remove inner locks from InclusionState and leave it guarded by DashMap;
        //  also sign points during their validation, see comments in DagLocation::add_validate()
        let Some(state) = point_dag_round.view(author, |loc| loc.state().clone()) else {
            // retry broadcast
            return SignatureResponse::NoPoint;
        };
        if let Some(signable) = state.signable() {
            let is_witness = round.next() == engine_round;
            let current_dag_round_bind = if is_witness {
                next_dag_round.prev().get()
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
                unreachable!(
                    "requests for other rounds are filtered out at the beginning of this method"
                )
            };
            signable.sign(engine_round, key_pair, MempoolConfig::sign_time_range());
        }
        match state.signed() {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.with.clone()),
            Some(Err(())) => SignatureResponse::Rejected,
            None => SignatureResponse::TryLater,
        }
    }
}
