use std::cmp;

use tycho_network::PeerId;

use crate::dag::DagHead;
use crate::dyn_event;
use crate::effects::{AltFormat, Ctx, RoundCtx};
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
        round_ctx: &RoundCtx,
    ) -> SignatureResponse {
        let response = Self::make_signature_response(round, author, head, broadcast_filter);
        let level = match response {
            SignatureResponse::Rejected(_) => tracing::Level::WARN,
            _ => tracing::Level::TRACE,
        };
        dyn_event!(
            parent: round_ctx.span(),
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
        let state = match point_dag_round.view(author, |loc| {
            if loc.versions.is_empty() {
                Err(())
            } else {
                Ok(loc.state.clone())
            }
        }) {
            // locations are prepopulated for all expected peers
            None => return SignatureResponse::Rejected(SignatureRejectedReason::UnknownPeer),
            // point may be moving from broadcast filter to dag, do not respond with `NoPoint`;
            // anyway cannot sign future round, even if point future is resolved
            Some(_) if round > current_round => return SignatureResponse::TryLater,
            Some(Err(())) => return SignatureResponse::NoPoint,
            Some(Ok(state)) => state,
        };
        let keys = if round == current_round {
            &head.keys().to_include
        } else {
            // only previous to current round
            &head.keys().to_witness
        };
        match state.sign(current_round, keys.as_deref()) {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.signature.clone()),
            Some(Err(())) => SignatureResponse::Rejected(SignatureRejectedReason::CannotSign),
            None => SignatureResponse::TryLater,
        }
    }
}
