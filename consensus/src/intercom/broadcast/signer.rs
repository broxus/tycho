use std::cmp;

use tycho_network::PeerId;

use crate::dag::DagHead;
use crate::dyn_event;
use crate::effects::{AltFormat, Ctx, RoundCtx};
use crate::engine::MempoolConfig;
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
        let response =
            Self::make_signature_response(round, author, head, broadcast_filter, round_ctx.conf());
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
        conf: &MempoolConfig,
    ) -> SignatureResponse {
        if round >= head.next().round() {
            // first check BroadcastFilter, then DAG for top_dag_round exactly
            if broadcast_filter.has_point(round, author) {
                // hold fast nodes from moving forward
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
                Err(SignatureResponse::NoPoint)
            } else if round > current_round {
                // we found the point, but cannot sign future rounds
                Err(SignatureResponse::TryLater)
            } else {
                Ok(loc.state.clone())
            }
        }) {
            // locations are prepopulated for all expected peers
            None => return SignatureResponse::Rejected(SignatureRejectedReason::UnknownPeer),
            Some(Err(err)) => return err,
            Some(Ok(state)) => state,
        };
        let keys = if round == current_round {
            &head.keys().to_include
        } else {
            // only previous to current round
            &head.keys().to_witness
        };
        match state.sign(current_round, keys.as_deref(), conf) {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.signature.clone()),
            Some(Err(())) => SignatureResponse::Rejected(SignatureRejectedReason::CannotSign),
            None => SignatureResponse::TryLater,
        }
    }
}
