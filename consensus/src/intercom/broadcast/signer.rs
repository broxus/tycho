use std::cmp;

use tycho_network::PeerId;

use crate::dag::DagHead;
use crate::dyn_event;
use crate::effects::{AltFormat, Ctx, RoundCtx};
use crate::intercom::core::{SignatureRejectedReason, SignatureResponse};
use crate::models::Round;

pub struct Signer;
impl Signer {
    pub fn signature_response(
        author: &PeerId,
        round: Round,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) -> SignatureResponse {
        let response = Self::make_signature_response(round, author, head, round_ctx);
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
        round_ctx: &RoundCtx,
    ) -> SignatureResponse {
        if round >= head.next().round() {
            // No matter if we received the point or not, we're not ready to sign.
            // Check in BF is racy during flush especially when head jumps over several rounds.
            // If broadcast failed, sender has to repeat it when we reach the round.
            return SignatureResponse::TryLater;
        } else if round < head.prev().round() {
            // lagged too far from consensus and us, will sign only 1 round behind current;
            return SignatureResponse::Rejected(SignatureRejectedReason::TooOldRound);
        }; // else: may be ready to sign, check inside DAG

        let current_round = head.current().round();

        let point_dag_round = match round.cmp(&current_round) {
            cmp::Ordering::Greater => unreachable!("next round should return earlier"),
            cmp::Ordering::Equal => head.current(),
            cmp::Ordering::Less => head.prev(),
        };
        let state = match point_dag_round.view(author, |loc| {
            if loc.versions.is_empty() {
                Err(SignatureResponse::NoPoint)
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
        match state.sign(current_round, keys.as_deref(), round_ctx.conf()) {
            Some(Ok(signed)) => SignatureResponse::Signature(signed.signature.clone()),
            Some(Err(())) => SignatureResponse::Rejected(SignatureRejectedReason::CannotSign),
            None => SignatureResponse::TryLater,
        }
    }
}
