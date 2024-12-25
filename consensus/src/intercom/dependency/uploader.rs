use bytes::Bytes;
use tycho_network::PeerId;
use tycho_storage::point_status::PointStatus;

use crate::dag::DagHead;
use crate::effects::{AltFormat, Ctx, MempoolStore, RoundCtx};
use crate::intercom::dto::PointByIdResponse;
use crate::models::PointId;

pub struct Uploader;

impl Uploader {
    pub fn find(
        peer_id: &PeerId,
        point_id: &PointId,
        head: &DagHead,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> PointByIdResponse<Bytes> {
        let (status_opt, result) = if point_id.round > head.current().round() {
            (None, PointByIdResponse::TryLater)
        } else {
            let status_opt = store.get_status(point_id.round, &point_id.digest);
            let result = match (status_opt.as_ref())
                // point has default status during validation - should retry when finished
                .filter(|status| **status != PointStatus::default())
            {
                Some(status) => {
                    if status.is_valid || status.is_trusted || status.is_certified {
                        match store.get_point_raw(point_id.round, &point_id.digest) {
                            None => PointByIdResponse::DefinedNone,
                            Some(slice) => PointByIdResponse::Defined(slice),
                        }
                    } else {
                        PointByIdResponse::DefinedNone
                    }
                }
                None => {
                    if head.last_back_bottom() <= point_id.round {
                        // may be downloading or resolving - dag may be incomplete
                        PointByIdResponse::TryLater
                    } else {
                        // must have been stored and committed, but not found
                        PointByIdResponse::DefinedNone
                    }
                }
            };
            (status_opt, result)
        };
        tracing::debug!(
            parent: round_ctx.span(),
            result = display(result.alt()),
            not_found = Some(status_opt.is_none()).filter(|x| *x),
            found = status_opt.map(debug),
            peer = display(peer_id.alt()),
            author = display(point_id.author.alt()),
            round = point_id.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        result
    }
}
