use tycho_network::PeerId;
use tycho_storage::point_status::PointStatus;
use weedb::rocksdb::DBPinnableSlice;

use crate::dag::DagHead;
use crate::effects::{AltFormat, Ctx, MempoolStore, RoundCtx};
use crate::intercom::dto::PointByIdResponse;
use crate::models::PointId;

pub struct Uploader;

impl Uploader {
    pub fn find<'a>(
        peer_id: &PeerId,
        point_id: &PointId,
        head: &DagHead,
        store: &'a MempoolStore,
        round_ctx: &RoundCtx,
    ) -> PointByIdResponse<DBPinnableSlice<'a>> {
        if point_id.round > head.current().round() {
            return PointByIdResponse::TryLater;
        }
        match Self::from_store(peer_id, point_id, store, round_ctx) {
            Some(status) => {
                if status.is_valid || status.is_trusted || status.is_certified {
                    match store.get_point_raw(point_id.round, point_id.digest) {
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
        }
    }

    fn from_store(
        peer_id: &PeerId,
        point_id: &PointId,
        store: &MempoolStore,
        round_ctx: &RoundCtx,
    ) -> Option<PointStatus> {
        let status = store.get_status(point_id.round, &point_id.digest)?;
        tracing::debug!(
            parent: round_ctx.span(),
            from = display("store"),
            trusted = status.is_trusted,
            certified = status.is_certified,
            peer = display(peer_id.alt()),
            author = display(point_id.author.alt()),
            round = point_id.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        Some(status)
    }
}
