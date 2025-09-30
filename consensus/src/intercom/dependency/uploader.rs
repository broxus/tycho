use std::sync::LazyLock;

use bytes::Bytes;
use tycho_network::PeerId;

use crate::dag::DagHead;
use crate::effects::{AltFormat, Cancelled, Ctx, RoundCtx, SpawnLimit};
use crate::engine::NodeConfig;
use crate::intercom::core::query::response::DownloadResponse;
use crate::models::{PointId, PointStatusStored};
use crate::storage::MempoolStore;

static LIMIT: LazyLock<SpawnLimit> =
    LazyLock::new(|| SpawnLimit::new(NodeConfig::get().max_upload_tasks.get() as usize));

pub struct Uploader;

impl Uploader {
    pub async fn find(
        peer_id: &PeerId,
        point_id: PointId,
        store: &MempoolStore,
        head: &DagHead,
        round_ctx: &RoundCtx,
    ) -> DownloadResponse<Bytes> {
        let (status_opt, result) = if point_id.round > head.current().round() {
            (None, DownloadResponse::TryLater)
        } else {
            let task_opt = LIMIT.try_spawn_blocking(round_ctx.task(), {
                let store = store.clone();
                let last_back_bottom = head.last_back_bottom();
                move || {
                    let status_opt = store.get_status(point_id.round, &point_id.digest);
                    let result = match &status_opt {
                        Some(PointStatusStored::Validated(usable))
                            if usable.is_valid | usable.is_certified =>
                        {
                            match store.get_point_raw(point_id.round, &point_id.digest) {
                                None => DownloadResponse::DefinedNone,
                                Some(slice) => DownloadResponse::Defined(slice),
                            }
                        }
                        Some(PointStatusStored::IllFormed(ill)) if ill.is_certified => {
                            DownloadResponse::TryLater
                        }
                        Some(PointStatusStored::NotFound(not_found)) if not_found.is_certified => {
                            DownloadResponse::TryLater
                        }
                        Some(PointStatusStored::Exists) | None => {
                            if last_back_bottom <= point_id.round {
                                // may be downloading, unknown or resolving - dag may be incomplete
                                DownloadResponse::TryLater
                            } else {
                                // must have been stored and committed, may be too old and deleted
                                DownloadResponse::DefinedNone
                            }
                        }
                        Some(
                            PointStatusStored::IllFormed(_)
                            | PointStatusStored::NotFound(_)
                            | PointStatusStored::Validated(_),
                        ) => DownloadResponse::DefinedNone,
                    };
                    (status_opt, result)
                }
            });
            match task_opt {
                Some(task) => task
                    .await
                    .unwrap_or_else(|Cancelled()| (None, DownloadResponse::TryLater)),
                None => (None, DownloadResponse::TryLater),
            }
        };
        tracing::debug!(
            parent: round_ctx.span(),
            result = display(result.alt()),
            not_found = status_opt.is_none().then_some(true),
            found = status_opt.map(display),
            peer = display(peer_id.alt()),
            author = display(point_id.author.alt()),
            round = point_id.round.0,
            digest = display(point_id.digest.alt()),
            "upload",
        );
        result
    }
}
