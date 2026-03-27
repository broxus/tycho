use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tycho_consensus::prelude::Moderator;
use tycho_control::mempool::*;
use tycho_types::cell::HashBytes;

pub struct MempoolServer {
    moderator: Moderator,
}

impl MempoolServer {
    pub fn new(moderator: Moderator) -> Self {
        Self { moderator }
    }
}

impl MempoolService for MempoolServer {
    fn list_events(
        &self,
        req: ListEventsRequest,
    ) -> BoxFuture<'static, Result<Vec<MempoolEventDisplay>>> {
        let moderator = self.moderator.clone();
        let task = tokio::task::spawn_blocking(move || {
            let events = moderator.list_events(req.count, req.page, req.asc)?;
            let result = events
                .into_iter()
                .map(|event| MempoolEventDisplay {
                    created: event.key.created.millis(),
                    seq_no: event.key.seq_no(),
                    peer_id: HashBytes::from(event.value.peer_id.0),
                    points: event.value.point_refs.len(),
                    kind: format!("{:?}", event.value.kind),
                    message: event.value.message,
                    point_refs: req.with_ids.then(|| {
                        (event.value.point_refs.iter())
                            .map(|p_ref| StoredPointKeyRef {
                                stored: p_ref.is_stored,
                                key: PointKey(
                                    p_ref.key.round.0,
                                    HashBytes(*p_ref.key.digest.inner()),
                                ),
                            })
                            .collect()
                    }),
                })
                .collect();
            Ok(result)
        });
        Box::pin(async move { task.await.context("tokio spawn blocking")? })
    }
}
