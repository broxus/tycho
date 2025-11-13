use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio::sync::Semaphore;
use tycho_consensus::prelude::Moderator;
use tycho_control::mempool::*;
use tycho_network::PeerId;
use tycho_types::cell::HashBytes;

pub struct MempoolServer {
    moderator: Moderator,
    manual_compaction: Arc<Semaphore>,
}

impl MempoolServer {
    pub fn new(moderator: Moderator) -> Self {
        Self {
            moderator,
            manual_compaction: Arc::new(Semaphore::new(1)),
        }
    }
}

impl MempoolService for MempoolServer {
    fn manual_ban(&self, req: BanRequest) -> BoxFuture<'static, Result<String>> {
        (self.moderator)
            .manual_ban(PeerId::wrap(req.peer_id.as_array()), req.duration)
            .map(move |json| {
                Ok(if req.pretty {
                    serde_json::to_string_pretty(&json?)?
                } else {
                    serde_json::to_string(&json?)?
                })
            })
            .boxed()
    }

    fn manual_unban(&self, peer_id: HashBytes) -> BoxFuture<'static, Result<()>> {
        self.moderator
            .manual_unban(PeerId::wrap(peer_id.as_array()))
    }

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

    fn delete_events(&self, millis: Range<u64>) -> BoxFuture<'static, Result<()>> {
        let moderator = self.moderator.clone();
        let manual_compaction = self.manual_compaction.clone();
        Box::pin(async move {
            let permit = manual_compaction
                .acquire_owned()
                .await
                .context("acquire manual compaction permit")?;
            moderator.delete_events(millis).await?;
            drop(permit);
            Ok(())
        })
    }
}
