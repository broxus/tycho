use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
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
    fn dump_bans(&self) -> Result<Vec<DumpBansItem>> {
        let bans = self.moderator.dump_bans()?;
        let mapped = bans.into_iter().map(|(peer_id, q_ban)| DumpBansItem {
            peer_id: peer_id.0.into(),
            until_millis: q_ban.until.millis(),
            created_millis: q_ban.key.created.millis(),
            record_seq_no: q_ban.key.seq_no(),
        });
        Ok(mapped.collect())
    }

    fn dump_events(&self, req: DumpEventsRequest) -> Result<String> {
        let peer_id = req.peer_id.as_ref().map(|v| PeerId::wrap(v.as_array()));
        let json = self.moderator.dump_events(peer_id)?;
        Ok(if req.pretty {
            serde_json::to_string_pretty(&json)?
        } else {
            serde_json::to_string(&json)?
        })
    }

    fn manual_ban(&self, req: BanRequest) -> Result<String> {
        let json = self
            .moderator
            .manual_ban(PeerId::wrap(req.peer_id.as_array()), req.duration)?;
        Ok(if req.pretty {
            serde_json::to_string_pretty(&json)?
        } else {
            serde_json::to_string(&json)?
        })
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
                    points: event.value.point_keys.len(),
                    kind: format!("{:?}", event.value.kind),
                    message: event.value.message,
                    point_keys: req.with_ids.then(|| {
                        (event.value.point_keys.iter())
                            .map(|k| PointKey(k.round.0, HashBytes(*k.digest.inner())))
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
            // thread running compaction should not and will not be aborted once started
            tokio::task::spawn_blocking(move || {
                moderator.delete_events(millis)?;
                drop(permit);
                Ok(())
            })
            .await?
        })
    }
}
