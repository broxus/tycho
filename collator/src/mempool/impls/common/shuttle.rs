use std::time::Duration;

use anyhow::Result;
use bumpalo::Bump;
use tokio::task::JoinHandle;
use tycho_consensus::prelude::{AnchorData, MempoolAdapterStore};
use tycho_util::time::now_millis;

use crate::mempool::impls::common::parser::{Parser, ParserOutput};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

pub struct Shuttle {
    pub store: MempoolAdapterStore,
    pub parser: Parser,
    /// value set from outside, but cleaned inside
    pub first_after_gap: Option<MempoolAnchorId>,
    pub set_committed_in_db: bool,
}

/// removed from hot path at the price of anchor dump may not recover last anchor
/// in case of ungraceful shutdown; node restart is independent of stored anchor flags
pub struct DirtyShuttle(JoinHandle<Result<Shuttle>>);

impl Shuttle {
    pub async fn handle(
        mut self,
        committed: AnchorData,
    ) -> Result<(Option<MempoolAnchor>, DirtyShuttle)> {
        let anchor_id: MempoolAnchorId = committed.anchor.round().0;
        metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

        let chain_time = committed.anchor.time().millis();
        let is_executable =
            (self.first_after_gap.as_ref()).is_none_or(|first_id| anchor_id >= *first_id);

        let task = tokio::task::spawn_blocking(move || {
            let bump = Bump::with_capacity(
                (self.store).expand_anchor_history_arena_size(&committed.history),
            );

            let payloads = (self.store).expand_anchor_history(&committed, &bump)?;

            let total_messages = payloads.len();
            let total_bytes: usize = payloads.iter().fold(0, |acc, bytes| acc + bytes.len());

            let ParserOutput {
                unique_messages,
                unique_payload_bytes,
            } = self.parser.parse_unique(anchor_id, payloads);

            let unique_messages_len = unique_messages.len();

            let output = if is_executable {
                // don't link to prev anchor in case there was a gap
                let prev_id = if self.first_after_gap.is_some() {
                    self.first_after_gap = None;
                    None
                } else {
                    committed.prev_anchor.map(|round| round.0)
                };
                Some(MempoolAnchor {
                    id: anchor_id,
                    prev_id,
                    chain_time,
                    author: *committed.anchor.author(),
                    externals: unique_messages,
                })
            } else {
                None
            };

            let dirty = DirtyShuttle::new(self, committed, bump);

            metrics::counter!("tycho_mempool_msgs_unique_count")
                .increment(unique_messages_len as _);
            metrics::counter!("tycho_mempool_msgs_unique_bytes")
                .increment(unique_payload_bytes as _);

            metrics::counter!("tycho_mempool_msgs_duplicates_count")
                .increment((total_messages - unique_messages_len) as _);
            metrics::counter!("tycho_mempool_msgs_duplicates_bytes")
                .increment((total_bytes - unique_payload_bytes) as _);

            metrics::histogram!("tycho_mempool_commit_anchor_latency_time").record(
                Duration::from_millis(now_millis().max(chain_time) - chain_time),
            );

            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = anchor_id,
                %is_executable,
                time = chain_time,
                externals_unique = unique_messages_len,
                externals_skipped = total_messages - unique_messages_len,
                "new anchor"
            );

            Ok((output, dirty))
        });

        match task.await {
            Ok(result) => result,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => Err(e.into()),
        }
    }
}

impl DirtyShuttle {
    fn new(mut shuttle: Shuttle, committed: AnchorData, bump: Bump) -> Self {
        Self(tokio::task::spawn_blocking(move || {
            if shuttle.set_committed_in_db {
                shuttle.store.set_committed(&committed)?;
            }

            let anchor_id = committed.anchor.round();

            tycho_util::mem::Reclaimer::instance().drop((committed, bump));

            shuttle.parser.clean(anchor_id.0);

            anyhow::Ok(shuttle)
        }))
    }

    pub async fn clean(self) -> Result<Shuttle> {
        match self.0.await {
            Ok(result) => result,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => Err(e.into()),
        }
    }
}
