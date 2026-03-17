use std::time::Duration;

use anyhow::Result;
use bumpalo::Bump;
use tycho_consensus::prelude::{AnchorData, MempoolAdapterStore};
use tycho_util::time::now_millis;

use crate::mempool::impls::common::parser::{Parser, ParserOutput};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

pub struct Shuttle {
    pub store: MempoolAdapterStore,
    pub parser: Parser,
    pub set_committed_in_db: bool,
}

/// removed from hot path at the price of anchor dump may not recover last anchor
/// in case of ungraceful shutdown; node restart is independent of stored anchor flags
pub struct DirtyShuttle {
    shuttle: Box<Shuttle>,
    committed: Box<AnchorData>,
    bump: Bump,
}

impl Shuttle {
    pub async fn handle(
        mut self: Box<Self>,
        committed: Box<AnchorData>,
    ) -> Result<(Option<MempoolAnchor>, DirtyShuttle)> {
        let anchor_id: MempoolAnchorId = committed.anchor.round().0;
        metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

        let chain_time = committed.anchor.time().millis();

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

            let output = committed.is_executable.then(|| MempoolAnchor {
                id: anchor_id,
                prev_id: committed.prev_anchor.map(|round| round.0),
                chain_time,
                author: *committed.anchor.author(),
                externals: unique_messages,
            });

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
                is_executable = committed.is_executable,
                needs_empty_cache = committed.needs_empty_cache,
                time = chain_time,
                externals_unique = unique_messages_len,
                externals_skipped = total_messages - unique_messages_len,
                "new anchor"
            );

            let dirty = DirtyShuttle {
                shuttle: self,
                committed,
                bump,
            };

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
    pub async fn clean(mut self) -> Result<Box<Shuttle>> {
        let task = tokio::task::spawn_blocking(move || {
            if self.shuttle.set_committed_in_db {
                self.shuttle.store.set_committed(&self.committed)?;
            }

            let anchor_id = self.committed.anchor.round();

            tycho_util::mem::Reclaimer::instance().drop((self.committed, self.bump));

            self.shuttle.parser.clean(anchor_id.0);

            anyhow::Ok(self.shuttle)
        });
        match task.await {
            Ok(result) => result,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => Err(e.into()),
        }
    }
}
