use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tycho_network::PeerId;
use tycho_types::models::ConsensusConfig;
use tycho_util::time::now_millis;

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::parser::{Parser, ParserOutput};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

pub const ANCHOR_ID_STEP: u32 = 4;
pub const ANCHOR_ID_DEFAULT: u32 = 5;

pub struct AnchorSingleNodeHandler {
    deduplicate_rounds: u16,
    peer_id: PeerId,
    anchor_id: MempoolAnchorId,
}

impl AnchorSingleNodeHandler {
    pub fn new(config: &ConsensusConfig, peer_id: PeerId, anchor_id: MempoolAnchorId) -> Self {
        Self {
            deduplicate_rounds: config.deduplicate_rounds,
            peer_id,
            anchor_id,
        }
    }

    pub async fn run(self, cache: Arc<Cache>, external_messages: Vec<Bytes>) {
        scopeguard::defer!(tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "handle anchors task stopped"
        ));

        let mut parser = Parser::new(self.deduplicate_rounds);
        let anchor_id = self.anchor_id;

        metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

        let chain_time = tycho_util::time::now_millis();

        let task = tokio::task::spawn_blocking(move || {
            let total_messages = external_messages.len();
            let total_bytes: usize = external_messages
                .iter()
                .fold(0, |acc, bytes| acc + bytes.len());

            let ParserOutput {
                unique_messages,
                unique_payload_bytes,
            } = parser.parse_unique(
                anchor_id,
                external_messages.iter().map(AsRef::as_ref).collect(),
            );

            let unique_messages_len = unique_messages.len();

            cache.push(Arc::new(MempoolAnchor {
                id: anchor_id,
                prev_id: if anchor_id != ANCHOR_ID_DEFAULT {
                    // @todo not stable
                    Some(anchor_id - ANCHOR_ID_STEP)
                } else {
                    None
                },
                chain_time,
                author: self.peer_id,
                externals: unique_messages,
            }));

            parser.clean(anchor_id);

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
                time = chain_time,
                externals_unique = unique_messages_len,
                externals_skipped = total_messages - unique_messages_len,
                "new anchor"
            );
        });

        match task.await {
            Ok(_) => {}
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => {
                tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    id = anchor_id,
                    time = chain_time,
                    "handle anchor is cancelled: future will hang until dropped"
                );
                scopeguard::defer!(tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    id = anchor_id,
                    time = chain_time,
                    "handle anchor is cancelled: hung future is dropped"
                ));
                futures_util::future::pending().await
            }
        }

        tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "anchor channel from mempool is closed"
        );
    }
}
