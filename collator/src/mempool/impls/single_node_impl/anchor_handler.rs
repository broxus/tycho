use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tycho_network::PeerId;
use tycho_types::models::ConsensusConfig;
use tycho_util::time::{MonotonicClock, now_millis};
use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::parser::{Parser, ParserOutput};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;
pub const ANCHOR_ID_STEP: u32 = 4;
pub struct SingleNodeAnchorHandler {
    cache: Arc<Cache>,
    parser: Parser,
    peer_id: PeerId,
    prev_anchor_id: Option<MempoolAnchorId>,
}
impl SingleNodeAnchorHandler {
    pub fn new(
        cache: Arc<Cache>,
        peer_id: PeerId,
        top_processed_to_anchor_id: MempoolAnchorId,
        config: &ConsensusConfig,
    ) -> Self {
        let prev_anchor_id = top_processed_to_anchor_id.saturating_sub(ANCHOR_ID_STEP);
        Self {
            cache,
            parser: Parser::new(config.deduplicate_rounds),
            peer_id,
            prev_anchor_id: (prev_anchor_id > 1).then_some(prev_anchor_id),
        }
    }
    pub async fn handle(mut self, payloads: Vec<Bytes>) -> Self {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle)),
            file!(),
            39u32,
        );
        let payloads = payloads;
        let prev_anchor_id = self.prev_anchor_id.take();
        let anchor_id = prev_anchor_id.unwrap_or(1) + ANCHOR_ID_STEP;
        metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);
        let chain_time = MonotonicClock::now_millis();
        let task = tokio::task::spawn_blocking(move || {
            let total_messages = payloads.len();
            let total_bytes: usize = payloads
                .iter()
                .fold(0, |acc, bytes| acc + bytes.len());
            let ParserOutput { unique_messages, unique_payload_bytes } = (self.parser)
                .parse_unique(anchor_id, payloads);
            let unique_messages_len = unique_messages.len();
            self.cache
                .push(
                    Arc::new(MempoolAnchor {
                        id: anchor_id,
                        prev_id: prev_anchor_id,
                        chain_time,
                        author: self.peer_id,
                        externals: unique_messages,
                    }),
                );
            self.prev_anchor_id = Some(anchor_id);
            self.parser.clean(anchor_id);
            metrics::counter!("tycho_mempool_msgs_unique_count")
                .increment(unique_messages_len as _);
            metrics::counter!("tycho_mempool_msgs_unique_bytes")
                .increment(unique_payload_bytes as _);
            metrics::counter!("tycho_mempool_msgs_duplicates_count")
                .increment((total_messages - unique_messages_len) as _);
            metrics::counter!("tycho_mempool_msgs_duplicates_bytes")
                .increment((total_bytes - unique_payload_bytes) as _);
            metrics::histogram!("tycho_mempool_commit_anchor_latency_time")
                .record(
                    Duration::from_millis(now_millis().max(chain_time) - chain_time),
                );
            tracing::info!(
                target : tracing_targets::MEMPOOL_ADAPTER, id = anchor_id, time =
                chain_time, externals_unique = unique_messages_len, externals_skipped =
                total_messages - unique_messages_len, "new anchor"
            );
            self
        });
        match {
            __guard.end_section(94u32);
            let __result = task.await;
            __guard.start_section(94u32);
            __result
        } {
            Ok(this) => this,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => {
                tracing::warn!(
                    target : tracing_targets::MEMPOOL_ADAPTER, id = anchor_id, time =
                    chain_time,
                    "handle anchor is cancelled: future will hang until dropped"
                );
                scopeguard::defer!(
                    tracing::warn!(target : tracing_targets::MEMPOOL_ADAPTER, id =
                    anchor_id, time = chain_time,
                    "handle anchor is cancelled: hung future is dropped")
                );
                {
                    __guard.end_section(110u32);
                    let __result = futures_util::future::pending().await;
                    __guard.start_section(110u32);
                    __result
                }
            }
        }
    }
}
