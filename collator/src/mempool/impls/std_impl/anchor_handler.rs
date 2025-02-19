use std::sync::Arc;
use std::time::Duration;

use everscale_types::models::ConsensusConfig;
use tokio::sync::mpsc;
use tycho_consensus::prelude::{AnchorData, MempoolAdapterStore, MempoolOutput};
use tycho_util::time::now_millis;

use crate::mempool::impls::std_impl::cache::Cache;
use crate::mempool::impls::std_impl::parser::{Parser, ParserOutput};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

pub struct AnchorHandler {
    anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
    deduplicate_rounds: u16,
}

struct Shuttle {
    cache: Arc<Cache>,
    store: MempoolAdapterStore,
    parser: Parser,
    first_after_gap: Option<MempoolAnchorId>,
}

impl AnchorHandler {
    pub fn new(
        config: &ConsensusConfig,
        anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
    ) -> Self {
        Self {
            anchor_rx,
            deduplicate_rounds: config.deduplicate_rounds,
        }
    }

    pub async fn run(mut self, cache: Arc<Cache>, store: MempoolAdapterStore) {
        scopeguard::defer!(tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "handle anchors task stopped"
        ));
        let mut shuttle = Shuttle {
            cache,
            store,
            parser: Parser::new(self.deduplicate_rounds),
            first_after_gap: None,
        };
        while let Some(output) = self.anchor_rx.recv().await {
            shuttle = self.handle_mempool_output(shuttle, output).await;
        }
        tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "anchor channel from mempool is closed"
        );
    }

    /// `None` if should break, `Some` if ok and continue
    #[must_use = "modified arg is returned from future task"]
    async fn handle_mempool_output(&self, mut shuttle: Shuttle, output: MempoolOutput) -> Shuttle {
        match output {
            MempoolOutput::NextAnchor(committed) => return shuttle.handle(committed).await,
            MempoolOutput::NewStartAfterGap(anchors_full_bottom) => {
                shuttle.reset(self.deduplicate_rounds, anchors_full_bottom.0);
            }
            MempoolOutput::Running => shuttle.cache.set_paused(false),
            MempoolOutput::Paused => shuttle.cache.set_paused(true),
        };
        shuttle
    }
}

impl Shuttle {
    fn reset(&mut self, deduplicate_rounds: u16, anchors_full_bottom: MempoolAnchorId) {
        self.cache.reset();
        self.parser = Parser::new(deduplicate_rounds);
        let first_to_execute = anchors_full_bottom.saturating_add(deduplicate_rounds as u32);
        self.store.report_new_start(first_to_execute);
        self.first_after_gap = Some(first_to_execute);
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            new_bottom = anchors_full_bottom,
            first_after_gap = first_to_execute,
            "externals cache dropped",
        );
    }

    async fn handle(mut self, committed: AnchorData) -> Self {
        let anchor_id: MempoolAnchorId = committed.anchor.round().0;
        metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

        let chain_time = committed.anchor.data().time.millis();
        let is_executable =
            (self.first_after_gap.as_ref()).is_none_or(|first_id| anchor_id >= *first_id);

        let task = tokio::task::spawn_blocking(move || {
            let payloads =
                (self.store).expand_anchor_history(&committed.anchor, &committed.history);

            let total_messages = payloads.len();
            let total_bytes: usize = payloads.iter().fold(0, |acc, bytes| acc + bytes.len());

            let ParserOutput {
                unique_messages,
                unique_payload_bytes,
            } = self.parser.parse_unique(anchor_id, payloads);

            let unique_messages_len = unique_messages.len();

            if is_executable {
                self.cache.push(Arc::new(MempoolAnchor {
                    id: anchor_id,
                    prev_id: committed.prev_anchor.map(|round| round.0),
                    chain_time,
                    author: committed.anchor.data().author,
                    externals: unique_messages,
                }));
            }

            self.parser.clean(anchor_id);

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

            self
        });

        match task.await {
            Ok(this) => this,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => {
                tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    id = anchor_id,
                    %is_executable,
                    time = chain_time,
                    "handle anchor is cancelled: future will hang until dropped"
                );
                scopeguard::defer!(tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    id = anchor_id,
                    %is_executable,
                    time = chain_time,
                    "handle anchor is cancelled: hung future is dropped"
                ));
                futures_util::future::pending().await
            }
        }
    }
}
