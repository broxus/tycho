use std::sync::Arc;

use anyhow::Result;
use futures_util::never::Never;
use tokio::sync::mpsc;
use tycho_consensus::prelude::{Commit, MempoolAdapterStore, MempoolDb, MempoolOutput, RoundWatch};
use tycho_types::models::ConsensusConfig;

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::parser::Parser;
use crate::mempool::impls::common::shuttle::Shuttle;
use crate::tracing_targets;

pub struct StdAnchorHandler {
    anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
    commit_finished: RoundWatch<Commit>,
    cache: Arc<Cache>,
    deduplicate_rounds: u16,
}

impl StdAnchorHandler {
    pub fn new(
        anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
        commit_finished: RoundWatch<Commit>,
        cache: &Arc<Cache>,
        config: &ConsensusConfig,
    ) -> Self {
        Self {
            anchor_rx,
            commit_finished,
            cache: cache.clone(),
            deduplicate_rounds: config.deduplicate_rounds,
        }
    }

    pub async fn run(mut self, mempool_db: Arc<MempoolDb>) -> Result<Never> {
        scopeguard::defer!(tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "handle anchors task stopped"
        ));
        let mut shuttle = Shuttle {
            store: MempoolAdapterStore::new(mempool_db),
            parser: Parser::new(self.deduplicate_rounds),
            first_after_gap: None,
            set_committed_in_db: true,
        };
        while let Some(output) = self.anchor_rx.recv().await {
            shuttle = self.handle_mempool_output(shuttle, output).await?;
        }
        Err(anyhow::Error::msg("anchor channel from mempool is closed"))
    }

    async fn handle_mempool_output(
        &self,
        mut shuttle: Shuttle,
        output: MempoolOutput,
    ) -> Result<Shuttle> {
        match output {
            MempoolOutput::NextAnchor(adata) => {
                let (output, dirty) = shuttle.handle(adata).await?;
                if let Some(anchor) = output {
                    self.cache.push(Arc::new(anchor));
                }
                return dirty.clean().await;
            }
            MempoolOutput::CommitFinished(round) => {
                // history payloads are read from DB and marked committed, so ready to be removed
                self.commit_finished.set_max(round);
            }
            MempoolOutput::NewStartAfterGap(anchors_full_bottom) => {
                let first_to_execute = (anchors_full_bottom + self.deduplicate_rounds).0;

                shuttle.parser = Parser::new(self.deduplicate_rounds);
                shuttle.first_after_gap = Some(first_to_execute);

                // set as committed because every anchor is repeatable, i.e. enough tail is preserved
                self.commit_finished.set_max_raw(first_to_execute);

                tracing::info!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    new_bottom = anchors_full_bottom.0,
                    first_after_gap = first_to_execute,
                    "externals cache dropped",
                );
            }
            MempoolOutput::Running => self.cache.set_paused(false),
            MempoolOutput::Paused => self.cache.set_paused(true),
        };
        Ok(shuttle)
    }
}
