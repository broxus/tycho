use std::sync::Arc;

use anyhow::Result;
use futures_util::never::Never;
use tokio::sync::mpsc;
use tycho_consensus::prelude::{MempoolAdapterStore, MempoolDb, MempoolOutput};
use tycho_types::models::ConsensusConfig;

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::parser::Parser;
use crate::mempool::impls::common::shuttle::Shuttle;
use crate::tracing_targets;

pub struct StdAnchorHandler {
    anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
    cache: Arc<Cache>,
    deduplicate_rounds: u16,
}

impl StdAnchorHandler {
    pub fn new(
        anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
        cache: &Arc<Cache>,
        config: &ConsensusConfig,
    ) -> Self {
        Self {
            anchor_rx,
            cache: cache.clone(),
            deduplicate_rounds: config.deduplicate_rounds,
        }
    }

    pub async fn run(mut self, mempool_db: Arc<MempoolDb>) -> Result<Never> {
        scopeguard::defer!(tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "handle anchors task stopped"
        ));
        let mut shuttle = Box::new(Shuttle {
            store: MempoolAdapterStore::new(mempool_db),
            parser: Parser::new(self.deduplicate_rounds),
            set_committed_in_db: true,
        });
        while let Some(output) = self.anchor_rx.recv().await {
            shuttle = self.handle_mempool_output(shuttle, output).await?;
        }
        Err(anyhow::Error::msg("anchor channel from mempool is closed"))
    }

    async fn handle_mempool_output(
        &self,
        mut shuttle: Box<Shuttle>,
        output: MempoolOutput,
    ) -> Result<Box<Shuttle>> {
        match output {
            MempoolOutput::Paused(is_paused) => self.cache.set_paused(is_paused),
            MempoolOutput::NextAnchor(adata) => {
                if adata.needs_empty_cache {
                    shuttle.parser = Parser::new(self.deduplicate_rounds);
                    tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        is_executable = adata.is_executable,
                        "deduplication state dropped",
                    );
                };
                let (output, dirty) = shuttle.handle(adata).await?;
                if let Some(anchor) = output {
                    self.cache.push(Arc::new(anchor))?;
                }
                return dirty.clean().await;
            }
        };
        Ok(shuttle)
    }
}
