use anyhow::{Context, Result};
use bumpalo::Bump;
use tycho_consensus::prelude::{
    AnchorStageRole, ConsensusConfigExt, MempoolAdapterStore, MempoolConfigBuilder, MempoolDb,
    MempoolNodeConfig,
};
use tycho_storage::StorageContext;
use tycho_types::models::{ConsensusConfig, GenesisInfo};

use crate::mempool::impls::common::parser::{Parser, ParserOutput};
use crate::mempool::{MempoolAnchor, MempoolAnchorId};

pub struct DumpAnchors {
    store: MempoolAdapterStore,
}

#[allow(dead_code, reason = "not yet used")] // FIXME bind code and remove this
impl DumpAnchors {
    pub fn new(storage_context: &StorageContext) -> Result<Self> {
        let mempool_db = MempoolDb::open(storage_context.clone())
            .context("failed to create mempool adapter storage")?;
        Ok(Self {
            store: MempoolAdapterStore::new(mempool_db),
        })
    }
    pub fn load(
        &self,
        top_processed_to_anchor: MempoolAnchorId,
        mempool_node_config: &MempoolNodeConfig,
        consensus_config: &ConsensusConfig,
        genesis_info: GenesisInfo,
    ) -> Result<Vec<MempoolAnchor>> {
        anyhow::ensure!(
            top_processed_to_anchor >= genesis_info.start_round,
            "Cannot load history earlier than genesis round: \
             got top_processed_to_anchor={top_processed_to_anchor} and {genesis_info:?}",
        );

        let mut config_builder = MempoolConfigBuilder::new(mempool_node_config);
        config_builder.set_consensus_config(consensus_config)?;
        config_builder.set_genesis(genesis_info);

        let conf = config_builder.build()?.conf;

        let bottom_round = top_processed_to_anchor
            .saturating_sub(conf.consensus.replay_anchor_rounds())
            .max(conf.genesis_round.0);

        // no overlay id check: do not rewrite db state, just try to load data

        let anchors = (self.store).load_history_since(bottom_round);

        let mut total_payload_bytes: usize = 0;
        for (_, history) in anchors.values() {
            for info in history {
                total_payload_bytes += info.payload_bytes() as usize;
            }
        }
        let bump = Bump::with_capacity(total_payload_bytes);

        let mut parser = Parser::new(conf.consensus.deduplicate_rounds);

        let mut output = Vec::new();

        let mut prev_visited_anchor = None;

        for (anchor_round, (anchor, history)) in anchors {
            let payloads = (self.store).expand_anchor_history(&anchor, &history, &bump, false);

            let ParserOutput {
                unique_messages, ..
            } = parser.parse_unique(anchor_round, payloads);

            let prev_linked_anchor = anchor.anchor_round(AnchorStageRole::Proof).prev().0;

            if let Some(prev_visited_anchor) = prev_visited_anchor {
                anyhow::ensure!(
                    prev_visited_anchor == prev_linked_anchor,
                    "cannot reproduce anchor history because mempool state is not synced; \
                    there is a gap after anchor {prev_visited_anchor}: \
                    expected at most {prev_linked_anchor} got {anchor_round}",
                );
            }

            if anchor_round >= top_processed_to_anchor {
                output.push(MempoolAnchor {
                    id: anchor_round,
                    prev_id: (prev_linked_anchor > conf.genesis_round.0)
                        .then_some(prev_linked_anchor),
                    chain_time: anchor.time().millis(),
                    author: *anchor.author(),
                    externals: unique_messages,
                });
            }

            prev_visited_anchor = Some(anchor_round);
        }

        Ok(output)
    }
}

#[cfg(all(test, feature = "test"))]
mod test {
    use std::num::NonZeroU16;

    use tycho_consensus::test_utils::default_test_config;
    use tycho_storage::StorageConfig;
    use tycho_util::test::init_logger;

    use super::*;

    #[tokio::test]
    #[ignore] // FIXME it's an example for manual run, should replace with smth more valuable
    async fn dump_mempool_anchors() -> Result<()> {
        init_logger("test_dump_mempool_anchors", "debug");

        let storage_conf = StorageConfig {
            root_dir: "../.temp/db1".into(), // filled by `just node 1`
            ..Default::default()
        };

        let ctx = StorageContext::new(storage_conf).await?;
        let dump_anchors = DumpAnchors::new(&ctx)?;

        let top_processed_to_anchor: MempoolAnchorId = 10;

        let mempool_node_conf = MempoolNodeConfig {
            clean_db_period_rounds: NonZeroU16::new(10).unwrap(),
            ..Default::default()
        };

        let test_conf = default_test_config();

        let dump = dump_anchors.load(
            top_processed_to_anchor,
            &mempool_node_conf,
            &test_conf.conf.consensus,
            GenesisInfo {
                start_round: 2,
                genesis_millis: 0,
            },
        )?;

        for i in dump {
            tracing::info!("{i:?}");
        }

        Ok(())
    }
}
