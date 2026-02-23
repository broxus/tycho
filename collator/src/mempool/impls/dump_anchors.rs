use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_consensus::prelude::{
    MempoolAdapterStore, MempoolConfigBuilder, MempoolDb, MempoolNodeConfig, MempoolOutput,
};
use tycho_network::PeerId;
use tycho_storage::StorageContext;
use tycho_types::boc::Boc;
use tycho_types::models::{ConsensusConfig, GenesisInfo, Message, MsgInfo};

use crate::mempool::impls::common::parser::Parser;
use crate::mempool::impls::common::shuttle::Shuttle;
use crate::mempool::{ExternalMessage, MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct DumpedAnchor {
    pub id: MempoolAnchorId,
    pub prev_id: Option<MempoolAnchorId>,
    pub author: PeerId,
    pub chain_time: u64,
    pub externals: Vec<String>,
}

impl TryFrom<DumpedAnchor> for MempoolAnchor {
    type Error = anyhow::Error;
    fn try_from(value: DumpedAnchor) -> Result<Self> {
        let mut externals: Vec<Arc<ExternalMessage>> = vec![];

        for data in value.externals {
            let cell = Boc::decode_base64(data)?;
            let message: Message<'_> = cell.parse()?;

            if let MsgInfo::ExtIn(info) = message.info {
                externals.push(Arc::new(ExternalMessage { cell, info }));
            } else {
                return Err(anyhow::anyhow!("Can not parse message"));
            }
        }

        Ok(MempoolAnchor {
            id: value.id,
            prev_id: value.prev_id,
            author: value.author,
            chain_time: value.chain_time,
            externals,
        })
    }
}

pub struct DumpAnchors {
    store: MempoolAdapterStore,
}

impl DumpAnchors {
    pub fn new(storage_context: &StorageContext) -> Result<Self> {
        let mempool_db = MempoolDb::open(storage_context.clone())
            .context("failed to create mempool adapter storage")?;
        Ok(Self {
            store: MempoolAdapterStore::new(mempool_db),
        })
    }

    pub async fn load(
        self,
        top_processed_to_anchor: MempoolAnchorId,
        mempool_node_config: &MempoolNodeConfig,
        consensus_config: &ConsensusConfig,
        genesis_info: GenesisInfo,
    ) -> Result<Vec<MempoolAnchor>> {
        anyhow::ensure!(
            top_processed_to_anchor > genesis_info.start_round,
            "Cannot load history of previous genesis: \
             got top_processed_to_anchor={top_processed_to_anchor} and {genesis_info:?}",
        );

        let mut config_builder = MempoolConfigBuilder::new(mempool_node_config);
        config_builder.set_consensus_config(consensus_config)?;
        config_builder.set_genesis(genesis_info);

        let conf = config_builder.build()?.conf;

        let outputs = (self.store)
            .restore_committed(top_processed_to_anchor, &conf)
            .await?;

        let mut shuttle = Shuttle {
            store: self.store,
            parser: Parser::new(conf.consensus.deduplicate_rounds),
            first_after_gap: Some(top_processed_to_anchor),
            set_committed_in_db: false,
        };

        let mut results = Vec::new();

        for output in outputs {
            match output {
                MempoolOutput::NewStartAfterGap(anchors_full_bottom) => {
                    let first_to_execute =
                        (anchors_full_bottom + conf.consensus.deduplicate_rounds).0;
                    shuttle.parser = Parser::new(conf.consensus.deduplicate_rounds);
                    shuttle.first_after_gap = Some(first_to_execute);

                    tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        new_bottom = anchors_full_bottom.0,
                        first_after_gap = first_to_execute,
                        "unrecoverable gap in anchor chain",
                    );
                }
                MempoolOutput::NextAnchor(adata) => {
                    let (output, dirty) = shuttle.handle(adata).await?;
                    if let Some(anchor) = output {
                        results.push(anchor);
                    };
                    shuttle = dirty.clean().await?;
                }
                MempoolOutput::CommitFinished(_)
                | MempoolOutput::Running
                | MempoolOutput::Paused => {}
            }
        }

        Ok(results)
    }
}

#[cfg(all(test, feature = "test"))]
mod test {
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

        let test_conf = default_test_config();

        let _ = dump_anchors
            .load(
                top_processed_to_anchor,
                test_conf.node_config(),
                &test_conf.conf.consensus,
                GenesisInfo {
                    start_round: 2,
                    genesis_millis: 0,
                },
            )
            .await?;

        Ok(())
    }
}
