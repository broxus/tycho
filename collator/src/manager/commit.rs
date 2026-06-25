use anyhow::Result;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_types::models::BlockId;
use tycho_util::FastHashSet;
use tycho_util::metrics::HistogramGuard;

use super::CollationManager;
use super::types::{
    BlockCacheEntryData, CandidateStatus, CollatorJoinTask, McBlockSubgraph, McBlockSubgraphExtract,
};
use crate::collator::CollatorFactory;
use crate::tracing_targets;
use crate::validator::Validator;

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    /// Try to commit validated and valid master block
    /// if it was not already committed before
    /// 1. Check if master block is valid
    /// 2. Extract master block subgraph with shard blocks
    /// 3. Send to sync
    /// 4. Commit queue diff
    /// 5. Clean up from cache
    /// 6. Process delayed master state update if exists
    /// 7. Notify top processed anchor to mempool if block commited by received from bc
    pub(super) async fn commit_valid_master_block(
        &self,
        mc_block_id: &BlockId,
        skip_process_delayed_mc_state_update: bool,
    ) -> Result<Vec<CollatorJoinTask<CF::Collator>>> {
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Start to commit validated and valid master block ({})...",
            mc_block_id.as_short_id(),
        );

        // gc blocks from cache when commit finished
        scopeguard::defer!(self.blocks_cache.gc_prev_blocks());

        // we can process delayed master state update now
        let (mut top_processed_to_anchor_to_notify, collator_tasks) = self
            .notify_to_mempool_and_process_delayed_mc_state_update(
                mc_block_id,
                skip_process_delayed_mc_state_update,
            )
            .await?
            .unzip();

        let histogram = HistogramGuard::begin("tycho_collator_commit_valid_master_block_time");
        let histogram_extract =
            HistogramGuard::begin("tycho_collator_extract_master_block_subgraph_time");
        let mut extract_elapsed = Default::default();
        let mut sync_elapsed = Default::default();

        // extract master block with all shard blocks if valid, and process them
        match self
            .blocks_cache
            .extract_mc_block_subgraph_for_sync(mc_block_id)
        {
            McBlockSubgraphExtract::Extracted(subgraph) => {
                extract_elapsed = histogram_extract.finish();

                let partitions = subgraph.get_partitions();

                let McBlockSubgraph {
                    master_block,
                    shard_blocks,
                } = subgraph;

                // we can gc upto to current master block after commit
                // because we do not need to commit all previous blocks
                // because all previous blocks are already in bc state
                // and all previous diffs already committed by current one
                let to_blocks_keys = master_block.get_top_blocks_keys()?;
                self.blocks_cache.set_gc_to_boundary(&to_blocks_keys);

                // check if mc block was not sent to sync
                let mut mc_block_was_not_sent_to_sync = true;

                // send to sync only if was not received from bc
                if matches!(&master_block.data, BlockCacheEntryData::Collated {
                    received_after_collation: false,
                    ..
                }) {
                    let histogram =
                        HistogramGuard::begin("tycho_collator_send_blocks_to_sync_time");

                    mc_block_was_not_sent_to_sync =
                        !self.send_block_to_sync(master_block.data, Some(partitions.clone()))?;

                    for shard_block in shard_blocks {
                        self.send_block_to_sync(shard_block.data, None)?;
                    }

                    sync_elapsed = histogram.finish();
                    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
                        total = sync_elapsed.as_millis(),
                        "send_blocks_to_sync timings",
                    );

                    // if current master block was not applied to bc state yet
                    // then we should not notify top processed to anchor to mempool now
                    // because we are not sure that block will be applied
                    // and should wait until block_accepted event is received
                    top_processed_to_anchor_to_notify = None;
                } else {
                    // if current block was committed by received one
                    // then `on_block_accepted` will not be called further
                    // so we need to notify `top_processed_to_anchor` to mempool here
                    top_processed_to_anchor_to_notify = master_block.top_processed_to_anchor;
                }

                // if mc block was not sent to sync by any reason then
                // we will not receive OwnBlockApplied event,
                // so we have to commit messages queue right now
                if mc_block_was_not_sent_to_sync {
                    let _histogram = HistogramGuard::begin(
                        "tycho_collator_send_blocks_to_sync_commit_diffs_time",
                    );

                    Self::commit_block_queue_diff(
                        self.mq_adapter.clone(),
                        &master_block.block_id,
                        &master_block.top_shard_blocks_info,
                        &partitions,
                    )?;
                }
            }
            McBlockSubgraphExtract::AlreadyExtracted => {
                tracing::debug!(
                    target: tracing_targets::COLLATION_MANAGER,
                    "Master block subgraph is already extracted and cleaned up from cache ({}). Do nothing",
                    mc_block_id.as_short_id(),
                );
            }
        }

        tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
            total = histogram.finish().as_millis(),
            extract_subgraph = extract_elapsed.as_millis(),
            sync = sync_elapsed.as_millis(),
            "commit_valid_master_block timings",
        );

        // report last processed anchor to mempool
        if let Some(top_processed_to_anchor) = top_processed_to_anchor_to_notify {
            self.notify_top_processed_to_anchor_to_mempool(
                mc_block_id.seqno,
                top_processed_to_anchor,
            )
            .await?;
        }

        Ok(collator_tasks.unwrap_or_default())
    }

    fn send_block_to_sync(
        &self,
        data: BlockCacheEntryData,
        queue_partitions: Option<FastHashSet<QueuePartitionIdx>>,
    ) -> Result<bool> {
        let candidate_stuff = match data {
            BlockCacheEntryData::Collated {
                candidate_stuff,
                status,
                received_after_collation: false,
                ..
            } if status != CandidateStatus::Synced => candidate_stuff,
            // do not try to apply block because it is already applied,
            // more likely we have synced to some next applied mc block
            _ => return Ok(false),
        };

        let block_id = *candidate_stuff.candidate.block.id();
        self.state_node_adapter
            .accept_block(candidate_stuff.into_block_for_sync(queue_partitions))?;
        tracing::debug!(
            target: tracing_targets::COLLATION_MANAGER,
            "Block was successfully sent to sync ({})",
            block_id,
        );
        Ok(true)
    }
}
