use anyhow::Result;
use async_trait::async_trait;
use everscale_types::{cell::HashBytes, models::BlockId};

use crate::{
    mempool::MempoolAdapter,
    msg_queue::{MessageQueueAdapter, QueueIterator},
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{BlockCandidate, BlockCollationResult},
};

use super::{
    collator_processor::{CollatorProcessorSpecific, CollatorProcessorStdImpl},
    CollatorEventEmitter,
};

#[async_trait]
pub(super) trait DoCollate<MQ, MP, ST>:
    CollatorProcessorSpecific<MQ, MP, ST> + CollatorEventEmitter + Sized + Send + Sync + 'static
{
    async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_ids: Vec<BlockId>,
    ) -> Result<()>;
}

#[async_trait]
impl<MQ, QI, MP, ST> DoCollate<MQ, MP, ST> for CollatorProcessorStdImpl<MQ, QI, MP, ST>
where
    MQ: MessageQueueAdapter,
    QI: QueueIterator + Send + Sync + 'static,
    MP: MempoolAdapter,
    ST: StateNodeAdapter,
{
    async fn do_collate(
        &mut self,
        mut next_chain_time: u64,
        top_shard_blocks_ids: Vec<BlockId>,
    ) -> Result<()> {
        //TODO: make real implementation
        let _tracing_top_shard_blocks_descr = if top_shard_blocks_ids.is_empty() {
            "".to_string()
        } else {
            format!(
                ", top_shard_blocks: {:?}",
                top_shard_blocks_ids
                    .iter()
                    .map(|id| id.as_short_id().to_string())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
        };
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): next chain time: {}: start collating block...",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
            next_chain_time,
        );

        //STUB: just remove fisrt anchor from cache
        let _ext_msg = self.get_next_external();
        self.set_has_pending_externals(false);

        //STUB: just send dummy block to collation manager
        let prev_blocks_ids = self.working_state().prev_shard_data.blocks_ids().clone();
        let prev_block_id = prev_blocks_ids[0];
        let collated_block_id = BlockId {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };
        let new_state = self.working_state().prev_shard_data.pure_states()[0]
            .state()
            .clone();
        let collation_result = BlockCollationResult {
            candidate: BlockCandidate::new(
                collated_block_id,
                prev_blocks_ids,
                top_shard_blocks_ids,
                vec![],
                vec![],
                collated_block_id.file_hash,
                next_chain_time,
            ),
            new_state,
        };
        self.on_block_candidate_event(collation_result).await?;
        tracing::info!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): STUB: created and sent dummy block candidate...",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
        );

        self.update_working_state(collated_block_id)?;

        Ok(())
    }
}
