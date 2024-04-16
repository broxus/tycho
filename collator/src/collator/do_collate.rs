use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;

use everscale_types::{
    cell::{CellBuilder, HashBytes},
    models::{BlockId, BlockIdShort},
};
use rand::Rng;
use tycho_block_util::state::ShardStateStuff;

use crate::{
    collator::{
        collator_processor::execution_manager::ExecutionManager,
        types::{BlockCollationData, McData, OutMsgQueueInfoStuff, PrevData},
    },
    mempool::MempoolAdapter,
    msg_queue::{MessageQueueAdapter, QueueIterator},
    state_node::StateNodeAdapter,
    tracing_targets,
    types::{BlockCandidate, BlockCollationResult, ShardStateStuffExt},
};

use super::super::CollatorEventEmitter;

use super::{CollatorProcessorSpecific, CollatorProcessorStdImpl};

#[async_trait]
pub trait DoCollate<MQ, MP, ST>:
    CollatorProcessorSpecific<MQ, MP, ST> + CollatorEventEmitter + Sized + Send + Sync + 'static
{
    async fn do_collate(
        &mut self,
        next_chain_time: u64,
        top_shard_blocks_info: Vec<(BlockId, Arc<ShardStateStuff>)>,
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
        top_shard_blocks_info: Vec<(BlockId, Arc<ShardStateStuff>)>,
    ) -> Result<()> {
        //TODO: make real implementation
        let mc_data = &self.working_state().mc_data;
        let prev_shard_data = &self.working_state().prev_shard_data;

        let _tracing_top_shard_blocks_descr = if top_shard_blocks_info.is_empty() {
            "".to_string()
        } else {
            format!(
                ", top_shard_blocks: {:?}",
                top_shard_blocks_info
                    .iter()
                    .map(|(id, _)| id.as_short_id().to_string())
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

        //TODO: get rand seed from the anchor
        let rand_bytes = {
            let mut rng = rand::thread_rng();
            (0..32).map(|_| rng.gen::<u8>()).collect::<Vec<_>>()
        };
        let rand_seed = HashBytes::from_slice(rand_bytes.as_slice());

        // prepare block collation data
        //STUB: consider split/merge in future for taking prev_block_id
        let prev_block_id = prev_shard_data.blocks_ids()[0];
        let mut collation_data = BlockCollationData::default();
        collation_data.block_id_short = BlockIdShort {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
        };
        collation_data.rand_seed = rand_seed;

        //TODO: init ShardHashes
        if collation_data.block_id_short.shard.is_masterchain() {
            collation_data.top_shard_blocks =
                top_shard_blocks_info.iter().map(|(id, _)| *id).collect();
            collation_data.set_shards(Default::default());
        }

        collation_data.update_ref_min_mc_seqno(mc_data.mc_state_stuff().state().seqno);
        collation_data.chain_time = next_chain_time as u32;
        collation_data.start_lt = Self::calc_start_lt(
            self.collator_descr(),
            mc_data,
            prev_shard_data,
            &collation_data,
        )?;
        collation_data.max_lt = collation_data.start_lt + 1;

        //TODO: should consider split/merge in future
        let out_msg_queue_info = prev_shard_data.observable_states()[0]
            .state()
            .load_out_msg_queue_info()
            .unwrap_or_default();
        collation_data.out_msg_queue_stuff = OutMsgQueueInfoStuff {
            proc_info: out_msg_queue_info.proc_info,
        };
        collation_data.externals_processed_upto = prev_shard_data.observable_states()[0]
            .state()
            .externals_processed_upto
            .clone();

        // init execution manager
        let exec_manager = ExecutionManager::new(
            collation_data.chain_time,
            collation_data.start_lt,
            collation_data.max_lt,
            collation_data.rand_seed,
            mc_data.mc_state_stuff().state().libraries.clone(),
            mc_data.config().clone(),
            self.config.max_collate_threads,
        );

        //STUB: just remove fisrt anchor from cache
        let _ext_msg = self.get_next_external();
        self.set_has_pending_externals(false);

        //STUB: do not execute transactions and produce empty block

        // build block candidate and new state
        //TODO: return `new_state: ShardStateStuff`
        let (candidate, new_state_stuff) = self
            .finalize_block(&mut collation_data, exec_manager)
            .await?;

        /*
        //STUB: just send dummy block to collation manager
        let prev_blocks_ids = prev_shard_data.blocks_ids().clone();
        let prev_block_id = prev_blocks_ids[0];
        let collated_block_id_short = BlockIdShort {
            shard: prev_block_id.shard,
            seqno: prev_block_id.seqno + 1,
        };
        let mut builder = CellBuilder::new();
        builder.store_bit(collated_block_id_short.shard.workchain().is_negative())?;
        builder.store_u32(collated_block_id_short.shard.workchain().unsigned_abs())?;
        builder.store_u64(collated_block_id_short.shard.prefix())?;
        builder.store_u32(collated_block_id_short.seqno)?;
        let cell = builder.build()?;
        let hash = cell.repr_hash();
        let collated_block_id = BlockId {
            shard: collated_block_id_short.shard,
            seqno: collated_block_id_short.seqno,
            root_hash: *hash,
            file_hash: *hash,
        };
        let mut new_state = prev_shard_data.pure_states()[0]
            .state()
            .clone();
        new_state.seqno = collated_block_id.seqno;
        let candidate = BlockCandidate::new(
            collated_block_id,
            prev_blocks_ids,
            top_shard_blocks_ids,
            vec![],
            vec![],
            collated_block_id.file_hash,
            next_chain_time,
        );
        */

        let collation_result = BlockCollationResult {
            candidate,
            new_state_stuff: new_state_stuff.clone(),
        };
        self.on_block_candidate_event(collation_result).await?;
        tracing::info!(
            target: tracing_targets::COLLATOR,
            "Collator ({}{}): STUB: created and sent empty block candidate...",
            self.collator_descr(),
            _tracing_top_shard_blocks_descr,
        );

        self.update_working_state(new_state_stuff)?;

        Ok(())
    }
}

impl<MQ, QI, MP, ST> CollatorProcessorStdImpl<MQ, QI, MP, ST> {
    fn calc_start_lt(
        collator_descr: &str,
        mc_data: &McData,
        prev_shard_data: &PrevData,
        collation_data: &BlockCollationData,
    ) -> Result<u64> {
        tracing::trace!("Collator ({}): init_lt", collator_descr);

        let mut start_lt = if !collation_data.block_id_short.shard.is_masterchain() {
            std::cmp::max(
                mc_data.mc_state_stuff().state().gen_lt,
                prev_shard_data.gen_lt(),
            )
        } else {
            std::cmp::max(
                mc_data.mc_state_stuff().state().gen_lt,
                collation_data.shards_max_end_lt(),
            )
        };

        let align = mc_data.get_lt_align();
        let incr = align - start_lt % align;
        if incr < align || 0 == start_lt {
            if start_lt >= (!incr + 1) {
                bail!("cannot compute start logical time (uint64 overflow)");
            }
            start_lt += incr;
        }

        tracing::debug!(
            "Collator ({}): start_lt set to {}",
            collator_descr,
            start_lt
        );

        Ok(start_lt)
    }
}
