use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::GlobalCapability;
use ton_executor::{ExecuteParams, PreloadedBlockchainConfig};

use super::execute::ExecuteState;
use super::execution_wrapper::ExecutorWrapper;
use super::phase::{Phase, PhaseState};
use crate::collator::do_collate::phase::ActualState;
use crate::collator::execution_manager::MessagesExecutor;
use crate::collator::messages_reader::{MessagesReader, MessagesReaderContext, ReaderState};
use crate::collator::types::AnchorsCache;
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;

pub struct PrepareState {
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    reader_state: ReaderState,
    anchors_cache: AnchorsCache,
}

impl PhaseState for PrepareState {}

impl Phase<PrepareState> {
    pub fn new(
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        reader_state: ReaderState,
        anchors_cache: AnchorsCache,
        state: Box<ActualState>,
    ) -> Self {
        Self {
            state,
            extra: PrepareState {
                mq_adapter,
                reader_state,
                anchors_cache,
            },
        }
    }

    pub fn run(self) -> Result<Phase<ExecuteState>> {
        // log initial processed upto
        tracing::debug!(target: tracing_targets::COLLATOR,
            "initial processed_upto = {:?}",
            self.state.prev_shard_data.processed_upto(),
        );

        // init executor
        let preloaded_bc_config = Arc::new(PreloadedBlockchainConfig::with_config(
            self.state.mc_data.config.clone(),
            self.state.mc_data.global_id,
        )?);
        let block_version = preloaded_bc_config.global_version().version;
        let signature_with_id = if preloaded_bc_config
            .global_version()
            .capabilities
            .contains(GlobalCapability::CapSignatureWithId)
        {
            Some(self.state.mc_data.global_id)
        } else {
            None
        };
        let executor = MessagesExecutor::new(
            self.state.shard_id,
            self.state.collation_data.next_lt,
            preloaded_bc_config,
            Arc::new(ExecuteParams {
                state_libs: self.state.mc_data.libraries.clone(),
                // generated unix time
                block_unixtime: self.state.collation_data.gen_utime,
                // block's start logical time
                block_lt: self.state.collation_data.start_lt,
                // block random seed
                seed_block: self.state.collation_data.rand_seed,
                block_version,
                behavior_modifiers: Some(tycho_vm::BehaviourModifiers {
                    signature_with_id,
                    ..Default::default()
                }),
                debug: false,
            }),
            self.state.prev_shard_data.observable_accounts().clone(),
            self.state
                .collation_config
                .work_units_params
                .execute
                .clone(),
        );

        // if this is a masterchain, we must take top shard blocks end lt
        let mc_top_shards_end_lts: Vec<_> = if self.state.shard_id.is_masterchain() {
            self.state
                .collation_data
                .get_shards()?
                .iter()
                .map(|(k, v)| (*k, v.end_lt))
                .collect()
        } else {
            self.state
                .mc_data
                .shards
                .iter()
                .map(|(k, v)| (*k, v.end_lt))
                .collect()
        };

        // create messages reader
        let mut messages_reader = MessagesReader::new(
            MessagesReaderContext {
                for_shard_id: self.state.shard_id,
                block_seqno: self.state.collation_data.block_id_short.seqno,
                next_chain_time: self.state.collation_data.get_gen_chain_time(),
                msgs_exec_params: Arc::new(self.state.collation_config.msgs_exec_params.clone()),
                mc_state_gen_lt: self.state.mc_data.gen_lt,
                prev_state_gen_lt: self.state.prev_shard_data.gen_lt(),
                mc_top_shards_end_lts,
                reader_state: self.extra.reader_state,
                anchors_cache: self.extra.anchors_cache,
            },
            self.extra.mq_adapter.clone(),
        )?;

        // refill messages buffer and skip groups upto offset (on node restart)
        if messages_reader.check_need_refill() {
            messages_reader.refill_buffers_upto_offsets()?;
        }

        Ok(Phase::<ExecuteState> {
            extra: ExecuteState {
                messages_reader,
                execute_result: None,
                executor: ExecutorWrapper::new(executor, self.state.shard_id),
            },
            state: self.state,
        })
    }
}
