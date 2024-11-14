use std::sync::Arc;

use anyhow::Result;
use ton_executor::{ExecuteParams, PreloadedBlockchainConfig};
use tycho_block_util::queue::QueueKey;

use super::execute::ExecuteState;
use super::execution_wrapper::ExecutorWrapper;
use super::phase::{Phase, PhaseState};
use crate::collator::do_collate::phase::ActualState;
use crate::collator::execution_manager::{
    GetNextMessageGroupContext, GetNextMessageGroupMode, MessagesExecutor, MessagesReader,
};
use crate::collator::mq_iterator_adapter::{InitIteratorMode, QueueIteratorAdapter};
use crate::collator::types::AnchorsCache;
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::CollatorConfig;

pub struct PrepareState {
    config: Arc<CollatorConfig>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    anchors_cache: AnchorsCache,
}

impl PhaseState for PrepareState {}

impl Phase<PrepareState> {
    pub fn new(
        config: Arc<CollatorConfig>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        anchors_cache: AnchorsCache,
        state: Box<ActualState>,
    ) -> Self {
        Self {
            state,
            extra: PrepareState {
                config,
                mq_adapter,
                anchors_cache,
            },
        }
    }

    pub fn run(mut self) -> Result<Phase<ExecuteState>> {
        // init executor
        let executor = MessagesExecutor::new(
            self.state.shard_id,
            self.state.collation_data.next_lt,
            Arc::new(PreloadedBlockchainConfig::with_config(
                self.state.mc_data.config.clone(),
                self.state.mc_data.global_id,
            )?),
            Arc::new(ExecuteParams {
                state_libs: self.state.mc_data.libraries.clone(),
                // generated unix time
                block_unixtime: self.state.collation_data.gen_utime,
                // block's start logical time
                block_lt: self.state.collation_data.start_lt,
                // block random seed
                seed_block: self.state.collation_data.rand_seed,
                block_version: self.extra.config.supported_block_version,
                ..ExecuteParams::default()
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

        // create iterator adapter
        let mut mq_iterator_adapter = QueueIteratorAdapter::new(
            self.state.shard_id,
            self.extra.mq_adapter.clone(),
            self.state
                .msgs_buffer
                .current_iterator_positions
                .take()
                .unwrap(),
            self.state.mc_data.gen_lt,
            self.state.prev_shard_data.gen_lt(),
        );

        // we need to init iterator anyway because we need it to add new messages to queue
        tracing::debug!(target: tracing_targets::COLLATOR,
            "init iterator for current ranges"
        );
        mq_iterator_adapter.try_init_next_range_iterator(
            &mut self.state.collation_data.processed_upto,
            mc_top_shards_end_lts.iter().copied(),
            // We always init first iterator during block collation
            // with current ranges from processed_upto info
            // and do not touch next range before we read all existing messages buffer.
            // In this case the initial iterator range will be equal both
            // on Refill and on Continue.
            InitIteratorMode::OmitNextRange,
        )?;

        // create messages reader
        let mut messages_reader = MessagesReader::new(
            self.state.shard_id,
            self.state.collation_config.msgs_exec_params.buffer_limit as _,
            mc_top_shards_end_lts,
        );

        // holds the max LT_HASH of a new created messages to current shard
        // it needs to define the read range for new messages when we get next message group
        let max_new_message_key_to_current_shard = QueueKey::MIN;

        // refill messages buffer and skip groups upto offset (on node restart)
        let prev_processed_offset = self.state.collation_data.processed_upto.processed_offset;
        if !self.state.msgs_buffer.has_pending_messages() && prev_processed_offset > 0 {
            tracing::debug!(target: tracing_targets::COLLATOR,
                prev_processed_offset,
                "refill messages buffer and skip groups upto",
            );

            while self.state.msgs_buffer.message_groups_offset() < prev_processed_offset {
                let msg_group = messages_reader.get_next_message_group(
                    GetNextMessageGroupContext {
                        next_chain_time: self.state.collation_data.get_gen_chain_time(),
                        max_new_message_key_to_current_shard,
                        mode: GetNextMessageGroupMode::Refill,
                    },
                    &mut self.state.collation_data.processed_upto,
                    &mut self.state.msgs_buffer,
                    &mut self.extra.anchors_cache,
                    &mut mq_iterator_adapter,
                )?;
                if msg_group.is_none() {
                    // on restart from a new genesis we will not be able to refill buffer with externals
                    // so we stop refilling when there is no more groups in buffer
                    break;
                }
            }

            // next time we should read next message group like we did not make refill before
            // so we need to reset flags that control from where to read messages
            messages_reader.reset_read_state();
        }

        Ok(Phase::<ExecuteState> {
            extra: ExecuteState {
                messages_reader,
                execute_result: None,
                anchors_cache: self.extra.anchors_cache,
                executor: ExecutorWrapper::new(
                    executor,
                    max_new_message_key_to_current_shard,
                    mq_iterator_adapter,
                    self.state.shard_id,
                    self.extra.mq_adapter.clone(),
                ),
            },
            state: self.state,
        })
    }
}
