use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use ton_executor::{ExecuteParams, PreloadedBlockchainConfig};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;

use super::execute::ExecuteState;
use super::execution_wrapper::ExecutorWrapper;
use super::phase::{Phase, PhaseState};
use crate::collator::do_collate::phase::ActualState;
use crate::collator::execution_manager::MessagesExecutor;
use crate::collator::messages_reader::{MessagesReaderV2, ReaderState};
use crate::collator::mq_iterator_adapter::{InitIteratorMode, QueueIteratorAdapter};
use crate::collator::types::AnchorsCache;
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::{CollatorConfig, DisplayExternalsProcessedUpto};

pub struct PrepareState {
    config: Arc<CollatorConfig>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    reader_state: ReaderState,
    anchors_cache: AnchorsCache,
}

impl PhaseState for PrepareState {}

impl Phase<PrepareState> {
    pub fn new(
        config: Arc<CollatorConfig>,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
        reader_state: ReaderState,
        anchors_cache: AnchorsCache,
        state: Box<ActualState>,
    ) -> Self {
        Self {
            state,
            extra: PrepareState {
                config,
                mq_adapter,
                reader_state,
                anchors_cache,
            },
        }
    }

    pub fn run(mut self) -> Result<Phase<ExecuteState>> {
        // show initial processed upto
        tracing::debug!(target: tracing_targets::COLLATOR, "initial processed_upto.offset = {}",
            self.state.collation_data.processed_upto.processed_offset,
        );
        tracing::debug!(target: tracing_targets::COLLATOR, "initial processed_upto.externals = {:?}",
            self.state.collation_data.processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
        );
        self.state
            .collation_data
            .processed_upto
            .internals
            .iter()
            .for_each(|(shard_ident, processed_upto)| {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "initial processed_upto.internals for shard {}: {}",
                    shard_ident, processed_upto,
                );
            });

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
            FastHashMap::<ShardIdent, QueueKey>::default(),
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
        let mut messages_reader = MessagesReaderV2::new(
            self.state.shard_id,
            self.state.collation_config.msgs_exec_params.buffer_limit as _,
            self.state.collation_config.msgs_exec_params.group_limit as _,
            self.state.collation_config.msgs_exec_params.group_vert_size as _,
            mc_top_shards_end_lts,
            self.extra.reader_state,
            self.extra.anchors_cache,
            mq_iterator_adapter,
        );

        // refill messages buffer and skip groups upto offset (on node restart)
        let prev_processed_offset = self.state.collation_data.processed_upto.processed_offset;
        if !messages_reader.has_messages_in_buffers() && prev_processed_offset > 0 {
            // TODO: pass processed_upto from collation data
            messages_reader.refill_buffers_upto_offsets(&mut Default::default())?;
        }

        Ok(Phase::<ExecuteState> {
            extra: ExecuteState {
                messages_reader,
                execute_result: None,
                executor: ExecutorWrapper::new(
                    executor,
                    self.state.shard_id,
                    self.extra.mq_adapter.clone(),
                ),
            },
            state: self.state,
        })
    }
}
