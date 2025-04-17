use std::sync::Arc;

use everscale_types::models::GlobalCapability;
use tycho_executor::{ExecutorParams, ParsedConfig};

use super::execute::ExecuteState;
use super::execution_wrapper::ExecutorWrapper;
use super::phase::{Phase, PhaseState};
use crate::collator::do_collate::phase::ActualState;
use crate::collator::error::CollatorError;
use crate::collator::execution_manager::MessagesExecutor;
use crate::collator::messages_reader::{MessagesReader, MessagesReaderContext, ReaderState};
use crate::collator::types::AnchorsCache;
use crate::collator::CollationCancelReason;
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::processed_upto::build_all_shards_processed_to_by_partitions;

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

    pub fn run(self) -> Result<Phase<ExecuteState>, CollatorError> {
        // log initial processed upto
        tracing::debug!(target: tracing_targets::COLLATOR,
            "initial processed_upto = {:?}",
            self.state.prev_shard_data.processed_upto(),
        );

        // init executor
        let preloaded_bc_config = Arc::new(ParsedConfig::parse(
            self.state.mc_data.config.clone(),
            self.state.collation_data.gen_utime,
        )?);
        let capabilities = preloaded_bc_config.global.capabilities;
        let executor = MessagesExecutor::new(
            self.state.shard_id,
            self.state.collation_data.next_lt,
            preloaded_bc_config,
            Arc::new(ExecutorParams {
                libraries: self.state.mc_data.libraries.clone(),
                // generated unix time
                block_unixtime: self.state.collation_data.gen_utime,
                // block's start logical time
                block_lt: self.state.collation_data.start_lt,
                // block random seed
                rand_seed: self.state.collation_data.rand_seed,
                disable_delete_frozen_accounts: true,
                full_body_in_bounced: capabilities.contains(GlobalCapability::CapFullBodyInBounced),
                charge_action_fees_on_fail: true,
                vm_modifiers: tycho_vm::BehaviourModifiers {
                    signature_with_id: capabilities
                        .contains(GlobalCapability::CapSignatureWithId)
                        .then_some(self.state.mc_data.global_id),
                    ..Default::default()
                },
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

        let all_shards_processed_to_by_partitions = build_all_shards_processed_to_by_partitions(
            self.state.collation_data.block_id_short,
            self.extra
                .reader_state
                .get_updated_processed_upto()
                .get_internals_processed_to_by_partitions(),
            self.state
                .mc_data
                .processed_upto
                .get_internals_processed_to_by_partitions(),
            self.state
                .collation_data
                .mc_shards_processed_to_by_partitions
                .clone(),
            &self.state.mc_data.shards,
        );

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
                all_shards_processed_to_by_partitions,
                reader_state: self.extra.reader_state,
                anchors_cache: self.extra.anchors_cache,
                is_first_block_after_prev_master: self.state.is_first_block_after_prev_master,
            },
            self.extra.mq_adapter.clone(),
        )?;

        // metrics - sync finished
        let labels = [("workchain", self.state.shard_id.workchain().to_string())];
        metrics::gauge!("tycho_collator_sync_is_running", &labels).set(0);

        // refill messages buffer and skip groups upto offset (on node restart)
        if messages_reader.check_need_refill() {
            // metrics - refill is running
            metrics::gauge!("tycho_collator_refill_messages_is_running", &labels).set(1);

            let mut collation_is_cancelled_debounce = self.state.collation_is_cancelled.debounce(5);

            messages_reader
                .refill_buffers_upto_offsets(|| collation_is_cancelled_debounce.check())?;

            // metrics - refill finished
            metrics::gauge!("tycho_collator_refill_messages_is_running", &labels).set(0);

            // exit collation if cancelled
            if collation_is_cancelled_debounce.check() {
                return Err(CollatorError::Cancelled(
                    CollationCancelReason::ExternalCancel,
                ));
            }
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
