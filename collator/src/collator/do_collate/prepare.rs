use std::sync::Arc;

use everscale_types::models::{BlockId, BlockIdShort, GlobalCapability, ShardIdent};
use tycho_block_util::queue::QueueKey;
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
use crate::internal_queue::types::{EnqueuedMessage, QueueShardRange};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::{McData, ShardDescriptionShort};

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
                cumulative_stats_ranges: compute_cumulative_stats_ranges(
                    &self.state.mc_data,
                    self.state.collation_data.block_id_short,
                ),
                is_first_block_after_prev_master: is_first_block_after_prev_master(
                    self.state.prev_shard_data.blocks_ids()[0], // TODO: consider split/merge,
                    &self.state.mc_data.shards,
                ),
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

fn compute_cumulative_stats_ranges(
    mc_data: &Arc<McData>,
    next_block_id_short: BlockIdShort,
) -> Vec<QueueShardRange> {
    let mut ranges = vec![];

    if mc_data.block_id.seqno == 0 {
        return ranges;
    }

    let mut from_ranges = mc_data
        .processed_upto
        .get_min_internals_processed_to_by_shards();

    for processed_to_map in mc_data.shards_processed_to.values() {
        for (shard, processed_to) in processed_to_map {
            from_ranges
                .entry(*shard)
                .and_modify(|existing| {
                    if processed_to < existing {
                        *existing = *processed_to;
                    }
                })
                .or_insert(*processed_to);
        }
    }

    for (shard, processed_to) in from_ranges {
        let from = processed_to.next_value();

        if shard.is_masterchain() && shard == next_block_id_short.shard {
            let to = QueueKey::MAX;
            ranges.push(QueueShardRange {
                shard_ident: shard,
                from,
                to,
            });
        } else {
            for (current_shard, descr) in &mc_data.shards {
                if current_shard == &shard {
                    let to = QueueKey::max_for_lt(descr.end_lt);
                    ranges.push(QueueShardRange {
                        shard_ident: shard,
                        from,
                        to,
                    });
                    break;
                }
            }
        }
    }

    ranges
}

fn is_first_block_after_prev_master(
    prev_block_id: BlockId,
    mc_data_shards: &[(ShardIdent, ShardDescriptionShort)],
) -> bool {
    if prev_block_id.shard.is_masterchain() {
        return true;
    }

    for (shard, descr) in mc_data_shards {
        if shard == &prev_block_id.shard && descr.seqno >= prev_block_id.seqno {
            return true;
        }
    }

    false
}
