use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::CollationConfig;
use ton_executor::{ExecuteParams, PreloadedBlockchainConfig};
use tycho_block_util::queue::QueueKey;

use super::execution_wrapper::ExecutorWrapper;
use super::prepare::PreparedState;
use super::{BlockCollationData, PrevData};
use crate::collator::execution_manager::{
    GetNextMessageGroupContext, GetNextMessageGroupMode, MessagesExecutor, MessagesReader,
};
use crate::collator::mq_iterator_adapter::{InitIteratorMode, QueueIteratorAdapter};
use crate::collator::types::MessagesBuffer;
use crate::collator::CollatorStdImpl;
use crate::tracing_targets;
use crate::types::McData;

pub struct Phase<S: PhaseState> {
    pub state: Box<ActualState>,
    pub extra: S,
}

pub trait PhaseState {}

pub struct ActualState {
    pub collation_config: Arc<CollationConfig>,
    pub collation_data: Box<BlockCollationData>,
    pub msgs_buffer: MessagesBuffer,
    pub mc_data: Arc<McData>,
    pub prev_shard_data: PrevData,
}

pub async fn prepare_collation(
    collator: &mut CollatorStdImpl,
    collation_config: Arc<CollationConfig>,
    mut collation_data: Box<BlockCollationData>,
    mut msgs_buffer: MessagesBuffer,
    mc_data: Arc<McData>,
    prev_shard_data: PrevData,
) -> Result<(Phase<PreparedState>, ExecutorWrapper)> {
    // init executor
    let executor = MessagesExecutor::new(
        collator.shard_id,
        collation_data.next_lt,
        Arc::new(PreloadedBlockchainConfig::with_config(
            mc_data.config.clone(),
            mc_data.global_id,
        )?),
        Arc::new(ExecuteParams {
            state_libs: mc_data.libraries.clone(),
            // generated unix time
            block_unixtime: collation_data.gen_utime,
            // block's start logical time
            block_lt: collation_data.start_lt,
            // block random seed
            seed_block: collation_data.rand_seed,
            block_version: collator.config.supported_block_version,
            ..ExecuteParams::default()
        }),
        prev_shard_data.observable_accounts().clone(),
        collation_config.work_units_params.execute.clone(),
    );

    // if this is a masterchain, we must take top shard blocks end lt
    let mc_top_shards_end_lts: Vec<_> = if collator.shard_id.is_masterchain() {
        collation_data
            .shards()?
            .iter()
            .map(|(k, v)| (*k, v.end_lt))
            .collect()
    } else {
        mc_data.shards.iter().map(|(k, v)| (*k, v.end_lt)).collect()
    };

    // create iterator adapter
    let mut mq_iterator_adapter = QueueIteratorAdapter::new(
        collator.shard_id,
        collator.mq_adapter.clone(),
        msgs_buffer.current_iterator_positions.take().unwrap(),
        mc_data.gen_lt,
        prev_shard_data.gen_lt(),
    );

    // we need to init iterator anyway because we need it to add new messages to queue
    tracing::debug!(target: tracing_targets::COLLATOR,
        "init iterator for current ranges"
    );
    mq_iterator_adapter
        .try_init_next_range_iterator(
            &mut collation_data.processed_upto,
            mc_top_shards_end_lts.iter().copied(),
            // We always init first iterator during block collation
            // with current ranges from processed_upto info
            // and do not touch next range before we read all existing messages buffer.
            // In this case the initial iterator range will be equal both
            // on Refill and on Continue.
            InitIteratorMode::OmitNextRange,
        )
        .await?;

    // create messages reader
    let mut messages_reader = MessagesReader::new(
        collator.shard_id,
        collation_config.msgs_exec_params.buffer_limit as _,
        mc_top_shards_end_lts,
    );

    // refill messages buffer and skip groups upto offset (on node restart)
    let prev_processed_offset = collation_data.processed_upto.processed_offset;
    if !msgs_buffer.has_pending_messages() && prev_processed_offset > 0 {
        tracing::debug!(target: tracing_targets::COLLATOR,
            prev_processed_offset,
            "refill messages buffer and skip groups upto",
        );

        while msgs_buffer.message_groups_offset() < prev_processed_offset {
            let msg_group = messages_reader
                .get_next_message_group(
                    GetNextMessageGroupContext {
                        next_chain_time: collation_data.get_gen_chain_time(),
                        max_new_message_key_to_current_shard: QueueKey::MIN,
                        mode: GetNextMessageGroupMode::Refill,
                    },
                    &mut collation_data.processed_upto,
                    &mut msgs_buffer,
                    &mut collator.anchors_cache,
                    &mut mq_iterator_adapter,
                )
                .await?;
            if msg_group.is_none() {
                // on restart from a new genesis we will not be able to refill buffer with externals
                // so we stop refilling when there is no more groups in buffer
                break;
            }
        }

        // next time we should read next message group like we did not make refill before
        // so we need to reset reader state that control from where to read messages
        messages_reader.reset_read_state();
    }

    // holds the max LT_HASH of a new created messages to current shard
    // it needs to define the read range for new messages when we get next message group
    let max_new_message_key_to_current_shard = QueueKey::MIN;

    Ok((
        Phase::<PreparedState> {
            state: Box::new(ActualState {
                collation_config,
                collation_data,
                msgs_buffer,
                mc_data,
                prev_shard_data,
            }),
            extra: PreparedState { messages_reader },
        },
        ExecutorWrapper::new(
            executor,
            max_new_message_key_to_current_shard,
            mq_iterator_adapter,
            collator.shard_id,
            collator.mq_adapter.clone(),
        ),
    ))
}
