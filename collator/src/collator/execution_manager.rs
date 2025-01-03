use std::cmp;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use humantime::format_duration;
use rayon::prelude::*;
use ton_executor::{
    ExecuteParams, ExecutedTransaction, ExecutorOutput, OrdinaryTransactionExecutor,
    PreloadedBlockchainConfig, TickTockTransactionExecutor, TransactionExecutor,
};
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use super::messages_buffer::MessageGroup;
use super::types::{AccountId, ParsedMessage, ShardAccountStuff};
use crate::tracing_targets;

#[cfg(test)]
#[path = "tests/execution_manager_tests.rs"]
pub(super) mod tests;

#[cfg(FALSE)]
pub(super) struct MessagesReader {
    shard_id: ShardIdent,
    /// max number of messages that could be loaded into runtime
    messages_buffer_limit: usize,

    /// flag indicates that should read ext messages
    read_ext_messages: bool,
    /// flag indicates that should read new messages
    read_new_messages: bool,

    /// last read to anchor chain time
    last_read_to_anchor_chain_time: Option<u64>,

    /// sum total time of reading existing internal messages
    read_existing_messages_total_elapsed: Duration,
    /// sum total time of reading new internal messages
    read_new_messages_total_elapsed: Duration,
    /// sum total time of reading external messages
    read_ext_messages_total_elapsed: Duration,
    /// sum total time of adding messages to groups
    add_to_message_groups_total_elapsed: Duration,

    /// end lt list from top shards of mc block
    mc_top_shards_end_lts: Vec<(ShardIdent, u64)>,

    read_int_msgs_from_iterator_count: u64,
    read_ext_msgs_count: u64,
    read_new_msgs_from_iterator_count: u64,
}

#[cfg(FALSE)]
impl MessagesReader {
    pub fn new(
        shard_id: ShardIdent,
        messages_buffer_limit: usize,
        mc_top_shards_end_lts: Vec<(ShardIdent, u64)>,
    ) -> Self {
        metrics::gauge!("tycho_do_collate_msgs_exec_params_buffer_limit")
            .set(messages_buffer_limit as f64);

        Self {
            shard_id,
            messages_buffer_limit,
            read_ext_messages: false,
            read_new_messages: false,
            read_existing_messages_total_elapsed: Duration::ZERO,
            read_new_messages_total_elapsed: Duration::ZERO,
            read_ext_messages_total_elapsed: Duration::ZERO,
            add_to_message_groups_total_elapsed: Duration::ZERO,
            last_read_to_anchor_chain_time: None,
            mc_top_shards_end_lts,

            read_int_msgs_from_iterator_count: 0,
            read_ext_msgs_count: 0,
            read_new_msgs_from_iterator_count: 0,
        }
    }

    pub fn reset_read_state(&mut self) {
        self.read_ext_messages = false;
        self.read_new_messages = false;

        self.read_int_msgs_from_iterator_count = 0;
        self.read_ext_msgs_count = 0;
        self.read_new_msgs_from_iterator_count = 0;
    }

    pub fn last_read_to_anchor_chain_time(&self) -> Option<u64> {
        self.last_read_to_anchor_chain_time
    }

    pub fn read_existing_messages_total_elapsed(&self) -> Duration {
        self.read_existing_messages_total_elapsed
    }

    pub fn read_new_messages_total_elapsed(&self) -> Duration {
        self.read_new_messages_total_elapsed
    }

    pub fn read_ext_messages_total_elapsed(&self) -> Duration {
        self.read_ext_messages_total_elapsed
    }

    pub fn add_to_message_groups_total_elapsed(&self) -> Duration {
        self.add_to_message_groups_total_elapsed
    }

    pub fn read_int_msgs_from_iterator_count(&self) -> u64 {
        self.read_int_msgs_from_iterator_count
    }

    pub fn read_ext_msgs_count(&self) -> u64 {
        self.read_ext_msgs_count
    }

    pub fn read_new_msgs_from_iterator_count(&self) -> u64 {
        self.read_new_msgs_from_iterator_count
    }

    #[tracing::instrument(skip_all)]
    pub fn get_next_message_group(
        &mut self,
        cx: GetNextMessageGroupContext,
        processed_upto: &mut ProcessedUptoInfoStuff,
        msgs_buffer: &mut MessagesBuffer,
        anchors_cache: &mut AnchorsCache,
        mq_iterator_adapter: &mut QueueIteratorAdapter<EnqueuedMessage>,
    ) -> Result<Option<MessageGroup>> {
        // messages polling logic differs regarding existing and new messages

        let mut group_opt = None;

        let init_iterator_mode = match cx.mode {
            GetNextMessageGroupMode::Continue => InitIteratorMode::UseNextRange,
            GetNextMessageGroupMode::Refill => InitIteratorMode::OmitNextRange,
        };

        // when buffer contains externals from prev collation
        // we should process them all before reading existing internals
        if msgs_buffer.message_groups.ext_messages_count() > 0 && !self.read_ext_messages {
            // just extract message group with externals from buffer
            group_opt = msgs_buffer.message_groups.extract_first_group();

            if msgs_buffer.message_groups.is_empty() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "all externals from message_groups buffer where processed, will read existing internals"
                );

                // set all read externals as processed
                if let Some(externals) = processed_upto.externals.as_mut() {
                    if externals.processed_to != externals.read_to {
                        externals.processed_to = externals.read_to;
                        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
                            processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
                        );
                    }
                }

                self.last_read_to_anchor_chain_time = None;

                msgs_buffer.message_groups.reset();
            }
        }

        // when all externals from prev collation were processed should read existing internals
        if group_opt.is_none() && !self.read_ext_messages && !self.read_new_messages {
            // for existing messages we use ranged iterator and process maximum possible groups in parallel

            let timer = std::time::Instant::now();
            let mut add_to_groups_elapsed = Duration::ZERO;

            // read messages from iterator and fill messages groups
            // until the first group fully loaded
            // or max messages buffer limit reached
            let mut existing_internals_read_count = 0;
            while let Some(int_msg) = mq_iterator_adapter.next_existing_message()? {
                assert!(!int_msg.is_new);

                existing_internals_read_count += 1;

                let timer_add_to_groups = std::time::Instant::now();
                msgs_buffer
                    .message_groups
                    .add_message(Box::new(ParsedMessage {
                        info: MsgInfo::Int(int_msg.item.message.info.clone()),
                        dst_in_current_shard: true,
                        cell: int_msg.item.message.cell.clone(),
                        special_origin: None,
                        dequeued: Some(Dequeued {
                            same_shard: int_msg.item.source == self.shard_id,
                        }),
                    }));
                add_to_groups_elapsed += timer_add_to_groups.elapsed();

                if msgs_buffer.message_groups.messages_count() >= self.messages_buffer_limit {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "message_groups buffer filled on {}/{}, stop reading existing internals",
                        msgs_buffer.message_groups.messages_count(), self.messages_buffer_limit,
                    );
                    break;
                }

                if msgs_buffer.message_groups.first_group_is_full() {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "first message group is full, stop reading existing internals",
                    );
                    break;
                }
            }
            self.read_int_msgs_from_iterator_count += existing_internals_read_count;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "existing_internals_read_count={}, buffer int={}, ext={}",
                existing_internals_read_count,
                msgs_buffer.message_groups.int_messages_count(), msgs_buffer.message_groups.ext_messages_count(),
            );

            group_opt = msgs_buffer.message_groups.extract_first_group();

            self.read_existing_messages_total_elapsed += timer.elapsed();
            self.read_existing_messages_total_elapsed -= add_to_groups_elapsed;
            self.add_to_message_groups_total_elapsed += add_to_groups_elapsed;

            // when message_groups buffer is empty and no more existing internals in current iterator
            // then set all read messages as processed
            // and try to init iterator for the next available ranges
            if msgs_buffer.message_groups.is_empty()
                && mq_iterator_adapter.no_pending_existing_internals()
            {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "message_groups buffer is empty and there are no pending existing internals, \
                    will try to init iterator for next available ranges"
                );

                // set all read existing internals as processed
                let updated_processed_to = set_int_upto_all_processed(processed_upto);

                // commit processed messages to iterator
                mq_iterator_adapter
                    .iterator()
                    .commit(updated_processed_to)?;

                msgs_buffer.message_groups.reset();

                let next_range_iterator_initialized = mq_iterator_adapter
                    .try_init_next_range_iterator(
                        processed_upto,
                        self.mc_top_shards_end_lts.iter().copied(),
                        init_iterator_mode,
                    )?;
                if !next_range_iterator_initialized {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "next available ranges for internals are not exist or skipped, \
                        will read externals"
                    );
                    self.read_ext_messages = true;
                }
            }
        }

        // when all available existing internals were processed should read externals
        if group_opt.is_none() && self.read_ext_messages && !self.read_new_messages {
            let timer = std::time::Instant::now();
            let mut add_to_groups_elapsed = Duration::ZERO;

            let read_next_externals_mode = match cx.mode {
                GetNextMessageGroupMode::Continue => ReadNextExternalsMode::ToTheEnd,
                GetNextMessageGroupMode::Refill => ReadNextExternalsMode::ToPreviuosReadTo,
            };

            tracing::debug!(target: tracing_targets::COLLATOR,
                ?read_next_externals_mode,
                externals_reader_position = ?msgs_buffer.current_ext_reader_position,
                processed_upto_externals = ?processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
                "start reading next externals",
            );

            let mut stopped_on_prev_read_to_reached;

            let mut expired_anchors_count = 0_u32;
            let mut expired_ext_msgs_count = 0_u64;
            let mut externals_read_count = 0;
            loop {
                let ParsedExternals {
                    ext_messages,
                    current_reader_position,
                    last_read_to_anchor_chain_time,
                    was_stopped_on_prev_read_to_reached,
                    count_expired_anchors,
                    count_expired_messages,
                } = CollatorStdImpl::read_next_externals(
                    &self.shard_id,
                    anchors_cache,
                    3,
                    cx.next_chain_time,
                    &mut None,
                    msgs_buffer.current_ext_reader_position,
                    read_next_externals_mode,
                )?;
                msgs_buffer.current_ext_reader_position = current_reader_position;

                // update last read_to anchor chain time only when next anchor was read
                if let Some(ct) = last_read_to_anchor_chain_time {
                    self.last_read_to_anchor_chain_time = Some(ct);
                }

                stopped_on_prev_read_to_reached = was_stopped_on_prev_read_to_reached;

                expired_anchors_count = expired_anchors_count.saturating_add(count_expired_anchors);
                expired_ext_msgs_count =
                    expired_ext_msgs_count.saturating_add(count_expired_messages);
                externals_read_count += ext_messages.len() as u64;

                let timer_add_to_groups = std::time::Instant::now();
                for ext_msg in ext_messages {
                    msgs_buffer.message_groups.add_message(ext_msg);
                }
                add_to_groups_elapsed += timer_add_to_groups.elapsed();

                if msgs_buffer.message_groups.messages_count() >= self.messages_buffer_limit {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "message_groups buffer filled on {}/{}, stop reading externals",
                        msgs_buffer.message_groups.messages_count(), self.messages_buffer_limit,
                    );
                    break;
                }

                if msgs_buffer.message_groups.first_group_is_full() {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "first message group is full, stop reading externals",
                    );
                    break;
                }

                if stopped_on_prev_read_to_reached {
                    break;
                }

                if !anchors_cache.has_pending_externals() {
                    break;
                }
            }
            self.read_ext_msgs_count += externals_read_count;

            tracing::debug!(target: tracing_targets::COLLATOR,
                ?read_next_externals_mode,
                externals_reader_position = ?msgs_buffer.current_ext_reader_position,
                processed_upto_externals = ?processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
                stopped_on_prev_read_to_reached,
                expired_anchors_count,
                expired_ext_msgs_count,
                has_pending_externals = anchors_cache.has_pending_externals(),
                "externals_read_count={}, buffer int={}, ext={}",
                externals_read_count,
                msgs_buffer.message_groups.int_messages_count(),
                msgs_buffer.message_groups.ext_messages_count(),
            );

            group_opt = msgs_buffer.message_groups.extract_first_group();

            self.read_ext_messages_total_elapsed += timer.elapsed();
            self.read_ext_messages_total_elapsed -= add_to_groups_elapsed;
            self.add_to_message_groups_total_elapsed += add_to_groups_elapsed;

            if msgs_buffer.message_groups.is_empty() && !anchors_cache.has_pending_externals() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "message_groups buffer is empty and there are no pending externals, will read new internals"
                );

                // set all read externals as processed
                if let Some(externals) = processed_upto.externals.as_mut() {
                    if externals.processed_to != externals.read_to {
                        externals.processed_to = externals.read_to;
                        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
                        processed_upto.externals.as_ref().map(DisplayExternalsProcessedUpto),
                        );
                    }
                }

                self.last_read_to_anchor_chain_time = None;

                msgs_buffer.message_groups.reset();

                self.read_new_messages = true;
            }
        }

        // when all existing internals and externals were processed should read new internals
        if group_opt.is_none() && self.read_new_messages {
            // when processing new messages we return group immediately when the next message does not fit it

            let timer = std::time::Instant::now();
            // first new messages epoch is from existing internals and externals
            // then we read next epoch of new messages only when the previous epoch processed
            mq_iterator_adapter
                .try_update_new_messages_read_to(&cx.max_new_message_key_to_current_shard)?;

            let mut add_to_groups_elapsed = Duration::ZERO;

            let mut new_internals_read_count = 0;
            while let Some(int_msg) = mq_iterator_adapter.next_new_message()? {
                assert!(int_msg.is_new);

                new_internals_read_count += 1;

                let timer_add_to_groups = std::time::Instant::now();
                msgs_buffer
                    .message_groups
                    .add_message(Box::new(ParsedMessage {
                        info: MsgInfo::Int(int_msg.item.message.info.clone()),
                        dst_in_current_shard: true,
                        cell: int_msg.item.message.cell.clone(),
                        special_origin: None,
                        dequeued: None,
                    }));
                add_to_groups_elapsed += timer_add_to_groups.elapsed();

                if msgs_buffer.message_groups.messages_count() >= self.messages_buffer_limit {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "message_groups buffer filled on {}/{}, stop reading new internals",
                        msgs_buffer.message_groups.messages_count(), self.messages_buffer_limit,
                    );
                    break;
                }

                if msgs_buffer.message_groups.len() > 1 {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "next new message does not fit first group, stop reading new internals",
                    );
                    break;
                }
            }
            self.read_new_msgs_from_iterator_count += new_internals_read_count;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "new_internals_read_count={}, buffer int={}, ext={}",
                new_internals_read_count,
                msgs_buffer.message_groups.int_messages_count(), msgs_buffer.message_groups.ext_messages_count(),
            );

            // when we have 2 groups, the second one contains only one message
            // that does not fit first group,
            // so append this one message to first group (merge)
            group_opt = msgs_buffer.message_groups.extract_merged_group();

            self.read_new_messages_total_elapsed += timer.elapsed();
            self.read_new_messages_total_elapsed -= add_to_groups_elapsed;
            self.add_to_message_groups_total_elapsed += add_to_groups_elapsed;

            // actually, we process all message groups with new messages in one step,
            // so we update internals processed_upto each step
            if msgs_buffer.message_groups.is_empty()
                && msgs_buffer.message_groups.max_message_key() > &QueueKey::MIN
            {
                update_internals_processed_upto(
                    processed_upto,
                    self.shard_id,
                    Some(ProcessedUptoUpdate::Force(
                        *msgs_buffer.message_groups.max_message_key(),
                    )),
                    Some(ProcessedUptoUpdate::Force(
                        *msgs_buffer.message_groups.max_message_key(),
                    )),
                );

                // commit processed message to iterator
                mq_iterator_adapter.iterator().commit(vec![(
                    self.shard_id,
                    *msgs_buffer.message_groups.max_message_key(),
                )])?;

                msgs_buffer.message_groups.reset();
            }
        }

        // store actual offset of current interator range
        if processed_upto.processed_offset != msgs_buffer.message_groups.offset() {
            processed_upto.processed_offset = msgs_buffer.message_groups.offset();
            tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.offset = {}",
                processed_upto.processed_offset,
            );
        }

        Ok(group_opt)
    }
}

pub(super) struct MessagesExecutor {
    shard_id: ShardIdent,
    // this time is used if account's lt is smaller
    min_next_lt: u64,
    /// blockchain config
    config: Arc<PreloadedBlockchainConfig>,
    /// vm execution params related to current block
    params: Arc<ExecuteParams>,
    /// shard accounts
    accounts_cache: AccountsCache,
    /// Params to calculate messages execution work in work units
    wu_params_execute: WorkUnitsParamsExecute,
}

impl MessagesExecutor {
    pub fn new(
        shard_id: ShardIdent,
        min_next_lt: u64,
        config: Arc<PreloadedBlockchainConfig>,
        params: Arc<ExecuteParams>,
        shard_accounts: ShardAccounts,
        wu_params_execute: WorkUnitsParamsExecute,
    ) -> Self {
        Self {
            shard_id,
            min_next_lt,
            config,
            params,
            accounts_cache: AccountsCache {
                shard_accounts,
                items: Default::default(),
            },
            wu_params_execute,
        }
    }

    pub fn min_next_lt(&self) -> u64 {
        self.min_next_lt
    }

    pub fn executor_params(&self) -> &Arc<ExecuteParams> {
        &self.params
    }

    pub fn into_accounts_cache_raw(
        self,
    ) -> (
        impl ExactSizeIterator<Item = Box<ShardAccountStuff>>,
        ShardAccounts,
    ) {
        let AccountsCache {
            shard_accounts,
            items,
        } = self.accounts_cache;
        (items.into_values(), shard_accounts)
    }

    pub fn take_account_stuff_if<F>(
        &mut self,
        account_id: &AccountId,
        f: F,
    ) -> Result<Option<Box<ShardAccountStuff>>>
    where
        F: FnOnce(&ShardAccountStuff) -> bool,
    {
        self.accounts_cache.take_account_stuff_if(account_id, f)
    }

    /// Run one execution group of messages by accounts
    pub fn execute_group(&mut self, msg_group: MessageGroup) -> Result<ExecutedGroup> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "execute messages group");

        let labels = &[("workchain", self.shard_id.workchain().to_string())];
        let mut ext_msgs_skipped = 0;

        // TODO: msgs-v3: rename to group_slots_count
        let group_horizontal_size = msg_group.len();
        let group_messages_count = msg_group.messages_count();
        let group_mean_vert_size: usize = group_messages_count
            .checked_div(group_horizontal_size)
            .unwrap_or_default();
        let mut group_max_vert_size = 0;

        let mut items = Vec::with_capacity(group_messages_count);
        let mut ext_msgs_error_count = 0;

        let mut max_account_msgs_exec_time = Duration::ZERO;
        let mut total_exec_time = Duration::ZERO;

        let mut total_exec_wu = 0u128;

        let min_next_lt = self.min_next_lt;
        let config = self.config.clone();
        let params = self.params.clone();

        let result = msg_group
            .into_par_iter()
            .map_with(
                (config, params, Arc::new(&self.accounts_cache)),
                |(config, params, accounts_cache), (account_id, msgs)| {
                    Self::execute_subgroup(
                        account_id,
                        msgs,
                        accounts_cache,
                        min_next_lt,
                        config,
                        params,
                    )
                },
            )
            .collect_vec_list();

        for result in result {
            for executed in result {
                self.save_subgroup_result(
                    &mut ext_msgs_skipped,
                    &mut max_account_msgs_exec_time,
                    &mut total_exec_time,
                    &mut ext_msgs_error_count,
                    &mut group_max_vert_size,
                    &mut total_exec_wu,
                    &mut items,
                    executed?,
                )?;
            }
        }

        let subgroup_count = {
            let subgroup_size = self.wu_params_execute.subgroup_size.max(1) as usize;
            group_horizontal_size.div_ceil(subgroup_size)
        };
        let total_exec_wu = if subgroup_count == 0 {
            0
        } else {
            total_exec_wu.saturating_div(subgroup_count as u128) as u64
        };

        let mean_account_msgs_exec_time = total_exec_time
            .checked_div(group_horizontal_size as u32)
            .unwrap_or_default();

        tracing::trace!(target: tracing_targets::EXEC_MANAGER,
            group_horizontal_size, group_max_vert_size,
            total_exec_time = %format_duration(total_exec_time),
            mean_account_msgs_exec_time = %format_duration(mean_account_msgs_exec_time),
            max_account_msgs_exec_time = %format_duration(max_account_msgs_exec_time),
            total_exec_wu, group_messages_count,
            "execute_group",
        );

        metrics::gauge!("tycho_do_collate_one_tick_group_messages_count", labels)
            .set(group_messages_count as f64);
        metrics::gauge!("tycho_do_collate_one_tick_group_horizontal_size", labels)
            .set(group_horizontal_size as f64);
        metrics::gauge!("tycho_do_collate_one_tick_group_mean_vert_size", labels)
            .set(group_mean_vert_size as f64);
        metrics::gauge!("tycho_do_collate_one_tick_group_max_vert_size", labels)
            .set(group_max_vert_size as f64);
        metrics::histogram!(
            "tycho_do_collate_one_tick_account_msgs_exec_mean_time",
            labels
        )
        .record(mean_account_msgs_exec_time);
        metrics::histogram!(
            "tycho_do_collate_one_tick_account_msgs_exec_max_time",
            labels
        )
        .record(max_account_msgs_exec_time);

        Ok(ExecutedGroup {
            items,
            ext_msgs_error_count,
            ext_msgs_skipped,
            total_exec_wu,
        })
    }

    #[allow(clippy::vec_box)]
    fn execute_subgroup(
        account_id: HashBytes,
        msgs: Vec<Box<ParsedMessage>>,
        accounts_cache: &AccountsCache,
        min_next_lt: u64,
        config: &Arc<PreloadedBlockchainConfig>,
        params: &Arc<ExecuteParams>,
    ) -> Result<ExecutedTransactions> {
        let shard_account_stuff = accounts_cache.get_account_stuff(&account_id)?;
        Self::execute_messages(shard_account_stuff, msgs, min_next_lt, config, params)
    }

    #[allow(clippy::too_many_arguments)]
    fn save_subgroup_result(
        &mut self,
        ext_msgs_skipped: &mut u64,
        max_account_msgs_exec_time: &mut Duration,
        total_exec_time: &mut Duration,
        ext_msgs_error_count: &mut u64,
        group_max_vert_size: &mut usize,
        total_exec_wu: &mut u128,
        items: &mut Vec<ExecutedTickItem>,
        executed: ExecutedTransactions,
    ) -> Result<()> {
        *ext_msgs_skipped += executed.ext_msgs_skipped;

        let mut current_wu = 0u64;

        *max_account_msgs_exec_time = (*max_account_msgs_exec_time).max(executed.exec_time);
        *total_exec_time += executed.exec_time;
        *group_max_vert_size = cmp::max(*group_max_vert_size, executed.transactions.len());

        for tx in executed.transactions {
            if matches!(&tx.in_message.info, MsgInfo::ExtIn(_)) {
                if let Err(e) = &tx.result {
                    tracing::warn!(
                        target: tracing_targets::EXEC_MANAGER,
                        account_addr = %executed.account_state.account_addr,
                        message_hash = %tx.in_message.cell.repr_hash(),
                        "failed to execute external message: {e:?}",
                    );
                    *ext_msgs_error_count += 1;
                    continue;
                }
            }

            let executed = tx.result?;

            self.min_next_lt = cmp::max(self.min_next_lt, executed.next_lt);

            current_wu = current_wu
                .saturating_add(self.wu_params_execute.prepare as u64)
                .saturating_add(
                    executed
                        .gas_used
                        .saturating_mul(self.wu_params_execute.execute as u64)
                        .saturating_div(self.wu_params_execute.execute_delimiter as u64),
                );

            items.push(ExecutedTickItem {
                in_message: tx.in_message,
                executed,
            });
        }

        self.accounts_cache
            .add_account_stuff(executed.account_state);

        *total_exec_wu = total_exec_wu.saturating_add(current_wu as _);

        Ok(())
    }

    #[allow(clippy::vec_box)]
    fn execute_messages(
        mut account_state: Box<ShardAccountStuff>,
        msgs: Vec<Box<ParsedMessage>>,
        min_next_lt: u64,
        config: &Arc<PreloadedBlockchainConfig>,
        params: &Arc<ExecuteParams>,
    ) -> Result<ExecutedTransactions> {
        let mut ext_msgs_skipped = 0;
        let timer = std::time::Instant::now();

        let mut transactions = Vec::with_capacity(msgs.len());
        let account_is_empty = account_state.is_empty()?;

        for msg in msgs {
            if msg.is_external() && account_is_empty {
                ext_msgs_skipped += 1;
                continue;
            }
            transactions.push(execute_ordinary_transaction_impl(
                &mut account_state,
                msg,
                min_next_lt,
                config,
                params,
            )?);
        }

        Ok(ExecutedTransactions {
            account_state,
            transactions,
            exec_time: timer.elapsed(),
            ext_msgs_skipped,
        })
    }

    /// Executes a single ordinary transaction.
    pub fn execute_ordinary_transaction(
        &mut self,
        mut account_stuff: Box<ShardAccountStuff>,
        in_message: Box<ParsedMessage>,
    ) -> Result<ExecutedOrdinaryTransaction> {
        let min_next_lt = self.min_next_lt;
        let config = self.config.clone();
        let params = self.params.clone();

        let (account_stuff, executed) = execute_ordinary_transaction_impl(
            &mut account_stuff,
            in_message,
            min_next_lt,
            &config,
            &params,
        )
        .map(|executed| (account_stuff, executed))?;

        if let Ok(tx) = &executed.result {
            self.min_next_lt = cmp::max(min_next_lt, tx.next_lt);
        }
        self.accounts_cache.add_account_stuff(account_stuff);
        Ok(executed)
    }

    /// Executes a single ticktock transaction.
    pub fn execute_ticktock_transaction(
        &mut self,
        mut account_stuff: Box<ShardAccountStuff>,
        tick_tock: TickTock,
    ) -> Result<ExecutedTransaction> {
        let min_next_lt = self.min_next_lt;
        let config = self.config.clone();
        let params = self.params.clone();

        let (account_stuff, executed) = execute_ticktock_transaction(
            &mut account_stuff,
            tick_tock,
            min_next_lt,
            &config,
            &params,
        )
        .map(|executed| (account_stuff, executed))?;

        self.min_next_lt = cmp::max(min_next_lt, executed.next_lt);
        self.accounts_cache.add_account_stuff(account_stuff);
        Ok(executed)
    }
}

struct AccountsCache {
    shard_accounts: ShardAccounts,
    items: FastHashMap<AccountId, Box<ShardAccountStuff>>,
}

impl AccountsCache {
    fn take_account_stuff_if<F>(
        &mut self,
        account_id: &AccountId,
        f: F,
    ) -> Result<Option<Box<ShardAccountStuff>>>
    where
        F: FnOnce(&ShardAccountStuff) -> bool,
    {
        match self.items.entry(*account_id) {
            Entry::Occupied(entry) => {
                if f(entry.get()) {
                    return Ok(Some(entry.remove()));
                }
            }
            Entry::Vacant(entry) => {
                if let Some((_, state)) = self.shard_accounts.get(account_id)? {
                    let account_stuff = ShardAccountStuff::new(account_id, state).map(Box::new)?;
                    if f(&account_stuff) {
                        return Ok(Some(account_stuff));
                    }

                    // NOTE: Reuse preloaded account state as it might be used later
                    entry.insert(account_stuff);
                }
            }
        }

        Ok(None)
    }

    fn get_account_stuff(&self, account_id: &AccountId) -> Result<Box<ShardAccountStuff>> {
        if let Some(account) = self.items.get(account_id) {
            Ok(account.clone())
        } else if let Some((_depth, shard_account)) = self.shard_accounts.get(account_id)? {
            ShardAccountStuff::new(account_id, shard_account).map(Box::new)
        } else {
            Ok(Box::new(ShardAccountStuff::new_empty(account_id)))
        }
    }

    fn add_account_stuff(&mut self, account_stuff: Box<ShardAccountStuff>) {
        tracing::trace!(
            target: tracing_targets::EXEC_MANAGER,
            account_addr = %account_stuff.account_addr,
            "updating shard account"
        );

        self.items.insert(account_stuff.account_addr, account_stuff);
    }
}

pub struct ExecutedGroup {
    pub items: Vec<ExecutedTickItem>,
    pub ext_msgs_error_count: u64,
    pub ext_msgs_skipped: u64,
    pub total_exec_wu: u64,
}

pub struct ExecutedTickItem {
    pub in_message: Box<ParsedMessage>,
    pub executed: ExecutedTransaction,
}

pub struct ExecutedTransactions {
    pub account_state: Box<ShardAccountStuff>,
    pub transactions: Vec<ExecutedOrdinaryTransaction>,
    pub exec_time: Duration,
    pub ext_msgs_skipped: u64,
}

pub struct ExecutedOrdinaryTransaction {
    pub result: Result<ExecutedTransaction>,
    pub in_message: Box<ParsedMessage>,
}

fn execute_ordinary_transaction_impl(
    account_stuff: &mut ShardAccountStuff,
    in_message: Box<ParsedMessage>,
    min_lt: u64,
    config: &PreloadedBlockchainConfig,
    params: &ExecuteParams,
) -> Result<ExecutedOrdinaryTransaction> {
    tracing::trace!(
        target: tracing_targets::EXEC_MANAGER,
        account_addr = %account_stuff.account_addr,
        message_hash = %in_message.cell.repr_hash(),
        message_kind = ?in_message.kind(),
        "executing ordinary message",
    );

    let _histogram = HistogramGuard::begin("tycho_collator_execute_ordinary_time");

    let shard_account = &mut account_stuff.shard_account;
    let result = OrdinaryTransactionExecutor::new().execute_with_libs_and_params(
        Some(&in_message.cell),
        shard_account,
        min_lt,
        params,
        config,
    );

    let result = match result {
        Ok((
            total_fees,
            ExecutorOutput {
                account,
                transaction,
            },
        )) => {
            let tx_lt = shard_account.last_trans_lt;
            account_stuff.apply_transaction(tx_lt, total_fees, account, &transaction);
            Ok(transaction)
        }
        Err(e) => Err(e),
    };

    Ok(ExecutedOrdinaryTransaction { result, in_message })
}

fn execute_ticktock_transaction(
    account_stuff: &mut ShardAccountStuff,
    tick_tock: TickTock,
    min_lt: u64,
    config: &PreloadedBlockchainConfig,
    params: &ExecuteParams,
) -> Result<ExecutedTransaction> {
    tracing::trace!(
        target: tracing_targets::EXEC_MANAGER,
        account_addr = %account_stuff.account_addr,
        kind = ?tick_tock,
        "executing ticktock",
    );

    let _histogram = HistogramGuard::begin("tycho_collator_execute_ticktock_time");

    let shard_account = &mut account_stuff.shard_account;

    // NOTE: Failed (without tx) ticktock execution is considered as a fatal error
    let (
        total_fees,
        ExecutorOutput {
            account,
            transaction,
        },
    ) = TickTockTransactionExecutor::new(tick_tock).execute_with_libs_and_params(
        None,
        shard_account,
        min_lt,
        params,
        config,
    )?;

    let tx_lt = shard_account.last_trans_lt;
    account_stuff.apply_transaction(tx_lt, total_fees, account, &transaction);

    Ok(transaction)
}
