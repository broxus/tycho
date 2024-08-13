use std::cmp;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::*;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use humantime::format_duration;
use ton_executor::{
    ExecuteParams, ExecutorOutput, OrdinaryTransactionExecutor, PreloadedBlockchainConfig,
    TickTockTransactionExecutor, TransactionExecutor,
};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run_fifo;
use tycho_util::FastHashMap;

use super::mq_iterator_adapter::QueueIteratorAdapter;
use super::types::{
    AccountId, BlockCollationData, Dequeued, DisplayMessageGroup, MessageGroup, MessageGroups,
    ParsedMessage, ShardAccountStuff, WorkingState,
};
use super::CollatorStdImpl;
use crate::internal_queue::types::{InternalMessageKey, QueueDiff};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::{InternalsProcessedUptoStuff, ProcessedUptoInfoStuff};

/// Execution manager
pub(super) struct ExecutionManager {
    shard_id: ShardIdent,
    /// messages groups
    message_groups: MessageGroups,
    /// max number of messages that could be loaded into runtime
    messages_buffer_limit: usize,
    /// flag indicates that should process ext messages
    process_ext_messages: bool,
    /// we started ext messages reading before and can continue reading from read_to
    ext_messages_reader_started: bool,
    /// flag indicates that should process new messages
    process_new_messages: bool,
    /// internal mq adapter
    mq_adapter: Arc<dyn MessageQueueAdapter>,
    /// current read positions of internals mq iterator
    /// when it is not finished
    current_iterator_positions: FastHashMap<ShardIdent, InternalMessageKey>,

    /// sum total time of reading existing internal messages
    read_existing_messages_total_elapsed: Duration,
    /// sum total time of reading new internal messages
    read_new_messages_total_elapsed: Duration,
    /// sum total time of reading external messages
    read_ext_messages_total_elapsed: Duration,
    /// sum total time of adding messages to groups
    add_to_message_groups_total_elapsed: Duration,
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
}

impl ExecutionManager {
    /// constructor
    pub fn new(
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter>,
        messages_buffer_limit: usize,
        group_limit: usize,
        group_vert_size: usize,
    ) -> Self {
        metrics::gauge!("tycho_do_collate_msgs_exec_params_buffer_limit")
            .set(messages_buffer_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_limit").set(group_limit as f64);
        metrics::gauge!("tycho_do_collate_msgs_exec_params_group_vert_size")
            .set(group_vert_size as f64);

        Self {
            shard_id,
            messages_buffer_limit,
            message_groups: MessageGroups::new(shard_id, group_limit, group_vert_size),
            process_ext_messages: false,
            ext_messages_reader_started: false,
            process_new_messages: false,
            mq_adapter,
            current_iterator_positions: Default::default(),
            read_existing_messages_total_elapsed: Duration::ZERO,
            read_new_messages_total_elapsed: Duration::ZERO,
            read_ext_messages_total_elapsed: Duration::ZERO,
            add_to_message_groups_total_elapsed: Duration::ZERO,
        }
    }

    pub fn get_current_iterator_positions(&self) -> FastHashMap<ShardIdent, InternalMessageKey> {
        self.current_iterator_positions.clone()
    }

    pub fn set_current_iterator_positions(
        &mut self,
        positions: FastHashMap<ShardIdent, InternalMessageKey>,
    ) {
        self.current_iterator_positions = positions;
    }

    pub fn get_message_groups_offset(&self) -> u32 {
        self.message_groups.offset()
    }

    pub fn has_pending_messages_in_buffer(&self) -> bool {
        !self.message_groups.is_empty()
    }

    pub fn create_iterator_adapter(&mut self) -> QueueIteratorAdapter {
        self.process_ext_messages = false;
        self.process_new_messages = false;

        self.read_existing_messages_total_elapsed = Duration::ZERO;
        self.read_new_messages_total_elapsed = Duration::ZERO;
        self.read_ext_messages_total_elapsed = Duration::ZERO;
        self.add_to_message_groups_total_elapsed = Duration::ZERO;

        let current_iterator_positions = std::mem::take(&mut self.current_iterator_positions);
        QueueIteratorAdapter::new(
            self.shard_id,
            self.mq_adapter.clone(),
            Some(current_iterator_positions),
        )
    }

    pub fn release_iterator_adapter(
        &mut self,
        mq_iterator_adapter: QueueIteratorAdapter,
    ) -> Result<(bool, QueueDiff)> {
        let has_pending_messages_in_buffer = self.has_pending_messages_in_buffer();
        let (current_positions, has_pending_internals, diff) =
            mq_iterator_adapter.release(!has_pending_messages_in_buffer)?;
        self.current_iterator_positions = current_positions;

        let has_pending_messages = has_pending_messages_in_buffer || has_pending_internals;

        Ok((has_pending_messages, diff))
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

    #[tracing::instrument(skip_all)]
    pub async fn get_next_message_group(
        &mut self,
        collator: &mut CollatorStdImpl,
        collation_data: &mut BlockCollationData,
        mq_iterator_adapter: &mut QueueIteratorAdapter,
        max_new_message_key_to_current_shard: InternalMessageKey,
        working_state: &WorkingState,
    ) -> Result<Option<MessageGroup>> {
        // messages polling logic differs regarding existing and new messages

        let mut group_opt = None;

        if !self.process_ext_messages && !self.process_new_messages {
            // for existing messages we use ranged iterator and process maximum possible groups in parallel

            // here iterator may not exist (on the first method call during collation)
            // so init iterator for current not fully processed ranges or next available
            if mq_iterator_adapter.iterator_is_none() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "current iterator not exist, \
                    will init iterator for current not fully processed ranges or next available"
                );
                mq_iterator_adapter
                    .try_init_next_range_iterator(&mut collation_data.processed_upto, working_state)
                    .await?;
            }

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
                self.message_groups.add_message(Box::new(ParsedMessage {
                    info: MsgInfo::Int(int_msg.message_with_source.message.info.clone()),
                    dst_in_current_shard: true,
                    cell: int_msg.message_with_source.message.cell.clone(),
                    special_origin: None,
                    dequeued: Some(Dequeued {
                        same_shard: int_msg.message_with_source.shard_id == self.shard_id,
                    }),
                }));
                add_to_groups_elapsed += timer_add_to_groups.elapsed();

                if self.message_groups.messages_count() >= self.messages_buffer_limit {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "message_groups buffer filled on {}/{}, stop reading existing internals",
                        self.message_groups.messages_count(), self.messages_buffer_limit,
                    );
                    break;
                }

                if self.message_groups.first_group_is_full() {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "first message group is full, stop reading existing internals",
                    );
                    break;
                }
            }
            collation_data.read_int_msgs_from_iterator += existing_internals_read_count;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "existing_internals_read_count={}, buffer int={}, ext={}",
                existing_internals_read_count,
                self.message_groups.int_messages_count(), self.message_groups.ext_messages_count(),
            );

            group_opt = self.message_groups.extract_first_group();
            if let Some(first_group) = group_opt.as_ref() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "extracted first message group from message_groups buffer: group {}, buffer int={}, ext={}",
                    DisplayMessageGroup(first_group),
                    self.message_groups.int_messages_count(), self.message_groups.ext_messages_count(),
                );
            }

            self.read_existing_messages_total_elapsed += timer.elapsed();
            self.read_existing_messages_total_elapsed -= add_to_groups_elapsed;
            self.add_to_message_groups_total_elapsed += add_to_groups_elapsed;

            // when message_groups buffer is empty and no more existing internals in current iterator
            // then set all read messages as processed
            // and try to init iterator for the next available ranges
            if self.message_groups.is_empty() && mq_iterator_adapter.no_pending_existing_internals()
            {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "message_groups buffer is empty and there are no pending existing internals, \
                    will try to init iterator for next available ranges"
                );

                // set all read existing internals as processed
                let updated_processed_to =
                    set_int_upto_all_processed(&mut collation_data.processed_upto);

                // commit processed message to iterator
                mq_iterator_adapter
                    .iterator()
                    .commit(updated_processed_to)?;

                self.message_groups.reset();

                // set all read externals as processed
                if let Some(externals) = collation_data.processed_upto.externals.as_mut() {
                    if externals.processed_to != externals.read_to {
                        externals.processed_to = externals.read_to;
                        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
                            collation_data.processed_upto.externals,
                        );
                    }
                }

                let next_range_iterator_initialized = mq_iterator_adapter
                    .try_init_next_range_iterator(&mut collation_data.processed_upto, working_state)
                    .await?;
                if !next_range_iterator_initialized {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "there are no next available ranges for existing internals iterator, \
                        will process externals"
                    );
                    self.process_ext_messages = true;
                }
            }
        }

        if group_opt.is_none() && self.process_ext_messages && !self.process_new_messages {
            let timer = std::time::Instant::now();
            let mut add_to_groups_elapsed = Duration::ZERO;

            let mut externals_read_count = 0;
            loop {
                let ext_msgs = collator.read_next_externals(
                    3,
                    collation_data,
                    self.ext_messages_reader_started,
                )?;
                self.ext_messages_reader_started = true;

                externals_read_count += ext_msgs.len() as u64;

                let timer_add_to_groups = std::time::Instant::now();
                for ext_msg in ext_msgs {
                    self.message_groups.add_message(ext_msg);
                }
                add_to_groups_elapsed += timer_add_to_groups.elapsed();

                if self.message_groups.messages_count() >= self.messages_buffer_limit {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "message_groups buffer filled on {}/{}, stop reading externals",
                        self.message_groups.messages_count(), self.messages_buffer_limit,
                    );
                    break;
                }

                if self.message_groups.first_group_is_full() {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "first message group is full, stop reading externals",
                    );
                    break;
                }

                if !collator.has_pending_externals {
                    break;
                }
            }
            collation_data.read_ext_msgs += externals_read_count;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "externals_read_count={}, buffer int={}, ext={}",
                externals_read_count,
                self.message_groups.int_messages_count(), self.message_groups.ext_messages_count(),
            );

            group_opt = self.message_groups.extract_first_group();
            if let Some(first_group) = group_opt.as_ref() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "extracted first message group from message_groups buffer: group {}, buffer int={}, ext={}",
                    DisplayMessageGroup(first_group),
                    self.message_groups.int_messages_count(), self.message_groups.ext_messages_count(),
                );
            }

            self.read_ext_messages_total_elapsed += timer.elapsed();
            self.read_ext_messages_total_elapsed -= add_to_groups_elapsed;
            self.add_to_message_groups_total_elapsed += add_to_groups_elapsed;

            if self.message_groups.is_empty() && !collator.has_pending_externals {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "message_groups buffer is empty and there are no pending externals, will process new internals"
                );

                // set all read externals as processed
                if let Some(externals) = collation_data.processed_upto.externals.as_mut() {
                    if externals.processed_to != externals.read_to {
                        externals.processed_to = externals.read_to;
                        tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.externals = {:?}",
                            collation_data.processed_upto.externals,
                        );
                    }
                }
                self.message_groups.reset();

                self.process_new_messages = true;
            }
        }

        if group_opt.is_none() && self.process_new_messages {
            // when processing new messages we return group immediately when the next message does not fit it

            // first new messages epoch is from existing internals and externals
            // then we read next epoch of new messages only when the previous epoch processed
            mq_iterator_adapter
                .try_update_new_messages_read_to(max_new_message_key_to_current_shard)?;

            let timer = std::time::Instant::now();
            let mut add_to_groups_elapsed = Duration::ZERO;

            let mut new_internals_read_count = 0;
            while let Some(int_msg) = mq_iterator_adapter.next_new_message()? {
                assert!(int_msg.is_new);

                new_internals_read_count += 1;

                let timer_add_to_groups = std::time::Instant::now();
                self.message_groups.add_message(Box::new(ParsedMessage {
                    info: MsgInfo::Int(int_msg.message_with_source.message.info.clone()),
                    dst_in_current_shard: true,
                    cell: int_msg.message_with_source.message.cell.clone(),
                    special_origin: None,
                    dequeued: None,
                }));
                add_to_groups_elapsed += timer_add_to_groups.elapsed();

                if self.message_groups.messages_count() >= self.messages_buffer_limit {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "message_groups buffer filled on {}/{}, stop reading new internals",
                        self.message_groups.messages_count(), self.messages_buffer_limit,
                    );
                    break;
                }

                if self.message_groups.len() > 1 {
                    tracing::debug!(target: tracing_targets::COLLATOR,
                        "next new message does not fit first group, stop reading new internals",
                    );
                    break;
                }
            }
            collation_data.read_new_msgs_from_iterator += new_internals_read_count;

            tracing::debug!(target: tracing_targets::COLLATOR,
                "new_internals_read_count={}, buffer int={}, ext={}",
                new_internals_read_count,
                self.message_groups.int_messages_count(), self.message_groups.ext_messages_count(),
            );

            // when we have 2 groups, the second one contains only one message
            // that does not fit first group,
            // so append this one message to first group (merge)
            group_opt = self.message_groups.extract_merged_group();
            if let Some(merged_group) = group_opt.as_ref() {
                tracing::debug!(target: tracing_targets::COLLATOR,
                    "extracted merged message group of new messages from message_groups buffer: group {}, buffer int={}, ext={}",
                    DisplayMessageGroup(merged_group),
                    self.message_groups.int_messages_count(), self.message_groups.ext_messages_count(),
                );
            }

            self.read_new_messages_total_elapsed += timer.elapsed();
            self.read_new_messages_total_elapsed -= add_to_groups_elapsed;
            self.add_to_message_groups_total_elapsed += add_to_groups_elapsed;

            // actually, we process all message groups with new messages in one step,
            // so we update internals processed_upto each step
            if self.message_groups.is_empty()
                && self.message_groups.max_message_key() > &InternalMessageKey::default()
            {
                // set_int_upto_all_processed(&mut collation_data.processed_upto)?;
                update_internals_processed_upto(
                    &mut collation_data.processed_upto,
                    self.shard_id,
                    Some(ProcessedUptoUpdate::Force(
                        self.message_groups.max_message_key().clone(),
                    )),
                    Some(ProcessedUptoUpdate::Force(
                        self.message_groups.max_message_key().clone(),
                    )),
                );

                // commit processed message to iterator
                mq_iterator_adapter.iterator().commit(vec![(
                    self.shard_id,
                    self.message_groups.max_message_key().clone(),
                )])?;

                self.message_groups.reset();
            }
        }

        // store actual offset of current interator range
        if collation_data.processed_upto.processed_offset != self.message_groups.offset() {
            collation_data.processed_upto.processed_offset = self.message_groups.offset();
            tracing::debug!(target: tracing_targets::COLLATOR, "updated processed_upto.offset = {}",
                collation_data.processed_upto.processed_offset,
            );
        }

        Ok(group_opt)
    }
}

impl MessagesExecutor {
    pub fn new(
        shard_id: ShardIdent,
        min_next_lt: u64,
        config: Arc<PreloadedBlockchainConfig>,
        params: Arc<ExecuteParams>,
        shard_accounts: ShardAccounts,
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
        }
    }

    pub fn min_next_lt(&self) -> u64 {
        self.min_next_lt
    }

    pub fn executor_params(&self) -> &Arc<ExecuteParams> {
        &self.params
    }

    pub fn into_changed_accounts(self) -> impl ExactSizeIterator<Item = Box<ShardAccountStuff>> {
        self.accounts_cache.items.into_values()
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
    pub async fn execute_group(&mut self, group: MessageGroup) -> Result<ExecutedGroup> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "execute messages group");

        let labels = &[("workchain", self.shard_id.workchain().to_string())];

        let group_horizontal_size = group.len();
        let group_messages_count = group.messages_count();
        let group_mean_vert_size = group_messages_count
            .checked_div(group_horizontal_size)
            .unwrap_or_default();
        let mut group_max_vert_size = 0;

        // TODO check externals is not exist accounts needed ?
        let mut futures = FuturesUnordered::new();
        for (account_id, msgs) in group {
            group_max_vert_size = cmp::max(group_max_vert_size, msgs.len());
            let shard_account_stuff = self.accounts_cache.create_account_stuff(&account_id)?;
            futures.push(self.execute_messages(shard_account_stuff, msgs));
        }

        let mut items = Vec::with_capacity(group_messages_count);
        let mut ext_msgs_error_count = 0;

        let mut max_account_msgs_exec_time = Duration::ZERO;
        let mut total_exec_time = Duration::ZERO;

        while let Some(executed_msgs_result) = futures.next().await {
            let executed = executed_msgs_result?;

            max_account_msgs_exec_time = max_account_msgs_exec_time.max(executed.exec_time);
            total_exec_time += executed.exec_time;

            for tx in executed.transactions {
                if matches!(&tx.in_message.info, MsgInfo::ExtIn(_)) {
                    if let Err(e) = &tx.result {
                        tracing::warn!(
                            target: tracing_targets::EXEC_MANAGER,
                            account_addr = %executed.account_state.account_addr,
                            message_hash = %tx.in_message.cell.repr_hash(),
                            "failed to execute external message: {e:?}",
                        );
                        ext_msgs_error_count += 1;
                        continue;
                    }
                }

                let executor_output = tx.result?;

                self.min_next_lt =
                    cmp::max(self.min_next_lt, executor_output.account_last_trans_lt);

                items.push(ExecutedTickItem {
                    in_message: tx.in_message,
                    executor_output,
                });
            }

            self.accounts_cache
                .add_account_stuff(executed.account_state);
        }

        let mean_account_msgs_exec_time = total_exec_time
            .checked_div(group_horizontal_size as u32)
            .unwrap_or_default();

        tracing::info!(target: tracing_targets::EXEC_MANAGER,
            group_horizontal_size, group_max_vert_size,
            total_exec_time = %format_duration(total_exec_time),
            mean_account_msgs_exec_time = %format_duration(mean_account_msgs_exec_time),
            max_account_msgs_exec_time = %format_duration(max_account_msgs_exec_time),
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
        })
    }

    fn execute_messages(
        &self,
        mut account_state: Box<ShardAccountStuff>,
        msgs: Vec<Box<ParsedMessage>>,
    ) -> impl Future<Output = Result<ExecutedTransactions>> + Send + 'static {
        let min_next_lt = self.min_next_lt;
        let config = self.config.clone();
        let params = self.params.clone();

        rayon_run_fifo(move || {
            let timer = std::time::Instant::now();

            let mut transactions = Vec::with_capacity(msgs.len());

            for msg in msgs {
                transactions.push(execute_ordinary_transaction_impl(
                    &mut account_state,
                    msg,
                    min_next_lt,
                    &config,
                    &params,
                )?);
            }

            Ok(ExecutedTransactions {
                account_state,
                transactions,
                exec_time: timer.elapsed(),
            })
        })
    }

    /// Executes a single ordinary transaction.
    pub async fn execute_ordinary_transaction(
        &mut self,
        mut account_stuff: Box<ShardAccountStuff>,
        in_message: Box<ParsedMessage>,
    ) -> Result<ExecutedOrdinaryTransaction> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "execute ordinary transaction for special message");

        let min_next_lt = self.min_next_lt;
        let config = self.config.clone();
        let params = self.params.clone();

        let (account_stuff, executed) = rayon_run_fifo(move || {
            let executed = execute_ordinary_transaction_impl(
                &mut account_stuff,
                in_message,
                min_next_lt,
                &config,
                &params,
            )?;
            Ok::<_, anyhow::Error>((account_stuff, executed))
        })
        .await?;

        if let Ok(executor_output) = &executed.result {
            self.min_next_lt = cmp::max(min_next_lt, executor_output.account_last_trans_lt);
        }
        self.accounts_cache.add_account_stuff(account_stuff);
        Ok(executed)
    }

    /// Executes a single ticktock transaction.
    pub async fn execute_ticktock_transaction(
        &mut self,
        mut account_stuff: Box<ShardAccountStuff>,
        tick_tock: TickTock,
    ) -> Result<ExecutorOutput> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "execute special transaction");

        let min_next_lt = self.min_next_lt;
        let config = self.config.clone();
        let params = self.params.clone();

        let (account_stuff, executed) = rayon_run_fifo(move || {
            let executed = execute_ticktock_transaction(
                &mut account_stuff,
                tick_tock,
                min_next_lt,
                &config,
                &params,
            )?;
            Ok::<_, anyhow::Error>((account_stuff, executed))
        })
        .await?;

        self.min_next_lt = cmp::max(min_next_lt, executed.account_last_trans_lt);
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

    fn create_account_stuff(&mut self, account_id: &AccountId) -> Result<Box<ShardAccountStuff>> {
        if let Some(account) = self.items.remove(account_id) {
            Ok(account)
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
}

pub struct ExecutedTickItem {
    pub in_message: Box<ParsedMessage>,
    pub executor_output: ExecutorOutput,
}

pub struct ExecutedTransactions {
    pub account_state: Box<ShardAccountStuff>,
    pub transactions: Vec<ExecutedOrdinaryTransaction>,
    pub exec_time: Duration,
}

pub struct ExecutedOrdinaryTransaction {
    pub result: Result<ExecutorOutput>,
    pub in_message: Box<ParsedMessage>,
}

fn execute_ordinary_transaction_impl(
    account_stuff: &mut ShardAccountStuff,
    in_message: Box<ParsedMessage>,
    min_lt: u64,
    config: &PreloadedBlockchainConfig,
    params: &ExecuteParams,
) -> Result<ExecutedOrdinaryTransaction> {
    tracing::debug!(
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
        Ok((total_fees, executor_output)) => {
            let tx_lt = shard_account.last_trans_lt;
            account_stuff.add_transaction(tx_lt, total_fees, executor_output.transaction.clone());
            Ok(executor_output)
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
) -> Result<ExecutorOutput> {
    tracing::trace!(
        target: tracing_targets::EXEC_MANAGER,
        account_addr = %account_stuff.account_addr,
        kind = ?tick_tock,
        "executing ticktock",
    );

    let _histogram = HistogramGuard::begin("tycho_collator_execute_ticktock_time");

    let shard_account = &mut account_stuff.shard_account;

    // NOTE: Failed (without tx) ticktock execution is considered as a fatal error
    let (total_fees, executor_output) = TickTockTransactionExecutor::new(tick_tock)
        .execute_with_libs_and_params(None, shard_account, min_lt, params, config)?;

    // TODO replace with batch set
    let tx_lt = shard_account.last_trans_lt;
    account_stuff.add_transaction(tx_lt, total_fees, executor_output.transaction.clone());

    Ok(executor_output)
}

#[derive(Clone)]
pub(super) enum ProcessedUptoUpdate {
    Force(InternalMessageKey),
    IfHigher(InternalMessageKey),
}

pub(super) fn set_int_upto_all_processed(
    processed_upto: &mut ProcessedUptoInfoStuff,
) -> Vec<(ShardIdent, InternalMessageKey)> {
    let mut updated_processed_to = vec![];
    for (shard_id, int_upto) in processed_upto.internals.iter_mut() {
        int_upto.processed_to_msg = int_upto.read_to_msg.clone();

        tracing::debug!(target: tracing_targets::COLLATOR,
            "updated processed_upto.internals for shard {}: {:?}",
            shard_id, int_upto,
        );

        updated_processed_to.push((*shard_id, int_upto.processed_to_msg.clone()));
    }
    updated_processed_to
}

pub(super) fn update_internals_processed_upto(
    processed_upto: &mut ProcessedUptoInfoStuff,
    shard_id: ShardIdent,
    processed_to_opt: Option<ProcessedUptoUpdate>,
    read_to_opt: Option<ProcessedUptoUpdate>,
) -> bool {
    use ProcessedUptoUpdate::{Force, IfHigher};

    fn get_new_to_key<F>(
        to_key_update_opt: Option<ProcessedUptoUpdate>,
        get_current: F,
    ) -> Option<InternalMessageKey>
    where
        F: FnOnce() -> InternalMessageKey,
    {
        if let Some(to_key_update) = to_key_update_opt {
            match to_key_update {
                Force(to_key) => Some(to_key),
                IfHigher(to_key) => {
                    let current_to_key = get_current();
                    if to_key > current_to_key {
                        Some(to_key)
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }

    if processed_to_opt.is_none() && read_to_opt.is_none() {
        return false;
    }

    let new_int_processed_upto_opt = if let Some(current) = processed_upto.internals.get(&shard_id)
    {
        let new_processed_to_opt =
            get_new_to_key(processed_to_opt, || current.processed_to_msg.clone());
        let new_read_to_opt = get_new_to_key(read_to_opt, || current.read_to_msg.clone());
        if new_processed_to_opt.is_none() && new_read_to_opt.is_none() {
            None
        } else {
            Some(InternalsProcessedUptoStuff {
                processed_to_msg: new_processed_to_opt.unwrap_or(current.processed_to_msg.clone()),
                read_to_msg: new_read_to_opt.unwrap_or(current.read_to_msg.clone()),
            })
        }
    } else {
        Some(InternalsProcessedUptoStuff {
            processed_to_msg: match processed_to_opt {
                Some(Force(to_key) | IfHigher(to_key)) => to_key,
                _ => InternalMessageKey::with_lt_and_min_hash(0),
            },
            read_to_msg: match read_to_opt {
                Some(Force(to_key) | IfHigher(to_key)) => to_key,
                _ => InternalMessageKey::with_lt_and_max_hash(0),
            },
        })
    };
    if let Some(new_int_processed_upto) = new_int_processed_upto_opt {
        tracing::debug!(target: tracing_targets::COLLATOR,
            "updated processed_upto.internals for shard {}: {:?}",
            shard_id, new_int_processed_upto,
        );
        processed_upto
            .internals
            .insert(shard_id, new_int_processed_upto);
        true
    } else {
        false
    }
}
