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
use tycho_vm::{SafeRc, SmcInfoTonV6, Tuple};

use super::messages_buffer::MessageGroup;
use super::types::{AccountId, ParsedMessage, ShardAccountStuff};
use crate::tracing_targets;

#[cfg(test)]
#[path = "tests/execution_manager_tests.rs"]
pub(super) mod tests;

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

        let accounts_cache = Arc::new(&self.accounts_cache);
        let result = msg_group
            .into_par_iter()
            .map_init(
                move || {
                    // TEMP: There will be a per-thread executor state.
                    let unpacked = SmcInfoTonV6::unpack_config(
                        &config.raw_config().params,
                        params.block_unixtime,
                    )
                    .ok();

                    (
                        config.clone(),
                        params.clone(),
                        accounts_cache.clone(),
                        unpacked,
                    )
                },
                |(config, params, accounts_cache, unpacked), (account_id, msgs)| {
                    Self::execute_subgroup(
                        account_id,
                        msgs,
                        accounts_cache,
                        min_next_lt,
                        config,
                        unpacked.clone().unwrap_or_default(),
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
        unpacked_config: SafeRc<Tuple>,
        params: &Arc<ExecuteParams>,
    ) -> Result<ExecutedTransactions> {
        let shard_account_stuff = accounts_cache.get_account_stuff(&account_id)?;
        Self::execute_messages(
            shard_account_stuff,
            msgs,
            min_next_lt,
            config,
            unpacked_config,
            params,
        )
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
        unpacked_config: SafeRc<Tuple>,
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
                unpacked_config.clone(),
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

        // TEMP: There will be a per-thread cached state for a new executor.
        let unpacked_config =
            SmcInfoTonV6::unpack_config(&config.raw_config().params, params.block_unixtime)?;

        let (account_stuff, executed) = execute_ordinary_transaction_impl(
            &mut account_stuff,
            in_message,
            min_next_lt,
            &config,
            unpacked_config,
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

        // TEMP: There will be a per-thread cached state for a new executor.
        let unpacked_config =
            SmcInfoTonV6::unpack_config(&config.raw_config().params, params.block_unixtime)?;

        let (account_stuff, executed) = execute_ticktock_transaction(
            &mut account_stuff,
            tick_tock,
            min_next_lt,
            &config,
            unpacked_config,
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
    unpacked_config: SafeRc<Tuple>,
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
        unpacked_config,
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
    unpacked_config: SafeRc<Tuple>,
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
        unpacked_config,
    )?;

    let tx_lt = shard_account.last_trans_lt;
    account_stuff.apply_transaction(tx_lt, total_fees, account, &transaction);

    Ok(transaction)
}
