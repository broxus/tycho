use std::cmp;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use ton_executor::{
    ExecuteParams, ExecutorOutput, OrdinaryTransactionExecutor, PreloadedBlockchainConfig,
    TickTockTransactionExecutor, TransactionExecutor,
};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run_fifo;
use tycho_util::FastHashMap;

use super::types::ParsedMessage;
use crate::collator::types::{AccountId, ShardAccountStuff};
use crate::tracing_targets;

/// Execution manager
pub(super) struct ExecutionManager {
    // this time is used if account's lt is smaller
    min_next_lt: u64,
    /// blockchain config
    config: Arc<PreloadedBlockchainConfig>,
    /// vm execution params related to current block
    params: Arc<ExecuteParams>,
    /// messages groups
    message_groups: MessageGroups,
    /// group limit
    group_limit: usize,
    /// group vertical size
    group_vert_size: usize,
    accounts_cache: AccountsCache,
}

type MessageGroups = FastHashMap<u32, MessageGroup>;
type MessageGroup = FastHashMap<HashBytes, Vec<Box<ParsedMessage>>>;

impl ExecutionManager {
    /// constructor
    pub fn new(
        min_next_lt: u64,
        config: Arc<PreloadedBlockchainConfig>,
        params: Arc<ExecuteParams>,
        group_limit: usize,
        group_vert_size: usize,
        shard_accounts: ShardAccounts,
    ) -> Self {
        Self {
            min_next_lt,
            config,
            params,
            group_limit,
            group_vert_size,
            message_groups: FastHashMap::default(),
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

    /// Set messages that will be executed
    pub fn set_msgs_for_execution(&mut self, msgs: Vec<Box<ParsedMessage>>) {
        let message_count = msgs.len();
        self.message_groups = pre_calculate_groups(msgs, self.group_limit, self.group_vert_size);

        tracing::info!(
            target: tracing_targets::EXEC_MANAGER,
            message_count,
            group_count = self.message_groups.len(),
            group_limit = self.group_limit,
            group_vert_size = self.group_vert_size,
            groups = %DisplayMessageGroups(&self.message_groups),
            "added set of messages for execution, groups pre calculated",
        );
    }

    /// Run one execution tick of parallel transactions
    pub async fn tick(&mut self, offset: u32) -> Result<ExecutedTick> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, offset, "messages set execution tick");

        let (new_offset, group) = match self.message_groups.remove(&offset) {
            Some(group) => (offset + 1, group),
            None => return Ok(ExecutedTick::new_finished(offset)),
        };
        let finished = !self.message_groups.contains_key(&new_offset);

        let group_len = group.len();
        tracing::debug!(target: tracing_targets::EXEC_MANAGER, offset, group_len);

        // TODO check externals is not exist accounts needed ?
        let mut futures = FuturesUnordered::new();
        for (account_id, msgs) in group {
            let shard_account_stuff = self.accounts_cache.create_account_stuff(&account_id)?;
            futures.push(self.execute_messages(shard_account_stuff, msgs));
        }

        let mut items = Vec::with_capacity(group_len);

        while let Some(executed_msgs_result) = futures.next().await {
            let executed = executed_msgs_result?;
            for tx in executed.transactions {
                if matches!(&tx.in_message.info, MsgInfo::ExtIn(_)) {
                    if let Err(e) = &tx.result {
                        tracing::error!(
                            target: tracing_targets::EXEC_MANAGER,
                            account_addr = %executed.account_state.account_addr,
                            message_hash = %tx.in_message.cell.repr_hash(),
                            "failed to execute external message: {e:?}",
                        );
                        continue;
                    }
                }

                let executor_output = tx.result?;

                self.min_next_lt =
                    cmp::max(self.min_next_lt, executor_output.account_last_trans_lt);

                items.push(ExecutedTickItem {
                    account_addr: executed.account_state.account_addr,
                    in_message: tx.in_message,
                    executor_output,
                });
            }

            self.accounts_cache
                .add_account_stuff(executed.account_state);
        }

        Ok(ExecutedTick {
            new_offset,
            finished,
            items,
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

pub struct ExecutedTick {
    pub new_offset: u32,
    pub finished: bool,
    pub items: Vec<ExecutedTickItem>,
}

impl ExecutedTick {
    fn new_finished(new_offset: u32) -> Self {
        Self {
            new_offset,
            finished: true,
            items: Vec::new(),
        }
    }
}

pub struct ExecutedTickItem {
    pub account_addr: AccountId,
    pub in_message: Box<ParsedMessage>,
    pub executor_output: ExecutorOutput,
}

pub struct ExecutedTransactions {
    pub account_state: Box<ShardAccountStuff>,
    pub transactions: Vec<ExecutedOrdinaryTransaction>,
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

    if let Ok((total_fees, executor_output)) = &result {
        // TODO replace with batch set
        let tx_lt = shard_account.last_trans_lt;
        account_stuff.add_transaction(tx_lt, total_fees, &executor_output.transaction)?;
    }

    Ok(ExecutedOrdinaryTransaction {
        result: result.map(|(_, exec_out)| exec_out),
        in_message,
    })
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
    account_stuff.add_transaction(tx_lt, &total_fees, &executor_output.transaction)?;

    Ok(executor_output)
}

// TODO: Update
/// calculate group
// pub fn _calculate_group(
//     messages_set: &[AsyncMessage],
//     group_limit: u32,
//     offset: u32,
// ) -> (u32, FastHashMap<AccountId, AsyncMessage>) {
//     let mut new_offset = offset;
//     let mut holes_group: FastHashMap<AccountId, u32> = FastHashMap::default();
//     let mut max_account_count = 0;
//     let mut holes_max_count: i32 = 0;
//     let mut holes_count: i32 = 0;
//     let mut group = FastHashMap::default();
//     for (i, msg) in messages_set.iter().enumerate() {
//         let account_id = match msg {
//             AsyncMessage::Ext(MsgInfo::ExtIn(ExtInMsgInfo { ref dst, .. }), _) => {
//                 dst.as_std().map(|a| a.address).unwrap_or_default()
//             }
//             AsyncMessage::Int(MsgInfo::Int(IntMsgInfo { ref dst, .. }), _, _) => {
//                 dst.as_std().map(|a| a.address).unwrap_or_default()
//             }
//             AsyncMessage::NewInt(MsgInfo::Int(IntMsgInfo { ref dst, .. }), _) => {
//                 dst.as_std().map(|a| a.address).unwrap_or_default()
//             }
//             s => {
//                 tracing::error!("wrong async message - {s:?}");
//                 unreachable!()
//             }
//         };
//         if (i as u32) < offset {
//             let count = holes_group.entry(account_id).or_default();
//             *count += 1;
//             if *count > max_account_count {
//                 max_account_count = *count;
//                 holes_max_count = (group_limit * max_account_count) as i32 - offset as i32;
//             }
//         } else if group.len() < group_limit as usize {
//             match holes_group.get_mut(&account_id) {
//                 None => {
//                     if holes_max_count > 0 {
//                         holes_group.insert(account_id, 1);
//                         holes_max_count -= 1;
//                         holes_count += 1;
//                     } else {
//                         // if group have this account we skip it
//                         if let std::collections::hash_map::Entry::Vacant(e) =
//                             group.entry(account_id)
//                         {
//                             e.insert(msg.clone());
//                         } else {
//                             // if the offset was not set previously, and the account is skipped then
//                             // it means that we need to move by current group length
//                             if new_offset == offset {
//                                 new_offset += group.len() as u32 + holes_count as u32;
//                             }
//                         }
//                     }
//                 }
//                 Some(count) => {
//                     if *count != max_account_count && holes_max_count > 0 {
//                         *count += 1;
//                         holes_max_count -= 1;
//                         holes_count += 1;
//                     } else {
//                         // group has this account, but it was not taken on previous runs
//                         if let std::collections::hash_map::Entry::Vacant(e) =
//                             group.entry(account_id)
//                         {
//                             e.insert(msg.clone());
//                         } else {
//                             // if the offset was not set previously, and the account is skipped then
//                             // it means that we need to move by current group length
//                             if new_offset == offset {
//                                 new_offset += group.len() as u32 + holes_count as u32;
//                             }
//                         }
//                     }
//                 }
//             }
//         } else {
//             break;
//         }
//     }
//     // if new offset was not set then it means that we took all group elements and all holes on our way
//     if new_offset == offset {
//         new_offset += group.len() as u32 + holes_count as u32;
//     }
//     (new_offset, group)
// }

/// calculate all groups in advance
pub fn pre_calculate_groups(
    messages_set: Vec<Box<ParsedMessage>>,
    group_limit: usize,
    group_vert_size: usize,
) -> MessageGroups {
    let mut res = MessageGroups::default();
    for msg in messages_set {
        assert_eq!(
            msg.special_origin, None,
            "unexpected special origin in ordinary messages set"
        );

        let account_id = match &msg.info {
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) => {
                dst.as_std().map(|a| a.address).unwrap_or_default()
            }
            MsgInfo::Int(IntMsgInfo { dst, .. }) => {
                dst.as_std().map(|a| a.address).unwrap_or_default()
            }
            MsgInfo::ExtOut(info) => {
                unreachable!("ext out message in ordinary messages set: {info:?}")
            }
        };

        let mut g_idx = 0;
        loop {
            let group_entry = res.entry(g_idx).or_default();

            let group_len = group_entry.len();
            match group_entry.entry(account_id) {
                Entry::Vacant(entry) => {
                    if group_len < group_limit {
                        entry.insert(vec![msg]);
                        break;
                    }

                    g_idx += 1;
                }
                Entry::Occupied(mut entry) => {
                    let msgs = entry.get_mut();
                    if msgs.len() < group_vert_size {
                        msgs.push(msg);
                        break;
                    }

                    g_idx += 1;
                }
            }
        }
    }

    res
}

struct DisplayMessageGroup<'a>(&'a MessageGroup);

impl std::fmt::Debug for DisplayMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut l = f.debug_list();
        for messages in self.0.values() {
            l.entry(&messages.len());
        }
        l.finish()
    }
}

struct DisplayMessageGroups<'a>(&'a MessageGroups);

impl std::fmt::Debug for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayMessageGroups<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in self.0.iter() {
            m.entry(k, &DisplayMessageGroup(v));
        }
        m.finish()
    }
}
