use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;

use anyhow::Result;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::dict::Dict;
use everscale_types::models::{
    BlockchainConfig, CurrencyCollection, ExtInMsgInfo, HashUpdate, IntMsgInfo, Lazy, LibDescr,
    MsgInfo, OptionalAccount, ShardAccount, ShardAccounts, TickTock, Transaction,
};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use ton_executor::blockchain_config::PreloadedBlockchainConfig;
use ton_executor::{
    ExecuteParams, OrdinaryTransactionExecutor, TickTockTransactionExecutor, TransactionExecutor,
};
use tycho_util::FastHashMap;

use super::types::{AsyncMessage, ExecutedMessage};
use crate::collator::types::{AccountId, ShardAccountStuff};
use crate::tracing_targets;

static EMPTY_SHARD_ACCOUNT: OnceLock<ShardAccount> = OnceLock::new();

/// Execution manager
pub(super) struct ExecutionManager {
    /// libraries
    pub libraries: Dict<HashBytes, LibDescr>,
    /// generated unix time
    gen_utime: u32,
    // block's start logical time
    start_lt: u64,
    // this time is used if account's lt is smaller
    pub min_next_lt: u64,
    // block random seed
    seed_block: HashBytes,
    /// blockchain config
    config: BlockchainConfig,
    /// total transaction duration
    total_trans_duration: u64,
    /// block version
    block_version: u32,
    /// messages set
    messages_set: Vec<AsyncMessage>,
    /// messages groups
    // messages_groups: Option<HashMap<u32, HashMap<HashBytes, AsyncMessage>>>,
    /// group limit
    group_limit: u32,
    /// shard accounts
    pub shard_accounts: ShardAccounts,
    /// changed accounts
    pub changed_accounts: FastHashMap<AccountId, ShardAccountStuff>,
}

impl ExecutionManager {
    /// constructor
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gen_utime: u32,
        start_lt: u64,
        min_next_lt: u64,
        seed_block: HashBytes,
        libraries: Dict<HashBytes, LibDescr>,
        config: BlockchainConfig,
        block_version: u32,
        group_limit: u32,
        shard_accounts: ShardAccounts,
    ) -> Self {
        Self {
            libraries,
            gen_utime,
            start_lt,
            min_next_lt,
            seed_block,
            config,
            block_version,
            group_limit,
            total_trans_duration: 0,
            messages_set: Vec::new(),
            // messages_groups: None,
            shard_accounts,
            changed_accounts: FastHashMap::default(),
        }
    }

    /// Set messages that will be executed
    pub fn set_msgs_for_execution(&mut self, msgs: Vec<AsyncMessage>) {
        tracing::debug!(target: tracing_targets::EXEC_MANAGER, "adding set of {} messages for execution", msgs.len());
        let _ = std::mem::replace(&mut self.messages_set, msgs);
        // self.messages_groups = Some(pre_calculate_groups(&self.messages_set, self.group_limit));
    }

    /// Run one execution tick of parallel transactions
    pub async fn execute_tick(
        &mut self,
        offset: u32,
    ) -> Result<(
        u32,
        Vec<(
            AccountId,
            AsyncMessage,
            Box<(CurrencyCollection, Lazy<Transaction>)>,
        )>,
        bool,
    )> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "messages set execution tick with offset {}", offset);

        let (new_offset, group) = calculate_group(&self.messages_set, self.group_limit, offset);
        // let messages_groups = self.messages_groups.as_ref().unwrap();
        // let (new_offset, group) = match messages_groups.get(&offset) {
        //     Some(g) => (offset + 1, g.clone()), // TODO: need to optimize without clone()
        //     None => return Ok((offset, vec![], true)),
        // };
        let finished = new_offset == self.messages_set.len() as u32;

        tracing::debug!(target: tracing_targets::EXEC_MANAGER, "offset {} group len: {}", offset, group.len());

        let mut futures: FuturesUnordered<_> = Default::default();

        // TODO check externals is not exist accounts needed ?
        for (account_id, msg) in group {
            let shard_account_stuff = self.get_shard_account_stuff(account_id)?;
            futures.push(self.execute_message(account_id, msg.clone(), shard_account_stuff));
        }

        let mut executed_messages = vec![];
        let mut min_next_lt = self.min_next_lt;
        while let Some(executed_message_result) = futures.next().await {
            let executed_message = executed_message_result?;
            let ExecutedMessage {
                transaction_result,
                in_message,
                updated_shard_account_stuff,
                ..
            } = &executed_message;
            if let AsyncMessage::Ext(_, _) = &in_message {
                if let Err(ref e) = transaction_result {
                    tracing::error!(target: tracing_targets::EXEC_MANAGER,
                        "failed to execute external transaction: in_msg: {:?}\nerror: {:?}",
                        in_message, e,
                    );
                    continue;
                }
            }
            min_next_lt = cmp::max(
                min_next_lt,
                updated_shard_account_stuff.shard_account.last_trans_lt + 1,
            );
            executed_messages.push(executed_message);
        }

        drop(futures);

        self.min_next_lt = min_next_lt;

        let mut result = vec![];
        for ExecutedMessage {
            transaction_result,
            in_message,
            updated_shard_account_stuff,
            transaction_duration,
        } in executed_messages
        {
            self.total_trans_duration += transaction_duration;
            result.push((
                updated_shard_account_stuff.account_addr,
                in_message,
                transaction_result?,
            ));
            self.update_shard_account_stuff_cache(
                updated_shard_account_stuff.account_addr,
                updated_shard_account_stuff,
            )?;
        }

        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "tick executed {}ms;", self.total_trans_duration);

        Ok((new_offset, result, finished))
    }

    pub fn get_shard_account_stuff(&self, account_id: AccountId) -> Result<ShardAccountStuff> {
        let shard_account_stuff = if let Some(a) = self.changed_accounts.get(&account_id) {
            a.clone()
        } else if let Ok(Some((_depth, shard_account))) = self.shard_accounts.get(account_id) {
            ShardAccountStuff::new(account_id, shard_account.clone())?
        } else {
            let shard_account = EMPTY_SHARD_ACCOUNT
                .get_or_init(|| ShardAccount {
                    account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
                    last_trans_hash: Default::default(),
                    last_trans_lt: 0,
                })
                .clone();
            ShardAccountStuff::new(account_id, shard_account)?
        };
        Ok(shard_account_stuff)
    }

    /// execute message
    pub async fn execute_message(
        &self,
        account_id: AccountId,
        new_msg: AsyncMessage,
        mut shard_account_stuff: ShardAccountStuff,
    ) -> Result<ExecutedMessage> {
        tracing::trace!(
            target: tracing_targets::EXEC_MANAGER,
            "executing {} message for account {}",
            match &new_msg {
                AsyncMessage::Recover(_) => "Recover",
                AsyncMessage::Mint(_) => "Mint",
                AsyncMessage::Ext(_, _) => "Ext",
                AsyncMessage::Int(_, _, _) => "Int",
                AsyncMessage::NewInt(_, _) => "NewInt",
                AsyncMessage::TickTock(TickTock::Tick) => "Tick",
                AsyncMessage::TickTock(TickTock::Tock) => "Tock",
            },
            account_id,
        );
        let now = std::time::Instant::now();
        let (config, params) = self.get_execute_params()?;
        let (transaction_result, in_message, updated_shard_account_stuff) =
            tokio::task::spawn_blocking(move || {
                let shard_account = &mut shard_account_stuff.shard_account;
                let mut transaction = match &new_msg {
                    AsyncMessage::Recover(new_msg_cell)
                    | AsyncMessage::Mint(new_msg_cell)
                    | AsyncMessage::Ext(_, new_msg_cell)
                    | AsyncMessage::Int(_, new_msg_cell, _)
                    | AsyncMessage::NewInt(_, new_msg_cell) => {
                        execute_ordinary_message(new_msg_cell, shard_account, params, &config)
                    }
                    AsyncMessage::TickTock(ticktock_) => {
                        execute_ticktock_message(*ticktock_, shard_account, params, &config)
                    }
                };

                if let Ok(transaction) = transaction.as_mut() {
                    // TODO replace with batch set
                    shard_account_stuff.add_transaction(&transaction.0, transaction.1.clone())?;
                }
                Ok((transaction, new_msg, shard_account_stuff))
                    as Result<(
                        Result<Box<(CurrencyCollection, Lazy<Transaction>)>>,
                        AsyncMessage,
                        ShardAccountStuff,
                    )>
            })
            .await??;
        let transaction_duration = now.elapsed().as_millis() as u64;
        Ok(ExecutedMessage {
            transaction_result,
            updated_shard_account_stuff,
            in_message,
            transaction_duration,
        })
    }

    fn update_shard_account_stuff_cache(
        &mut self,
        account_id: AccountId,
        mut account_stuff: ShardAccountStuff,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "updating shard account {}", account_id);
        tracing::debug!(target: tracing_targets::EXEC_MANAGER, "updated account {account_id} balance: {}",
            account_stuff.shard_account.account.load()?.balance().tokens,
        );
        let binding = &account_stuff.shard_account.account;
        let account_root = binding.inner();
        let new_state = account_root.repr_hash();
        let old_state = account_stuff.state_update.load()?.old;
        account_stuff.state_update = Lazy::new(&HashUpdate {
            old: old_state,
            new: *new_state,
        })?;

        self.changed_accounts.insert(account_id, account_stuff);
        Ok(())
    }

    /// execute special transaction
    pub async fn execute_special_transaction(
        &mut self,
        account_id: AccountId,
        msg: AsyncMessage,
        shard_account_stuff: ShardAccountStuff,
    ) -> Result<(AsyncMessage, Box<(CurrencyCollection, Lazy<Transaction>)>)> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "execute_special_transaction()");
        let ExecutedMessage {
            transaction_result,
            updated_shard_account_stuff,
            in_message,
            ..
        } = self
            .execute_message(account_id, msg, shard_account_stuff)
            .await?;
        self.min_next_lt = cmp::max(
            self.min_next_lt,
            updated_shard_account_stuff.shard_account.last_trans_lt + 1,
        );
        self.update_shard_account_stuff_cache(account_id, updated_shard_account_stuff)?;
        Ok((in_message, transaction_result?))
    }

    fn get_execute_params(&self) -> Result<(PreloadedBlockchainConfig, ExecuteParams)> {
        let state_libs = self.libraries.clone();
        let block_unixtime = self.gen_utime;
        let block_lt = self.start_lt;
        let seed_block = self.seed_block;
        let block_version = self.block_version;
        let min_lt = self.min_next_lt;
        let config = PreloadedBlockchainConfig::with_config(self.config.clone(), 0)?; // TODO: fix global id
        let params = ExecuteParams {
            state_libs,
            block_unixtime,
            block_lt,
            min_lt,
            seed_block,
            block_version,
            ..ExecuteParams::default()
        };
        Ok((config, params))
    }
}

/// execute ordinary message
fn execute_ordinary_message(
    new_msg_cell: &Cell,
    shard_account: &mut ShardAccount,
    params: ExecuteParams,
    config: &PreloadedBlockchainConfig,
) -> Result<Box<(CurrencyCollection, Lazy<Transaction>)>> {
    let executor = OrdinaryTransactionExecutor::new();
    executor
        .execute_with_libs_and_params(Some(new_msg_cell), shard_account, &params, config)
        .map(Box::new)
}

/// execute tick tock message
fn execute_ticktock_message(
    tick_tock: TickTock,
    shard_account: &mut ShardAccount,
    params: ExecuteParams,
    config: &PreloadedBlockchainConfig,
) -> Result<Box<(CurrencyCollection, Lazy<Transaction>)>> {
    let executor = TickTockTransactionExecutor::new(tick_tock);
    executor
        .execute_with_libs_and_params(None, shard_account, &params, config)
        .map(Box::new)
}

/// calculate group

pub fn calculate_group(
    messages_set: &[AsyncMessage],
    group_limit: u32,
    offset: u32,
) -> (u32, HashMap<AccountId, AsyncMessage>) {
    let mut new_offset = offset;
    // last group number of account
    let mut accounts_last_group: HashMap<AccountId, u32> = HashMap::new();
    // remaining accounts, that will be ignored, because they have been taken on previous step
    let mut remaining_ignored_accounts_count: i32 = 0;
    // ignored after offset accounts count
    let mut ignored_accounts_count: i32 = 0;
    // last group number calculated from offset
    let mut last_group_num = 0;
    // groups that have not enough elements to be filled to group limit
    let mut unfilled_groups: VecDeque<(u32, u32)> = VecDeque::new();

    let mut result = HashMap::new();
    'messages: for (i, msg) in messages_set.iter().enumerate() {
        let account_id = match msg {
            AsyncMessage::Ext(MsgInfo::ExtIn(ExtInMsgInfo { ref dst, .. }), _) => {
                dst.as_std().map(|a| a.address).unwrap_or_default()
            }
            AsyncMessage::Int(MsgInfo::Int(IntMsgInfo { ref dst, .. }), _, _) => {
                dst.as_std().map(|a| a.address).unwrap_or_default()
            }
            AsyncMessage::NewInt(MsgInfo::Int(IntMsgInfo { ref dst, .. }), _) => {
                dst.as_std().map(|a| a.address).unwrap_or_default()
            }
            s => {
                tracing::error!("wrong async message - {s:?}");
                unreachable!()
            }
        };
        // calculation of helping elements
        if (i as u32) < offset {
            // receiving last group number containing current account
            let account_last_group_num = match accounts_last_group.entry(account_id) {
                Entry::Vacant(last_group) => {
                    last_group.insert(last_group_num);
                    last_group_num
                }
                Entry::Occupied(mut last_group) => {
                    let last_group_element = *last_group.get();
                    if last_group_element >= last_group_num {
                        *last_group.get_mut() = last_group_element + 1;
                        last_group_element + 1
                    } else {
                        *last_group.get_mut() = last_group_num;
                        last_group_num
                    }
                }
            };

            // filling group by number with new element, if it's equal to limit, removing it from further equations
            match unfilled_groups
                .iter_mut()
                .find(|(num, _)| *num == account_last_group_num)
            {
                Some((_, account_last_group_len)) => {
                    *account_last_group_len += 1;
                    // switching last group number
                    if *account_last_group_len == group_limit {
                        unfilled_groups.pop_front();
                        last_group_num += 1;
                    }
                }
                None => {
                    unfilled_groups.push_back((account_last_group_num, 1));
                }
            }
        } else {
            // start of result collecting
            if (i as u32) == offset {
                // finding last unfilled by previous steps group number, the new one will be next
                if let Some(last_unfilled_group_num) = unfilled_groups.back().map(|(num, _)| *num) {
                    last_group_num = last_unfilled_group_num + 1;
                    remaining_ignored_accounts_count =
                        (group_limit * last_group_num) as i32 - offset as i32;
                }
            }
            if result.len() < group_limit as usize {
                match accounts_last_group.get_mut(&account_id) {
                    // it is the first time this account is being performed
                    None => {
                        // if it was taken by previous groups, ignore it
                        if remaining_ignored_accounts_count > 0 {
                            remaining_ignored_accounts_count -= 1;
                            ignored_accounts_count += 1;

                            // filling calculated unfilled groups, starting with the minimum one
                            if let Some((first_unfilled_group_num, first_unfilled_group_len)) =
                                unfilled_groups.front_mut()
                            {
                                accounts_last_group.insert(account_id, *first_unfilled_group_num);
                                *first_unfilled_group_len += 1;
                                if *first_unfilled_group_len == group_limit {
                                    unfilled_groups.pop_front();
                                }
                            } else {
                                // or if there were no unfilled groups, we can be sure that it was taken by previous group
                                accounts_last_group.insert(account_id, last_group_num - 1);
                            }

                            // and continue to the next element
                            continue 'messages;
                        }

                        if let Entry::Vacant(e) = result.entry(account_id) {
                            e.insert(msg.clone());
                        } else {
                            // if the offset was not set previously, and the account is skipped then
                            // it means that we need to move by current result length and all ignored on our way
                            if new_offset == offset {
                                new_offset += result.len() as u32 + ignored_accounts_count as u32;
                            }
                        }
                    }
                    // account was previously included in some groups
                    Some(account_last_group) => {
                        // if there is place in next unfilled groups, then it was taken by it
                        if remaining_ignored_accounts_count > 0 {
                            for (unfilled_group_num, unfilled_group_len) in unfilled_groups
                                .iter_mut()
                                .filter(|(group_num, _)| *group_num >= *account_last_group + 1)
                            {
                                // if we must ignore it, then we skip it, changing the last accounts group number
                                remaining_ignored_accounts_count -= 1;
                                ignored_accounts_count += 1;
                                *account_last_group = *unfilled_group_num;
                                *unfilled_group_len += 1;
                                if *unfilled_group_len == group_limit {
                                    unfilled_groups.pop_front();
                                }
                                // and continue to the next element
                                continue 'messages;
                            }
                        }

                        if let Entry::Vacant(e) = result.entry(account_id) {
                            e.insert(msg.clone());
                        } else {
                            // if the offset was not set previously, and the account is skipped then
                            // it means that we need to move by current result length and all ignored on our way
                            if new_offset == offset {
                                new_offset += result.len() as u32 + ignored_accounts_count as u32;
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }
    }
    // if new offset was not set then it means that we took all result elements and all ignored on our way
    if new_offset == offset {
        new_offset += result.len() as u32 + ignored_accounts_count as u32;
    }
    (new_offset, result)
}

// /// calculate all groups in advance
// pub fn pre_calculate_groups(
//     messages_set: &[AsyncMessage],
//     group_limit: u32,
// ) -> HashMap<u32, HashMap<AccountId, AsyncMessage>> {
//     let mut res: HashMap<u32, HashMap<AccountId, AsyncMessage>> = HashMap::new();
//     for msg in messages_set {
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
//
//         let mut g_idx = 0;
//         let mut group_entry;
//         loop {
//             group_entry = res.entry(g_idx).or_default();
//             if group_entry.len() == group_limit as usize {
//                 g_idx += 1;
//                 continue;
//             }
//             let account_entry = group_entry.entry(account_id);
//             match account_entry {
//                 Entry::Vacant(entry) => {
//                     entry.insert(msg.clone());
//                     break;
//                 }
//                 Entry::Occupied(_entry) => {
//                     g_idx += 1;
//                 }
//             }
//         }
//     }
//     res
// }
