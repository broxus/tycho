use std::cmp;
use std::collections::hash_map::Entry;
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
use tycho_util::sync::rayon_run;
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
    /// messages groups
    messages_groups: FastHashMap<u32, FastHashMap<HashBytes, Vec<AsyncMessage>>>,
    /// group limit
    group_limit: usize,
    /// group vertical size
    group_vert_size: usize,
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
        group_limit: usize,
        group_vert_size: usize,
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
            group_vert_size,
            total_trans_duration: 0,
            messages_groups: FastHashMap::default(),
            shard_accounts,
            changed_accounts: FastHashMap::default(),
        }
    }

    /// Set messages that will be executed
    pub fn set_msgs_for_execution(&mut self, msgs: Vec<AsyncMessage>) {
        let msgs_set_size = msgs.len();
        self.messages_groups = pre_calculate_groups(msgs, self.group_limit, self.group_vert_size);
        tracing::info!(target: tracing_targets::EXEC_MANAGER,
            msgs_set_size, groups_calculated = self.messages_groups.len(),
            group_limit = self.group_limit, group_vert_size = self.group_vert_size,
            "added set of messages for execution, calculated groups: {:?}",
            self
            .messages_groups
            .iter()
            .map(|(k, g)| {
                format!(
                    "{}: {:?}",
                    k,
                    g.values().map(|v| v.len()).collect::<Vec<_>>().as_slice()
                )
            })
            .collect::<Vec<_>>()
            .as_slice(),
        );
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

        let (new_offset, group) = match self.messages_groups.remove(&offset) {
            Some(group) => (offset + 1, group),
            None => return Ok((offset, vec![], true)),
        };
        let finished = !self.messages_groups.contains_key(&new_offset);

        tracing::debug!(target: tracing_targets::EXEC_MANAGER, "offset {} group len: {}", offset, group.len());

        let mut futures: FuturesUnordered<_> = Default::default();

        // TODO check externals is not exist accounts needed ?
        for (account_id, msgs) in group {
            let shard_account_stuff = self.get_shard_account_stuff(account_id)?;
            futures.push(self.execute_messages(account_id, msgs, shard_account_stuff));
        }

        let mut result = vec![];
        let mut updated_shard_account_stuffs = vec![];
        let mut total_tick_trans_duration = 0;
        let mut min_next_lt = self.min_next_lt;
        while let Some(executed_msgs_result) = futures.next().await {
            let (executed_msgs, updated_shard_account_stuff) = executed_msgs_result?;
            for executed_msg in executed_msgs {
                let ExecutedMessage {
                    transaction_result,
                    in_message,
                    transaction_duration,
                } = executed_msg;
                if let AsyncMessage::Ext(_, _) = &in_message {
                    if let Err(ref e) = transaction_result {
                        tracing::error!(target: tracing_targets::EXEC_MANAGER,
                            "failed to execute external transaction: in_msg: {:?}\nerror: {:?}",
                            in_message, e,
                        );
                        continue;
                    }
                }
                total_tick_trans_duration += transaction_duration;
                result.push((
                    updated_shard_account_stuff.account_addr,
                    in_message,
                    transaction_result?,
                ));
            }
            min_next_lt = cmp::max(
                min_next_lt,
                updated_shard_account_stuff.shard_account.last_trans_lt + 1,
            );
            updated_shard_account_stuffs.push(updated_shard_account_stuff);
        }

        drop(futures);

        for shard_account_stuff in updated_shard_account_stuffs {
            self.update_shard_account_stuff_cache(
                shard_account_stuff.account_addr,
                shard_account_stuff,
            )?;
        }

        self.total_trans_duration += total_tick_trans_duration;
        self.min_next_lt = min_next_lt;

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

    pub async fn execute_messages(
        &self,
        account_id: AccountId,
        msgs: Vec<AsyncMessage>,
        mut shard_account_stuff: ShardAccountStuff,
    ) -> Result<(Vec<ExecutedMessage>, ShardAccountStuff)> {
        let mut results = vec![];
        let (config, params) = self.get_execute_params()?;

        rayon_run(move || {
            for msg in msgs {
                let (executed_msg, updated_shard_account_stuff) = Self::execute_one_message(
                    account_id,
                    msg,
                    shard_account_stuff,
                    &config,
                    &params,
                )?;
                results.push(executed_msg);
                shard_account_stuff = updated_shard_account_stuff;
            }
            Ok((results, shard_account_stuff))
        })
        .await
    }

    /// execute message
    pub fn execute_one_message(
        account_id: AccountId,
        new_msg: AsyncMessage,
        mut shard_account_stuff: ShardAccountStuff,
        config: &PreloadedBlockchainConfig,
        params: &ExecuteParams,
    ) -> Result<(ExecutedMessage, ShardAccountStuff)> {
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
        let shard_account = &mut shard_account_stuff.shard_account;
        let mut transaction = match &new_msg {
            AsyncMessage::Recover(new_msg_cell)
            | AsyncMessage::Mint(new_msg_cell)
            | AsyncMessage::Ext(_, new_msg_cell)
            | AsyncMessage::Int(_, new_msg_cell, _)
            | AsyncMessage::NewInt(_, new_msg_cell) => {
                execute_ordinary_message(new_msg_cell, shard_account, params, config)
            }
            AsyncMessage::TickTock(ticktock_) => {
                execute_ticktock_message(*ticktock_, shard_account, params, config)
            }
        };

        if let Ok(transaction) = transaction.as_mut() {
            // TODO replace with batch set
            shard_account_stuff.add_transaction(&transaction.0, transaction.1.clone())?;
        }

        let elapsed = now.elapsed();
        metrics::histogram!("tycho_message_execution_time").record(elapsed);

        let transaction_duration = elapsed.as_millis() as u64;
        Ok((
            ExecutedMessage {
                transaction_result: transaction,
                in_message: new_msg,
                transaction_duration,
            },
            shard_account_stuff,
        ))
    }

    fn update_shard_account_stuff_cache(
        &mut self,
        account_id: AccountId,
        mut account_stuff: ShardAccountStuff,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "updating shard account {}", account_id);
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "updated account {} balance: {}",
            account_id, account_stuff.shard_account.account.load()?.balance().tokens,
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
        let (config, params) = self.get_execute_params()?;
        let (
            ExecutedMessage {
                transaction_result,
                in_message,
                ..
            },
            updated_shard_account_stuff,
        ) = rayon_run(move || {
            Self::execute_one_message(account_id, msg, shard_account_stuff, &config, &params)
        })
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
    params: &ExecuteParams,
    config: &PreloadedBlockchainConfig,
) -> Result<Box<(CurrencyCollection, Lazy<Transaction>)>> {
    let executor = OrdinaryTransactionExecutor::new();
    executor
        .execute_with_libs_and_params(Some(new_msg_cell), shard_account, params, config)
        .map(Box::new)
}

/// execute tick tock message
fn execute_ticktock_message(
    tick_tock: TickTock,
    shard_account: &mut ShardAccount,
    params: &ExecuteParams,
    config: &PreloadedBlockchainConfig,
) -> Result<Box<(CurrencyCollection, Lazy<Transaction>)>> {
    let executor = TickTockTransactionExecutor::new(tick_tock);
    executor
        .execute_with_libs_and_params(None, shard_account, params, config)
        .map(Box::new)
}

/// calculate group
pub fn _calculate_group(
    messages_set: &[AsyncMessage],
    group_limit: u32,
    offset: u32,
) -> (u32, FastHashMap<AccountId, AsyncMessage>) {
    let mut new_offset = offset;
    let mut holes_group: FastHashMap<AccountId, u32> = FastHashMap::default();
    let mut max_account_count = 0;
    let mut holes_max_count: i32 = 0;
    let mut holes_count: i32 = 0;
    let mut group = FastHashMap::default();
    for (i, msg) in messages_set.iter().enumerate() {
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
        if (i as u32) < offset {
            let count = holes_group.entry(account_id).or_default();
            *count += 1;
            if *count > max_account_count {
                max_account_count = *count;
                holes_max_count = (group_limit * max_account_count) as i32 - offset as i32;
            }
        } else if group.len() < group_limit as usize {
            match holes_group.get_mut(&account_id) {
                None => {
                    if holes_max_count > 0 {
                        holes_group.insert(account_id, 1);
                        holes_max_count -= 1;
                        holes_count += 1;
                    } else {
                        // if group have this account we skip it
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            group.entry(account_id)
                        {
                            e.insert(msg.clone());
                        } else {
                            // if the offset was not set previously, and the account is skipped then
                            // it means that we need to move by current group length
                            if new_offset == offset {
                                new_offset += group.len() as u32 + holes_count as u32;
                            }
                        }
                    }
                }
                Some(count) => {
                    if *count != max_account_count && holes_max_count > 0 {
                        *count += 1;
                        holes_max_count -= 1;
                        holes_count += 1;
                    } else {
                        // group has this account, but it was not taken on previous runs
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            group.entry(account_id)
                        {
                            e.insert(msg.clone());
                        } else {
                            // if the offset was not set previously, and the account is skipped then
                            // it means that we need to move by current group length
                            if new_offset == offset {
                                new_offset += group.len() as u32 + holes_count as u32;
                            }
                        }
                    }
                }
            }
        } else {
            break;
        }
    }
    // if new offset was not set then it means that we took all group elements and all holes on our way
    if new_offset == offset {
        new_offset += group.len() as u32 + holes_count as u32;
    }
    (new_offset, group)
}

/// calculate all groups in advance
pub fn pre_calculate_groups(
    messages_set: Vec<AsyncMessage>,
    group_limit: usize,
    group_vert_size: usize,
) -> FastHashMap<u32, FastHashMap<AccountId, Vec<AsyncMessage>>> {
    let mut res: FastHashMap<u32, FastHashMap<AccountId, Vec<AsyncMessage>>> = Default::default();
    for msg in messages_set {
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

        let mut g_idx = 0;
        let mut group_entry;
        loop {
            group_entry = res.entry(g_idx).or_default();
            if group_entry.len() == group_limit {
                g_idx += 1;
                continue;
            }
            let account_entry = group_entry.entry(account_id);
            match account_entry {
                Entry::Vacant(entry) => {
                    entry.insert(vec![msg]);
                    break;
                }
                Entry::Occupied(mut entry) => {
                    let msgs = entry.get_mut();
                    if msgs.len() < group_vert_size {
                        msgs.push(msg);
                        break;
                    } else {
                        g_idx += 1;
                    }
                }
            }
        }
    }
    res
}
