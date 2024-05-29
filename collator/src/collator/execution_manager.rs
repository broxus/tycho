use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::dict::Dict;
use everscale_types::models::{
    BlockchainConfig, ExtInMsgInfo, HashUpdate, IntMsgInfo, Lazy, LibDescr, MsgInfo,
    OptionalAccount, ShardAccount, ShardAccounts, TickTock, Transaction,
};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use ton_executor::blockchain_config::PreloadedBlockchainConfig;
use ton_executor::{
    ExecuteParams, OrdinaryTransactionExecutor, TickTockTransactionExecutor, TransactionExecutor,
};
use tycho_util::FastHashMap;

use super::types::AsyncMessage;
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
    // actual maximum logical time
    pub max_lt: Arc<AtomicU64>,
    // this time is used if account's lt is smaller
    pub min_lt: Arc<AtomicU64>,
    // block random seed
    seed_block: HashBytes,
    /// blockchain config
    config: BlockchainConfig,
    /// total transaction duration
    total_trans_duration: Arc<AtomicU64>,
    /// block version
    block_version: u32,
    /// messages set
    messages_set: Vec<AsyncMessage>,
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
        max_lt: u64,
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
            max_lt: Arc::new(AtomicU64::new(max_lt)),
            min_lt: Arc::new(AtomicU64::new(max_lt)),
            seed_block,
            config,
            block_version,
            group_limit,
            total_trans_duration: Arc::new(AtomicU64::new(0)),
            messages_set: Vec::new(),
            shard_accounts,
            changed_accounts: FastHashMap::default(),
        }
    }

    /// Set messages that will be executed
    pub fn set_msgs_for_execution(&mut self, msgs: Vec<AsyncMessage>) {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "adding set of {} messages", msgs.len());
        let _ = std::mem::replace(&mut self.messages_set, msgs);
    }

    /// Run one execution tick of parallel transactions
    pub async fn execute_tick(
        &mut self,
        offset: u32,
    ) -> Result<(u32, Vec<(AccountId, AsyncMessage, Box<Transaction>)>)> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "messages set execution tick with offset {offset}");

        let (new_offset, group) = calculate_group(&self.messages_set, self.group_limit, offset);

        let mut futures: FuturesUnordered<_> = Default::default();
        let total_trans_duration = self.total_trans_duration.clone();

        // TODO check externals is not exist accounts needed ?
        for (account_id, msg) in group {
            let max_lt = self.max_lt.load(Ordering::Acquire);
            let shard_account_stuff = self.get_shard_account_stuff(account_id, max_lt)?;
            futures.push(self.execute_message(account_id, msg, shard_account_stuff));
        }
        let now = std::time::Instant::now();

        let mut executed_messages = vec![];
        while let Some(executed_message) = futures.next().await {
            let (transaction, msg, ex) = executed_message?;
            if let AsyncMessage::Ext(_, _) = &msg {
                if let Err(ref e) = transaction {
                    tracing::error!(target: tracing_targets::EXEC_MANAGER, "failed to execute external transaction: {e:?}");
                    continue;
                }
            }
            executed_messages.push((transaction, msg, ex));
        }

        drop(futures);

        let mut result = vec![];
        for (transaction, msg, shard_account_stuff) in executed_messages {
            result.push((shard_account_stuff.account_addr, msg, transaction?));
            self.update_shard_account_stuff_cache(
                shard_account_stuff.account_addr,
                shard_account_stuff,
            )?;
        }

        let duration = now.elapsed().as_micros() as u64;
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "tick executed {duration}μ;",);

        total_trans_duration.fetch_add(duration, Ordering::Relaxed);

        Ok((new_offset, result))
    }

    pub fn get_shard_account_stuff(
        &self,
        account_id: AccountId,
        max_lt: u64,
    ) -> Result<ShardAccountStuff> {
        let shard_account_stuff = if let Some(a) = self.changed_accounts.get(&account_id) {
            a.clone()
        } else if let Ok(Some((_depth, shard_account))) = self.shard_accounts.get(account_id) {
            ShardAccountStuff::new(account_id, shard_account.clone(), max_lt)?
        } else {
            let shard_account = EMPTY_SHARD_ACCOUNT
                .get_or_init(|| ShardAccount {
                    account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
                    last_trans_hash: Default::default(),
                    last_trans_lt: 0,
                })
                .clone();
            ShardAccountStuff::new(account_id, shard_account, max_lt)?
        };
        Ok(shard_account_stuff)
    }

    /// execute message
    pub async fn execute_message(
        &self,
        account_id: AccountId,
        new_msg: AsyncMessage,
        mut shard_account_stuff: ShardAccountStuff,
    ) -> Result<(Result<Box<Transaction>>, AsyncMessage, ShardAccountStuff)> {
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
        let (config, params) = self.get_execute_params(shard_account_stuff.lt.clone())?;
        let (transaction_res, msg, shard_account_stuff) = tokio::task::spawn_blocking(move || {
            let mut account_root = shard_account_stuff.account_root.clone();
            let mut transaction = match &new_msg {
                AsyncMessage::Recover(new_msg_cell)
                | AsyncMessage::Mint(new_msg_cell)
                | AsyncMessage::Ext(_, new_msg_cell)
                | AsyncMessage::Int(_, new_msg_cell, _)
                | AsyncMessage::NewInt(_, new_msg_cell) => {
                    execute_ordinary_message(new_msg_cell, &mut account_root, params, &config)
                }
                AsyncMessage::TickTock(ticktock_) => {
                    execute_ticktock_message(*ticktock_, &mut account_root, params, &config)
                }
            };
            if let Ok(transaction) = transaction.as_mut() {
                // TODO replace with batch set
                shard_account_stuff.add_transaction(transaction, account_root)?;
            }
            Ok((transaction, new_msg, shard_account_stuff))
                as Result<(Result<Box<Transaction>>, AsyncMessage, ShardAccountStuff)>
        })
        .await??;

        self.max_lt.fetch_max(
            shard_account_stuff.lt.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        Ok((transaction_res, msg, shard_account_stuff))
    }

    fn update_shard_account_stuff_cache(
        &mut self,
        account_id: AccountId,
        mut account_stuff: ShardAccountStuff,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "update shard account for account {account_id}");
        let old_state = account_stuff.state_update.load()?.old;
        account_stuff.state_update = Lazy::new(&HashUpdate {
            old: old_state,
            new: *account_stuff.account_root.repr_hash(),
        })?;

        self.changed_accounts.insert(account_id, account_stuff);
        Ok(())
    }

    /// execute special transaction
    pub async fn execute_special_transaction(
        &mut self,
        account_id: AccountId,
        msg: AsyncMessage,
    ) -> Result<Box<Transaction>> {
        tracing::trace!(target: tracing_targets::EXEC_MANAGER, "execute special transaction");
        let max_lt = self.max_lt.load(Ordering::Acquire);
        let shard_account_stuff = self.get_shard_account_stuff(account_id, max_lt)?;
        let (transaction, _, shard_account_stuff) = self
            .execute_message(account_id, msg, shard_account_stuff)
            .await?;
        self.update_shard_account_stuff_cache(account_id, shard_account_stuff)?;
        transaction
    }

    fn get_execute_params(
        &self,
        last_tr_lt: Arc<AtomicU64>,
    ) -> Result<(PreloadedBlockchainConfig, ExecuteParams)> {
        let state_libs = self.libraries.clone();
        let block_unixtime = self.gen_utime;
        let block_lt = self.start_lt;
        let seed_block = self.seed_block;
        let block_version = self.block_version;
        let config = PreloadedBlockchainConfig::with_config(self.config.clone(), 0)?; // TODO: fix global id
        let params = ExecuteParams {
            state_libs,
            block_unixtime,
            block_lt,
            last_tr_lt,
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
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &PreloadedBlockchainConfig,
) -> Result<Box<Transaction>> {
    let executor = OrdinaryTransactionExecutor::new();
    executor
        .execute_with_libs_and_params(Some(new_msg_cell), account_root, &params, config)
        .map(Box::new)
}

/// execute tick tock message
fn execute_ticktock_message(
    tick_tock: TickTock,
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &PreloadedBlockchainConfig,
) -> Result<Box<Transaction>> {
    let executor = TickTockTransactionExecutor::new(tick_tock);
    executor
        .execute_with_libs_and_params(None, account_root, &params, config)
        .map(Box::new)
}

/// calculate group
pub fn calculate_group(
    messages_set: &[AsyncMessage],
    group_limit: u32,
    offset: u32,
) -> (u32, HashMap<AccountId, AsyncMessage>) {
    let mut new_offset = offset;
    let mut holes_group: HashMap<AccountId, u32> = HashMap::new();
    let mut max_account_count = 0;
    let mut holes_max_count: i32 = 0;
    let mut holes_count: i32 = 0;
    let mut group = HashMap::new();
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
                                new_offset += group.len() as u32;
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
                                new_offset += group.len() as u32;
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
