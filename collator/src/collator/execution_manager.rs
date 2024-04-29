use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::Result;
use everscale_types::cell::Cell;
use everscale_types::models::{
    ExtInMsgInfo, IntMsgInfo, Lazy, MsgInfo, OptionalAccount, ShardAccount, TickTock, Transaction,
};
use everscale_types::{
    cell::HashBytes,
    dict::Dict,
    models::{BlockchainConfig, LibDescr},
};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use ton_executor::{
    ExecuteParams, OrdinaryTransactionExecutor, TickTockTransactionExecutor, TransactionExecutor,
};

use super::super::types::{AccountId, ShardAccountStuff};
use crate::collator::types::ShardStateProvider;

/// Execution manager
pub(super) struct ExecutionManager {
    /// libraries
    pub libraries: Dict<HashBytes, LibDescr>,
    /// gen_utime
    gen_utime: u32,
    // block's start logical time
    start_lt: u64,
    // actual maximum logical time
    max_lt: Arc<AtomicU64>,
    // this time is used if account's lt is smaller
    min_lt: Arc<AtomicU64>,
    // block random seed
    seed_block: HashBytes,
    /// blockchain config
    config: BlockchainConfig,
    /// total transaction duration
    total_trans_duration: Arc<AtomicU64>,
    /// block version
    block_version: u32,
    /// messages set
    messages_set: Vec<(MsgInfo, Cell)>,
    /// group limit
    pub group_limit: u32,
}

impl ExecutionManager {
    /// constructor
    pub fn new(
        gen_utime: u32,
        start_lt: u64,
        max_lt: u64,
        seed_block: HashBytes,
        libraries: Dict<HashBytes, LibDescr>,
        config: BlockchainConfig,
        block_version: u32,
        group_limit: u32,
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
        }
    }

    /// execute messages set
    pub fn execute_msgs_set(&mut self, msgs: Vec<(MsgInfo, Cell)>) {
        tracing::trace!("adding set of messages");
        let _ = std::mem::replace(&mut self.messages_set, msgs);
    }

    /// tick
    pub async fn tick(
        &mut self,
        offset: u32,
        shard_state_provider: Arc<dyn ShardStateProvider>,
    ) -> Result<(u32, Vec<(Result<Box<Transaction>>, ShardAccount)>)> {
        tracing::trace!("execute manager messages set tick with {offset}");

        let (new_offset, group) = calculate_group(&self.messages_set, self.group_limit, offset);

        let mut futures: FuturesUnordered<_> = Default::default();
        let total_trans_duration = self.total_trans_duration.clone();

        for (account_id, msg) in group {
            futures.push(self.execute_message(account_id, msg, shard_state_provider.clone()));
        }
        let now = std::time::Instant::now();

        let mut executed_messages = vec![];
        while let Some(executed_message) = futures.next().await {
            executed_messages.push(executed_message?);
        }

        let duration = now.elapsed().as_micros() as u64;
        tracing::trace!("tick executed {duration}μ;",);

        total_trans_duration.fetch_add(duration, Ordering::Relaxed);

        Ok((new_offset, executed_messages))
    }

    /// execute message
    pub async fn execute_message(
        &self,
        account_id: AccountId,
        new_msg: (MsgInfo, Cell),
        shard_state_provider: Arc<dyn ShardStateProvider>,
    ) -> Result<(Result<Box<Transaction>>, ShardAccount)> {
        let (msg, new_msg_cell) = new_msg;
        tracing::trace!("execute message for account {account_id}");

        let state_libs = self.libraries.clone();
        let max_lt = self.max_lt.load(Ordering::Acquire);
        let min_lt = self.min_lt.load(Ordering::Acquire);
        let block_unixtime = self.gen_utime;
        let block_lt = self.start_lt;
        let seed_block = self.seed_block.clone();
        let block_version = self.block_version;

        let (mut transaction_res, shard_account_stuff) = tokio::task::spawn_blocking(move || {
            let shard_account =
                if let Some(shard_account) = shard_state_provider.get_shard_state(&account_id) {
                    shard_account
                } else {
                    ShardAccount {
                        account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
                        last_trans_hash: Default::default(),
                        last_trans_lt: 0,
                    }
                };

            let mut shard_acc = ShardAccountStuff::new(account_id, shard_account, max_lt, min_lt)?;

            let params = ExecuteParams {
                state_libs,
                block_unixtime,
                block_lt,
                last_tr_lt: shard_acc.lt.clone(),
                seed_block,
                block_version,
                ..ExecuteParams::default()
            };
            let config = self.config.clone();
            let mut account_root = shard_acc.account_root.clone();
            // TODO move spawn blocking upper level and result Box<Transacton>
            let mut transaction_res =
                execute_ordinary_message(&msg, &new_msg_cell, &mut account_root, params, &config);
            if let Ok(transaction) = transaction_res.as_mut() {
                shard_acc.add_transaction(transaction, account_root)?;
            }
            Ok((transaction_res, shard_acc))
        })
        .await??;

        self.max_lt.fetch_max(
            shard_account_stuff.lt.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        Ok((transaction_res, shard_account_stuff.shard_account))
    }
}

/// execute ordinary message
fn execute_ordinary_message(
    new_msg_info: &MsgInfo,
    new_msg_cell: &Cell,
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &BlockchainConfig,
) -> Result<Box<Transaction>> {
    let executor = OrdinaryTransactionExecutor::new();
    Box::new(executor.execute_with_libs_and_params(
        new_msg_info,
        new_msg_cell,
        account_root,
        params,
        config,
    ))
}

/// execute tick tock message
fn execute_ticktock_message(
    tick_tock: TickTock,
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &BlockchainConfig,
) -> Result<Box<Transaction>> {
    let executor = TickTockTransactionExecutor::new(tick_tock);
    Box::new(executor.execute_with_libs_and_params(None, account_root, params, config))
}

/// calculate group
pub fn calculate_group(
    messages_set: &[(MsgInfo, Cell)],
    group_limit: u32,
    offset: u32,
) -> (u32, HashMap<AccountId, (MsgInfo, Cell)>) {
    let mut new_offset = offset;
    let mut holes_group: HashMap<AccountId, u32> = HashMap::new();
    let mut max_account_count = 0;
    let mut holes_max_count: i32 = 0;
    let mut holes_count: i32 = 0;
    let mut group = HashMap::new();
    for (i, msg) in messages_set.iter().enumerate() {
        let account_id = match msg.0 {
            MsgInfo::ExtIn(ExtInMsgInfo { ref dst, .. }) => {
                dst.as_std().map(|a| a.address.clone()).unwrap_or_default()
            }
            MsgInfo::Int(IntMsgInfo { ref dst, .. }) => {
                dst.as_std().map(|a| a.address.clone()).unwrap_or_default()
            }
            _ => {
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
        } else {
            if group.len() < group_limit as usize {
                match holes_group.get_mut(&account_id) {
                    None => {
                        if holes_max_count > 0 {
                            holes_group.insert(account_id, 1);
                            holes_max_count -= 1;
                            holes_count += 1;
                        } else {
                            // if group have this account we skip it
                            if group.get(&account_id).is_none() {
                                group.insert(account_id, msg.clone());
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
                            if group.get(&account_id).is_none() {
                                group.insert(account_id, msg.clone());
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
    }
    // if new offset was not set then it means that we took all group elements and all holes on our way
    if new_offset == offset {
        new_offset += group.len() as u32 + holes_count as u32;
    }
    (new_offset, group)
}
