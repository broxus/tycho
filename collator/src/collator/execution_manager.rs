use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    mem, result,
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::{anyhow, Result};
use everscale_types::cell::{Cell, CellFamily, Store};
use everscale_types::models::envelope_message::MsgEnvelope;
use everscale_types::models::in_message::InMsg;
use everscale_types::models::out_message::OutMsg;
use everscale_types::models::{
    ExtInMsgInfo, IntMsgInfo, Lazy, MsgInfo, OptionalAccount, OwnedMessage, ShardAccount, TickTock,
    Transaction,
};
use everscale_types::{
    cell::HashBytes,
    dict::Dict,
    models::{BlockchainConfig, LibDescr},
};
use futures_util::future::try_join_all;
use ton_executor::{
    ExecuteParams, OrdinaryTransactionExecutor, TickTockTransactionExecutor, TransactionExecutor,
};

use super::super::types::{AccountId, AsyncMessage, ShardAccountStuff};
use crate::collator::types::{BlockCollationData, PrevData};

/// Execution manager
pub(super) struct ExecutionManager {
    /// changed accounts
    pub changed_accounts: HashMap<AccountId, ShardAccountStuff>,
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
    messages_set: Vec<OwnedMessage>,
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
            changed_accounts: HashMap::new(),
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
    pub fn execute_msgs_set(&mut self, msgs: Vec<OwnedMessage>) {
        tracing::trace!("adding set of messages");
        let _ = std::mem::replace(&mut self.messages_set, msgs);
    }

    /// tick
    pub async fn tick(
        &mut self,
        offset: u32,
        prev_data: &PrevData,
        collator_data: &mut BlockCollationData,
        anchor: u32,
    ) -> Result<()> {
        tracing::trace!("execute manager messages set tick with {offset}");

        let (new_offset, group) = calculate_group(&self.messages_set, self.group_limit, offset);

        let mut futures = vec![];
        let total_trans_duration = self.total_trans_duration.clone();

        for (account_id, msg) in group {
            futures.push(self.execute_message(account_id, msg, prev_data, collator_data));
        }
        let now = std::time::Instant::now();

        let res = try_join_all(futures).await?;

        let duration = now.elapsed().as_micros() as u64;
        tracing::trace!("tick executed {duration}μ;",);

        total_trans_duration.fetch_add(duration, Ordering::Relaxed);

        collator_data
            .externals_processed_upto
            .replace(anchor, &new_offset)?;

        Ok(())
    }

    /// execute message
    pub async fn execute_message(
        &mut self,
        account_id: AccountId,
        msg: OwnedMessage,
        prev_data: &PrevData,
        collator_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!("execute message for account {account_id}");

        let shard_account =
            if let Some(shard_account) = prev_data.observable_accounts().get(&account_id)? {
                shard_account
            } else if let MsgInfo::ExtIn(_) = msg.info {
                return Ok(()); // skip external messages for not existing accounts
            } else {
                ShardAccount {
                    account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
                    last_trans_hash: Default::default(),
                    last_trans_lt: 0,
                }
            };

        let mut shard_acc = ShardAccountStuff::new(
            account_id,
            shard_account,
            self.max_lt.load(Ordering::Relaxed),
            self.libraries.clone(),
        )?;

        shard_acc
            .lt()
            .fetch_max(self.min_lt.load(Ordering::Relaxed), Ordering::Relaxed);
        shard_acc
            .lt()
            .fetch_max(shard_acc.last_trans_lt + 1, Ordering::Relaxed);
        shard_acc
            .lt()
            .fetch_max(shard_acc.last_trans_lt + 1, Ordering::Relaxed);

        let params = ExecuteParams {
            state_libs: self.libraries.clone(),
            block_unixtime: self.gen_utime,
            block_lt: self.start_lt,
            last_tr_lt: shard_acc.lt(),
            seed_block: self.seed_block.clone(),
            block_version: self.block_version,
            ..ExecuteParams::default()
        };
        let config = self.config.clone();
        let mut account_root = shard_acc.account_root.clone();
        let (mut transaction_res, account_root) = tokio::task::spawn_blocking(move || {
            (
                execute_ordinary_message(&msg, &mut account_root, params, &config),
                account_root,
            )
        })
        .await?;

        if let Ok(transaction) = transaction_res.as_mut() {
            shard_acc.add_transaction(transaction, account_root)?;
        }

        self.max_lt
            .fetch_max(shard_acc.lt.load(Ordering::Relaxed), Ordering::Relaxed);

        self.finalize_transaction(msg, transaction_res, collator_data)?;
        self.changed_accounts.insert(account_id, shard_acc);

        // TODO! add to queue
        // collator_data.add_to_queue(account_id, msg);

        Ok(())
    }

    /// finalize transaction
    fn finalize_transaction(
        &mut self,
        new_msg: OwnedMessage,
        transaction_res: Result<Transaction>,
        collator_data: &mut BlockCollationData,
    ) -> Result<()> {
        if let MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) = new_msg.info {
            let account_id = dst.as_std().unwrap_or_default().address;
            if let Err(err) = transaction_res {
                tracing::warn!(
                    "account {account_id} rejected inbound external message  by reason: {err}",
                );
                return Ok(());
            } else {
                tracing::debug!("account {account_id} accepted inbound external message",);
            }
        }
        let tr = transaction_res?;
        let mut builder = everscale_types::cell::CellBuilder::new();

        tr.store_into(&mut builder, &mut Cell::empty_context())?;

        let tr_cell = builder.build()?;

        tracing::trace!(
            "finalize_transaction {} with hash {:x}, {}",
            tr.lt,
            tr_cell.repr_hash(),
            tr.account
        );
        let in_msg_opt = match new_msg.deref() {
            AsyncMessage::Int(enq, our) => {
                let in_msg = InMsg::final_msg(
                    enq.envelope_cell(),
                    tr_cell.clone(),
                    enq.fwd_fee_remaining().clone(),
                );
                if *our {
                    let out_msg =
                        OutMsg::dequeue_immediate(enq.envelope_cell(), in_msg.serialize()?);
                    collator_data.add_out_msg_to_block(enq.message_hash(), &out_msg)?;
                    collator_data.del_out_msg_from_state(&enq.out_msg_key())?;
                }
                Some(in_msg)
            }

            AsyncMessage::Ext(msg, _) => {
                let in_msg = InMsg::external(msg.serialize()?, tr_cell.clone());
                Some(in_msg)
            }
            AsyncMessage::TickTock(_) => None,
        };
        if tr.orig_status != tr.end_status {
            tracing::info!(
                "Status of account {:x} was changed from {:?} to {:?} by message {:X}",
                tr.account_id(),
                tr.orig_status,
                tr.end_status,
                tr.in_msg_cell().unwrap_or_default().repr_hash()
            );
        }
        collator_data.new_transaction(&tr, tr_cell, in_msg_opt.as_ref())?;

        collator_data.update_lt(self.max_lt.load(Ordering::Relaxed));

        collator_data.block_full |= !collator_data.limit_fits(ParamLimitIndex::Normal);
        Ok(())
    }
}

/// execute ordinary message
fn execute_ordinary_message(
    new_msg: &OwnedMessage,
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &BlockchainConfig,
) -> Result<Transaction> {
    let executor = OrdinaryTransactionExecutor::new();
    executor.execute_with_libs_and_params(new_msg, account_root, params, config)
}

/// execute tick tock message
fn execute_ticktock_message(
    tick_tock: TickTock,
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &BlockchainConfig,
) -> Result<Transaction> {
    let executor = TickTockTransactionExecutor::new(tick_tock);
    executor.execute_with_libs_and_params(None, account_root, params, config)
}

/// calculate group
pub fn calculate_group(
    messages_set: &[OwnedMessage],
    group_limit: u32,
    offset: u32,
) -> (u32, HashMap<AccountId, OwnedMessage>) {
    let mut new_offset = offset;
    let mut holes_group: HashMap<AccountId, u32> = HashMap::new();
    let mut max_account_count = 0;
    let mut holes_max_count: i32 = 0;
    let mut holes_count: i32 = 0;
    let mut group = HashMap::new();
    for (i, msg) in messages_set.iter().enumerate() {
        let account_id = match msg.info() {
            MsgInfo::ExtIn(ExtInMsgInfo { dst, .. }) => dst.as_std().unwrap_or_default().address,
            MsgInfo::Int(IntMsgInfo { dst, .. }) => dst.as_std().unwrap_or_default().address,
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
