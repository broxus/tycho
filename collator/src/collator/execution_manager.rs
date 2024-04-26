use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::{anyhow, Result};
use everscale_types::cell::Cell;
use everscale_types::models::envelope_message::MsgEnvelope;
use everscale_types::models::in_message::InMsg;
use everscale_types::models::out_message::OutMsg;
use everscale_types::models::{ShardAccount, Transaction};
use everscale_types::{
    cell::HashBytes,
    dict::Dict,
    models::{BlockchainConfig, LibDescr},
};
use ton_executor::{
    ExecuteParams, OrdinaryTransactionExecutor, TickTockTransactionExecutor, TransactionExecutor,
};

use super::super::types::{AccountId, AsyncMessage, ShardAccountStuff};
use crate::collator::types::{BlockCollationData, PrevData};

/// Execution manager
pub(super) struct ExecutionManager {
    /// changed accounts
    #[allow(clippy::type_complexity)]
    pub changed_accounts: HashMap<
        AccountId,
        (
            tokio::sync::mpsc::Sender<Arc<AsyncMessage>>,
            tokio::task::JoinHandle<Result<ShardAccountStuff>>,
        ),
    >,
    /// max collate threads
    max_collate_threads: u16,
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
    /// transaction receiver
    receiver_tr:
        tokio::sync::mpsc::UnboundedReceiver<Option<(Arc<AsyncMessage>, Result<Transaction>)>>,
    /// transaction sender
    sender_tr: tokio::sync::mpsc::UnboundedSender<Option<(Arc<AsyncMessage>, Result<Transaction>)>>,
    /// total transaction duration
    total_trans_duration: Arc<AtomicU64>,
    /// block version
    block_version: u32,
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
        max_collate_threads: u16,
        block_version: u32,
    ) -> Self {
        let (sender_tr, receiver_tr) = tokio::sync::mpsc::unbounded_channel();
        Self {
            changed_accounts: HashMap::new(),
            max_collate_threads,
            libraries,
            gen_utime,
            start_lt,
            max_lt: Arc::new(AtomicU64::new(max_lt)),
            min_lt: Arc::new(AtomicU64::new(max_lt)),
            seed_block,
            config,
            sender_tr,
            receiver_tr,
            block_version,
            total_trans_duration: Arc::new(AtomicU64::new(0)),
        }
    }

    /// execute message
    pub async fn execute(
        &mut self,
        account_id: AccountId,
        msg: AsyncMessage,
        prev_data: &PrevData,
        collator_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!("execute (adding into queue): {}", account_id);
        if let Some((sender, _handle)) = self.changed_accounts.get(&account_id) {
            sender.send(Arc::new(msg))?;
        } else {
            let shard_acc = if let Some(shard_acc) = prev_data.accounts().account(&account_id)? {
                shard_acc
            } else if let AsyncMessage::Ext(_, msg_id) = msg {
                return Ok(()); // skip external messages for not existing accounts
            } else {
                ShardAccount::default()
            };
            let (sender, handle) = self.start_account_job(account_id.clone(), shard_acc)?;
            sender.send(Arc::new(msg))?;
            self.changed_accounts.insert(account_id, (sender, handle));
        }

        self.check_parallel_transactions(collator_data).await?;

        Ok(())
    }

    /// start account job
    fn start_account_job(
        &self,
        account_addr: AccountId,
        shard_acc: ShardAccount,
    ) -> Result<(
        tokio::sync::mpsc::UnboundedSender<Arc<AsyncMessage>>,
        tokio::task::JoinHandle<Result<ShardAccountStuff>>,
    )> {
        tracing::trace!("start_account_job: {}", account_addr);

        let mut shard_acc = ShardAccountStuff::new(
            account_addr,
            shard_acc,
            Arc::new(AtomicU64::new(self.min_lt.load(Ordering::Relaxed))),
        )?;

        let block_unixtime = self.gen_utime;
        let block_lt = self.start_lt;
        let seed_block = self.seed_block.clone();
        let total_trans_duration = self.total_trans_duration.clone();
        let sender_tr = self.sender_tr.clone();
        let min_lt = self.min_lt.clone();
        let max_lt = self.max_lt.clone();
        let config = self.config.clone();
        let block_version = self.block_version;
        let libraries = self.libraries.clone().inner();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Arc<AsyncMessage>>();
        let handle = tokio::spawn(async move {
            while let Some(new_msg) = receiver.recv().await {
                tracing::trace!("new message for {:x}", shard_acc.account_addr());
                shard_acc
                    .lt()
                    .fetch_max(min_lt.load(Ordering::Relaxed), Ordering::Relaxed);
                shard_acc
                    .lt()
                    .fetch_max(shard_acc.last_trans_lt() + 1, Ordering::Relaxed);
                shard_acc
                    .lt()
                    .fetch_max(shard_acc.last_trans_lt() + 1, Ordering::Relaxed);

                let mut account_root = shard_acc.account_root();
                let params = ExecuteParams {
                    state_libs: libraries.clone(),
                    block_unixtime,
                    block_lt,
                    last_tr_lt: shard_acc.lt(),
                    seed_block: seed_block.clone(),
                    block_version,
                    ..ExecuteParams::default()
                };
                let new_msg1 = new_msg.clone();
                let (mut transaction_res, account_root, duration) =
                    tokio::task::spawn_blocking(move || {
                        let now = std::time::Instant::now();
                        (
                            execute_new_message(&new_msg1, &mut account_root, params, &config),
                            account_root,
                            now.elapsed().as_micros() as u64,
                        )
                    })
                    .await?;

                if let Ok(transaction) = transaction_res.as_mut() {
                    shard_acc.add_transaction(transaction, account_root)?;
                }
                total_trans_duration.fetch_add(duration, Ordering::Relaxed);
                tracing::trace!(
                    "account {:x} TIME execute {}μ;",
                    shard_acc.account_addr(),
                    duration
                );

                max_lt.fetch_max(shard_acc.lt().load(Ordering::Relaxed), Ordering::Relaxed);
                if let Err(e) = sender_tr.send(Some((new_msg, transaction_res))) {
                    tracing::error!("transaction channel send error: {}", e);
                    return Err(anyhow!("Execution manager transaction receiver is closed, sending msg for account {:x} failed", shard_acc.account_addr()));
                }
            }
            Ok(shard_acc)
        });
        Ok((sender, handle))
    }

    /// wait for transaction
    async fn wait_transaction(&mut self, collator_data: &mut BlockCollationData) -> Result<()> {
        tracing::trace!("wait_transaction");

        if let Some(Some((new_msg, transaction_res))) = self.receiver_tr.recv().await {
            self.finalize_transaction(new_msg, transaction_res, collator_data)?;
        }
        Ok(())
    }

    /// finalize transaction
    fn finalize_transaction(
        &mut self,
        new_msg: Arc<AsyncMessage>,
        transaction_res: Result<Transaction>,
        collator_data: &mut BlockCollationData,
    ) -> Result<()> {
        if let AsyncMessage::Ext(msg, msg_id) = new_msg.deref() {
            let account_id = msg.int_dst_account_id().unwrap_or_default();
            if let Err(err) = transaction_res {
                tracing::warn!(
                    "account {:x} rejected inbound external message {:x}, by reason: {}",
                    account_id,
                    msg_id,
                    err
                );
                // collator_data
                //     .rejected_ext_messages
                //     .push((msg_id.clone(), err.to_string()));
                return Ok(());
            } else {
                tracing::debug!(
                    "account {:x} accepted inbound external message {:x}",
                    account_id,
                    msg_id,
                );
                collator_data
                    .externals_processed_upto
                    .push((msg_id.clone(), msg.dst_workchain_id().unwrap_or_default()));
            }
        }
        let tr = transaction_res?;
        let tr_cell = tr.serialize()?;
        tracing::trace!(
            "finalize_transaction {} with hash {:x}, {:x}",
            tr.logical_time(),
            tr_cell.repr_hash(),
            tr.account_id()
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
            AsyncMessage::New(env, prev_tr_cell) => {
                let env_cell = env.inner().serialize()?;
                let in_msg = InMsg::immediate(
                    env_cell.clone(),
                    tr_cell.clone(),
                    env.fwd_fee_remaining().clone(),
                );
                let out_msg =
                    OutMsg::immediate(env_cell, prev_tr_cell.clone(), in_msg.serialize()?);
                collator_data.add_out_msg_to_block(env.message_hash(), &out_msg)?;
                Some(in_msg)
            }
            AsyncMessage::Mint(msg) | AsyncMessage::Recover(msg) => {
                let msg_cell = msg.serialize()?;
                let src = msg.src_ref().ok_or_else(|| {
                    tracing::error!(
                        "source address of message {:x} is invalid",
                        msg_cell.repr_hash()
                    )
                })?;
                let src_prefix = AccountIdPrefixFull::prefix(src)?;
                let dst = msg.dst_ref().ok_or_else(|| {
                    tracing::error!(
                        "destination address of message {:x} is invalid",
                        msg_cell.repr_hash()
                    )
                })?;
                let dst_prefix = AccountIdPrefixFull::prefix(dst)?;
                let (cur_addr, next_addr) =
                    perform_hypercube_routing(&src_prefix, &dst_prefix, shard, use_hypercube)?;
                let env = MsgEnvelope::with_routing(msg_cell, fwd_fee, cur_addr, next_addr);

                Some(InMsg::immediate(
                    env.inner().serialize()?,
                    tr_cell.clone(),
                    Default::default(),
                ))
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

        match new_msg.deref() {
            AsyncMessage::Mint(_) => collator_data.mint_msg = in_msg_opt,
            AsyncMessage::Recover(_) => collator_data.recover_create_msg = in_msg_opt,
            _ => (),
        }
        collator_data.block_full |= !collator_data.limit_fits(ParamLimitIndex::Normal);
        Ok(())
    }
}

/// execute new message
fn execute_new_message(
    new_msg: &AsyncMessage,
    account_root: &mut Cell,
    params: ExecuteParams,
    config: &BlockchainConfig,
) -> Result<Transaction> {
    let (executor, msg_opt): (Box<dyn TransactionExecutor>, _) = match new_msg {
        AsyncMessage::Int(enq, _our) => (
            Box::new(OrdinaryTransactionExecutor::new()),
            Some(enq.env.message.clone()),
        ),
        AsyncMessage::Recover(msg) | AsyncMessage::Mint(msg) | AsyncMessage::Ext(msg, _) => {
            (Box::new(OrdinaryTransactionExecutor::new()), Some(msg))
        }
        AsyncMessage::New(env, _prev_tr_cell) => (
            Box::new(OrdinaryTransactionExecutor::new()),
            Some(env.message.clone()),
        ),
        AsyncMessage::TickTock(tt) => {
            (Box::new(TickTockTransactionExecutor::new(tt.clone())), None)
        }
    };
    executor.execute_with_libs_and_params(msg_opt, account_root, params, config)
}
