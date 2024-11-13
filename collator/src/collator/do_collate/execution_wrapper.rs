use std::sync::Arc;

use anyhow::{bail, Result};
use everscale_types::cell::{CellBuilder, CellSlice, HashBytes};
use everscale_types::models::{
    BaseMessage, BlockchainConfig, CurrencyCollection, ImportFees, InMsg, InMsgExternal,
    InMsgFinal, IntAddr, IntMsgInfo, IntermediateAddr, Lazy, MsgEnvelope, MsgInfo, OutMsg,
    OutMsgDequeueImmediate, OutMsgExternal, OutMsgImmediate, OutMsgNew, ShardIdent, TickTock,
    Transaction,
};
use ton_executor::ExecutorOutput;
use tycho_block_util::queue::QueueKey;

use super::BlockCollationData;
use crate::collator::execution_manager::MessagesExecutor;
use crate::collator::mq_iterator_adapter::QueueIteratorAdapter;
use crate::collator::types::{ParsedMessage, PreparedInMsg, PreparedOutMsg, SpecialOrigin};
use crate::internal_queue::types::EnqueuedMessage;
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;

pub struct ExecutorWrapper {
    pub executor: MessagesExecutor,
    pub max_new_message_key_to_current_shard: QueueKey,
    pub mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
    pub shard_id: ShardIdent,
    pub mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
}

impl ExecutorWrapper {
    pub fn new(
        executor: MessagesExecutor,
        max_new_message_key_to_current_shard: QueueKey,
        mq_iterator_adapter: QueueIteratorAdapter<EnqueuedMessage>,
        shard_id: ShardIdent,
        mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) -> Self {
        Self {
            executor,
            max_new_message_key_to_current_shard,
            mq_iterator_adapter,
            shard_id,
            mq_adapter,
        }
    }

    pub fn destruct(
        self,
    ) -> (
        MessagesExecutor,
        QueueIteratorAdapter<EnqueuedMessage>,
        Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    ) {
        (self.executor, self.mq_iterator_adapter, self.mq_adapter)
    }

    pub fn process_transaction(
        &mut self,
        executor_output: ExecutorOutput,
        in_message: Option<Box<ParsedMessage>>,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        let new_messages =
            new_transaction(collation_data, &self.shard_id, executor_output, in_message)?;

        collation_data.new_msgs_created_count += new_messages.len() as u64;
        for new_message in new_messages {
            let MsgInfo::Int(int_msg_info) = new_message.info else {
                continue;
            };

            if new_message.dst_in_current_shard {
                let new_message_key = QueueKey {
                    lt: int_msg_info.created_lt,
                    hash: *new_message.cell.repr_hash(),
                };
                self.max_new_message_key_to_current_shard =
                    std::cmp::max(self.max_new_message_key_to_current_shard, new_message_key);
            }

            collation_data.inserted_new_msgs_to_iterator_count += 1;

            let enqueued_message = EnqueuedMessage::from((int_msg_info, new_message.cell));

            self.mq_adapter
                .add_message_to_iterator(self.mq_iterator_adapter.iterator(), enqueued_message)?;
        }

        collation_data.next_lt = self.executor.min_next_lt();
        collation_data.block_limit.lt_current = collation_data.next_lt;

        Ok(())
    }

    /// Create special transactions for the collator
    pub fn create_special_transactions(
        &mut self,
        config: &BlockchainConfig,
        collator_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(target: tracing_targets::COLLATOR, "create_special_transactions");

        // TODO: Execute in parallel if addresses are distinct?

        if !collator_data.value_flow.recovered.tokens.is_zero() {
            self.create_special_transaction(
                &config.get_fee_collector_address()?,
                collator_data.value_flow.recovered.clone(),
                SpecialOrigin::Recover,
                collator_data,
            )?;
        }

        if !collator_data.value_flow.minted.other.is_empty() {
            self.create_special_transaction(
                &config.get_minter_address()?,
                collator_data.value_flow.minted.clone(),
                SpecialOrigin::Mint,
                collator_data,
            )?;
        }

        Ok(())
    }

    fn create_special_transaction(
        &mut self,
        account_id: &HashBytes,
        amount: CurrencyCollection,
        special_origin: SpecialOrigin,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            amount = %amount.tokens,
            ?special_origin,
            "create_special_transaction",
        );

        let Some(account_stuff) = self.executor.take_account_stuff_if(account_id, |_| true)? else {
            return Ok(());
        };

        let in_message = {
            let info = MsgInfo::Int(IntMsgInfo {
                ihr_disabled: false,
                bounce: true,
                bounced: false,
                src: IntAddr::from((-1, HashBytes::ZERO)),
                dst: IntAddr::from((-1, *account_id)),
                value: amount,
                ihr_fee: Default::default(),
                fwd_fee: Default::default(),
                created_lt: collation_data.start_lt,
                created_at: collation_data.gen_utime,
            });
            let cell = CellBuilder::build_from(BaseMessage {
                info: info.clone(),
                init: None,
                body: CellSlice::default(),
                layout: None,
            })?;

            Box::new(ParsedMessage {
                info,
                dst_in_current_shard: true,
                cell,
                special_origin: Some(special_origin),
                dequeued: None,
            })
        };

        let executed = self
            .executor
            .execute_ordinary_transaction(account_stuff, in_message)?;

        let executor_output = executed.result?;

        self.process_transaction(executor_output, Some(executed.in_message), collation_data)?;

        Ok(())
    }

    pub fn create_ticktock_transactions(
        &mut self,
        config: &BlockchainConfig,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            kind = ?tick_tock,
            "create_ticktock_transactions"
        );

        // TODO: Execute in parallel since these are unique accounts

        for account_id in config.get_fundamental_addresses()?.keys() {
            self.create_ticktock_transaction(&account_id?, tick_tock, collation_data)?;
        }

        self.create_ticktock_transaction(&config.address, tick_tock, collation_data)?;
        Ok(())
    }

    fn create_ticktock_transaction(
        &mut self,
        account_id: &HashBytes,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
    ) -> Result<()> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            kind = ?tick_tock,
            "create_ticktock_transaction",
        );

        let Some(account_stuff) =
            self.executor
                .take_account_stuff_if(account_id, |stuff| match tick_tock {
                    TickTock::Tick => stuff.special.tick,
                    TickTock::Tock => stuff.special.tock,
                })?
        else {
            return Ok(());
        };

        let executor_output = self
            .executor
            .execute_ticktock_transaction(account_stuff, tick_tock)?;

        self.process_transaction(executor_output, None, collation_data)?;

        Ok(())
    }
}

/// add in and out messages from to block
#[allow(clippy::vec_box)]
fn new_transaction(
    collation_data: &mut BlockCollationData,
    shard_id: &ShardIdent,
    executor_output: ExecutorOutput,
    in_msg: Option<Box<ParsedMessage>>,
) -> Result<Vec<Box<ParsedMessage>>> {
    tracing::trace!(
        target: tracing_targets::COLLATOR,
        message_hash = ?in_msg.as_ref().map(|m| m.cell.repr_hash()),
        transaction_hash = %executor_output.transaction.inner().repr_hash(),
        "process new transaction from message",
    );

    collation_data.execute_count_all += 1;

    let gas_used = &mut collation_data.block_limit.gas_used;
    *gas_used = gas_used.saturating_add(executor_output.gas_used);

    if let Some(in_msg) = in_msg {
        process_in_message(collation_data, executor_output.transaction.clone(), in_msg)?;
    }

    let mut out_messages = vec![];

    for out_msg_cell in executor_output.out_msgs.values() {
        let out_msg_cell = out_msg_cell?;
        let out_msg_hash = *out_msg_cell.repr_hash();
        let out_msg_info = out_msg_cell.parse::<MsgInfo>()?;

        tracing::trace!(
            target: tracing_targets::COLLATOR,
            message_hash = %out_msg_hash,
            info = ?out_msg_info,
            "adding out message to out_msgs",
        );
        match &out_msg_info {
            MsgInfo::Int(IntMsgInfo { fwd_fee, dst, .. }) => {
                collation_data.int_enqueue_count += 1;

                let dst_prefix = dst.prefix();
                let dst_workchain = dst.workchain();
                let dst_in_current_shard = contains_prefix(shard_id, dst_workchain, dst_prefix);

                let out_msg = OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                        // NOTE: `next_addr` is not used in current routing between shards logic
                        next_addr: if dst_in_current_shard {
                            IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                        } else {
                            IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                        },
                        fwd_fee_remaining: *fwd_fee,
                        message: Lazy::from_raw(out_msg_cell.clone()),
                    })?,
                    transaction: executor_output.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: Some(executor_output.transaction.clone()),
                    });

                out_messages.push(Box::new(ParsedMessage {
                    info: out_msg_info,
                    dst_in_current_shard,
                    cell: out_msg_cell,
                    special_origin: None,
                    dequeued: None,
                }));
            }
            MsgInfo::ExtOut(_) => {
                let out_msg = OutMsg::External(OutMsgExternal {
                    out_msg: Lazy::from_raw(out_msg_cell),
                    transaction: executor_output.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: None,
                    });
            }
            MsgInfo::ExtIn(_) => bail!("External inbound message cannot be an output"),
        }
    }

    Ok(out_messages)
}

fn process_in_message(
    collation_data: &mut BlockCollationData,
    transaction: Lazy<Transaction>,
    in_msg: Box<ParsedMessage>,
) -> Result<()> {
    let import_fees;
    let in_msg_hash = *in_msg.cell.repr_hash();
    let in_msg = match (in_msg.info, in_msg.special_origin) {
        // Messages with special origin are always immediate
        (_, Some(special_origin)) => {
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&MsgEnvelope {
                    cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    fwd_fee_remaining: Default::default(),
                    message: Lazy::from_raw(in_msg.cell),
                })?,
                transaction,
                fwd_fee: Default::default(),
            });

            let msg = in_msg.clone();
            match special_origin {
                SpecialOrigin::Recover => {
                    collation_data.recover_create_msg = Some(msg);
                }
                SpecialOrigin::Mint => {
                    collation_data.mint_msg = Some(msg);
                }
            }

            import_fees = in_msg.compute_fees()?;
            Lazy::new(&in_msg)?
        }
        // External messages are added as is
        (MsgInfo::ExtIn(_), _) => {
            collation_data.execute_count_ext += 1;

            import_fees = ImportFees::default();
            Lazy::new(&InMsg::External(InMsgExternal {
                in_msg: Lazy::from_raw(in_msg.cell),
                transaction,
            }))?
        }
        // Dequeued messages have a dedicated `InMsg` type
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _) if in_msg.dequeued.is_some() => {
            collation_data.execute_count_int += 1;

            let same_shard = in_msg.dequeued.map(|d| d.same_shard).unwrap_or_default();

            let envelope = Lazy::new(&MsgEnvelope {
                // NOTE: `cur_addr` is not used in current routing between shards logic
                cur_addr: if same_shard {
                    IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                } else {
                    IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                },
                next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell),
            })?;

            let in_msg = InMsg::Final(InMsgFinal {
                in_msg_envelope: envelope.clone(),
                transaction,
                fwd_fee,
            });
            import_fees = in_msg.compute_fees()?;

            let in_msg = Lazy::new(&in_msg)?;

            if same_shard {
                let out_msg = OutMsg::DequeueImmediate(OutMsgDequeueImmediate {
                    out_msg_envelope: envelope.clone(),
                    reimport: in_msg.clone(),
                });
                let exported_value = out_msg.compute_exported_value()?;

                collation_data.out_msgs.insert(in_msg_hash, PreparedOutMsg {
                    out_msg: Lazy::new(&out_msg)?,
                    exported_value,
                    new_tx: None,
                });
            }
            collation_data.int_dequeue_count += 1;

            in_msg
        }
        // New messages are added as is
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _) => {
            collation_data.execute_count_new_int += 1;

            let msg_envelope = MsgEnvelope {
                cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                next_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell),
            };
            let in_msg = InMsg::Immediate(InMsgFinal {
                in_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction,
                fwd_fee,
            });

            import_fees = in_msg.compute_fees()?;
            let in_msg = Lazy::new(&in_msg)?;

            let prev_transaction = match collation_data.out_msgs.get(&in_msg_hash) {
                Some(prepared) => match &prepared.new_tx {
                    Some(tx) => tx.clone(),
                    None => anyhow::bail!("invalid out message state for in_msg {in_msg_hash}"),
                },
                None => anyhow::bail!("immediate in_msg {in_msg_hash} not found in out_msgs"),
            };

            let out_msg = OutMsg::Immediate(OutMsgImmediate {
                out_msg_envelope: Lazy::new(&msg_envelope)?,
                transaction: prev_transaction,
                reimport: in_msg.clone(),
            });
            let exported_value = out_msg.compute_exported_value()?;

            collation_data.out_msgs.insert(in_msg_hash, PreparedOutMsg {
                out_msg: Lazy::new(&out_msg)?,
                exported_value,
                new_tx: None,
            });
            collation_data.int_enqueue_count -= 1;

            in_msg
        }
        (msg_info, special_origin) => {
            unreachable!(
                "unexpected message. info: {msg_info:?}, \
                special_origin: {special_origin:?}"
            )
        }
    };

    collation_data.in_msgs.insert(in_msg_hash, PreparedInMsg {
        in_msg,
        import_fees,
    });

    Ok(())
}

pub fn contains_prefix(shard_id: &ShardIdent, workchain_id: i32, prefix_without_tag: u64) -> bool {
    if shard_id.workchain() == workchain_id {
        if shard_id.prefix() == 0x8000_0000_0000_0000u64 {
            return true;
        }
        let shift = 64 - shard_id.prefix_len();
        return (shard_id.prefix() >> shift) == (prefix_without_tag >> shift);
    }
    false
}
