use std::sync::Arc;

use anyhow::{Result, bail};
use tycho_block_util::queue::QueueKey;
use tycho_types::cell::{CellBuilder, CellSlice, HashBytes, Lazy};
use tycho_types::models::{
    BaseMessage, BlockchainConfig, CurrencyCollection, ImportFees, InMsg, InMsgExternal,
    InMsgFinal, IntAddr, IntMsgInfo, IntermediateAddr, MsgEnvelope, MsgInfo, OutMsg,
    OutMsgDequeueImmediate, OutMsgExternal, OutMsgImmediate, OutMsgNew, ShardIdent, TickTock,
    Transaction,
};

use crate::collator::execution_manager::{MessagesExecutor, TransactionResult};
use crate::collator::types::{
    BlockCollationData, ExecutedTransaction, ParsedMessage, PreparedInMsg, PreparedOutMsg,
    SpecialOrigin,
};
use crate::internal_queue::types::EnqueuedMessage;
use crate::tracing_targets;
use crate::types::{SaturatingAddAssign, ShardIdentExt};

pub struct ExecutorWrapper {
    pub executor: MessagesExecutor,
    pub max_new_message_key_to_current_shard: QueueKey,
    pub shard_id: ShardIdent,
}

impl ExecutorWrapper {
    pub fn new(executor: MessagesExecutor, shard_id: ShardIdent) -> Self {
        Self {
            executor,
            max_new_message_key_to_current_shard: QueueKey::MIN,
            shard_id,
        }
    }

    pub fn process_transaction(
        &mut self,
        executed: ExecutedTransaction,
        in_message: Option<Box<ParsedMessage>>,
        collation_data: &mut BlockCollationData,
    ) -> Result<Vec<Arc<EnqueuedMessage>>> {
        let mut new_messages = vec![];

        let out_msgs = new_transaction(collation_data, &self.shard_id, executed, in_message)?;
        collation_data.new_msgs_created_count += out_msgs.len() as u64;

        for out_msg in out_msgs {
            let MsgInfo::Int(int_msg_info) = out_msg.info else {
                continue;
            };

            if out_msg.dst_in_current_shard {
                let new_message_key = QueueKey {
                    lt: int_msg_info.created_lt,
                    hash: *out_msg.cell.repr_hash(),
                };
                self.max_new_message_key_to_current_shard =
                    std::cmp::max(self.max_new_message_key_to_current_shard, new_message_key);
            }

            collation_data.inserted_new_msgs_count += 1;
            new_messages.push(Arc::new(EnqueuedMessage::from((
                int_msg_info,
                out_msg.cell,
            ))));
        }

        collation_data.next_lt = self.executor.min_next_lt();
        collation_data.block_limit.lt_current = collation_data.next_lt;

        Ok(new_messages)
    }

    /// Create special transactions for the collator
    pub fn create_special_transactions(
        &mut self,
        config: &BlockchainConfig,
        collator_data: &mut BlockCollationData,
    ) -> Result<Vec<Arc<EnqueuedMessage>>> {
        tracing::trace!(target: tracing_targets::COLLATOR, "create_special_transactions");

        // TODO: Execute in parallel if addresses are distinct?

        let mut result = vec![];

        if !collator_data.value_flow.recovered.is_zero() {
            let mut new_messages = self.create_special_transaction(
                &config.get_fee_collector_address()?,
                collator_data.value_flow.recovered.clone(),
                SpecialOrigin::Recover,
                collator_data,
            )?;
            result.append(&mut new_messages);
        }

        if !collator_data.value_flow.minted.is_zero() {
            let mut new_messages = self.create_special_transaction(
                &config.get_minter_address()?,
                collator_data.value_flow.minted.clone(),
                SpecialOrigin::Mint,
                collator_data,
            )?;
            result.append(&mut new_messages);
        }

        Ok(result)
    }

    fn create_special_transaction(
        &mut self,
        account_id: &HashBytes,
        amount: CurrencyCollection,
        special_origin: SpecialOrigin,
        collation_data: &mut BlockCollationData,
    ) -> Result<Vec<Arc<EnqueuedMessage>>> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            amount = %amount.tokens,
            ?special_origin,
            "create_special_transaction",
        );

        let Some(account_stuff) = self.executor.take_account_stuff_if(account_id, |_| true)? else {
            return Ok(vec![]);
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
                block_seqno: Some(collation_data.block_id_short.seqno),
                from_same_shard: None,
                ext_msg_chain_time: None,
            })
        };

        let executed = self
            .executor
            .execute_ordinary_transaction(account_stuff, in_message)?;

        let TransactionResult::Executed(tx) = executed.result else {
            anyhow::bail!("special transactions can't be skipped");
        };

        self.process_transaction(tx, Some(executed.in_message), collation_data)
    }

    pub fn create_ticktock_transactions(
        &mut self,
        config: &BlockchainConfig,
        tick_tock: TickTock,
        collation_data: &mut BlockCollationData,
    ) -> Result<Vec<Arc<EnqueuedMessage>>> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            kind = ?tick_tock,
            "create_ticktock_transactions"
        );

        // TODO: Execute in parallel since these are unique accounts

        let mut result = vec![];

        for account_id in config.get_fundamental_addresses()?.keys() {
            let mut new_messages =
                self.create_ticktock_transaction(&account_id?, tick_tock, collation_data)?;
            result.append(&mut new_messages);
        }

        let mut new_messages =
            self.create_ticktock_transaction(&config.address, tick_tock, collation_data)?;
        result.append(&mut new_messages);

        Ok(result)
    }

    fn create_ticktock_transaction(
        &mut self,
        account_id: &HashBytes,
        kind: TickTock,
        collation_data: &mut BlockCollationData,
    ) -> Result<Vec<Arc<EnqueuedMessage>>> {
        tracing::trace!(
            target: tracing_targets::COLLATOR,
            account_addr = %account_id,
            ?kind,
            "create_ticktock_transaction",
        );

        let Some(account_stuff) =
            self.executor
                .take_account_stuff_if(account_id, |stuff| match kind {
                    TickTock::Tick => stuff.special.tick,
                    TickTock::Tock => stuff.special.tock,
                })?
        else {
            return Ok(Vec::new());
        };

        let TransactionResult::Executed(executor_output) = self
            .executor
            .execute_ticktock_transaction(account_stuff, kind)?
        else {
            return Ok(Vec::new());
        };

        self.process_transaction(executor_output, None, collation_data)
    }
}

/// add in and out messages from to block
#[allow(clippy::vec_box)]
fn new_transaction(
    collation_data: &mut BlockCollationData,
    shard_id: &ShardIdent,
    executed: ExecutedTransaction,
    in_msg: Option<Box<ParsedMessage>>,
) -> Result<Vec<Box<ParsedMessage>>> {
    tracing::trace!(
        target: tracing_targets::COLLATOR,
        message_hash = ?in_msg.as_ref().map(|m| m.cell.repr_hash()),
        transaction_hash = %executed.transaction.inner().repr_hash(),
        "process new transaction from message",
    );

    // Update collation data.
    collation_data.execute_count_all += 1;

    collation_data
        .block_limit
        .gas_used
        .saturating_add_assign(executed.gas_used);

    assert!(
        shard_id.is_masterchain() || executed.burned.is_zero(),
        "Burn is allowed only in masterchain (block_id={})",
        collation_data.block_id_short,
    );
    let value_flow = &mut collation_data.value_flow;
    value_flow.burned.try_add_assign_tokens(executed.burned)?;

    // Process inbound message.
    if let Some(in_msg) = in_msg {
        process_in_message(collation_data, executed.transaction.clone(), in_msg)?;
    }

    // Process outbound messages.
    let mut out_messages = vec![];
    for out_msg_cell in executed.out_msgs {
        let out_msg_hash = *out_msg_cell.inner().repr_hash();
        let out_msg_info = out_msg_cell.inner().parse::<MsgInfo>()?;

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
                let dst_in_current_shard = shard_id.contains_prefix(dst_workchain, dst_prefix);

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
                        message: out_msg_cell.clone(),
                    })?,
                    transaction: executed.transaction.clone(),
                });

                collation_data
                    .out_msgs
                    .insert(out_msg_hash, PreparedOutMsg {
                        out_msg: Lazy::new(&out_msg)?,
                        exported_value: out_msg.compute_exported_value()?,
                        new_tx: Some(executed.transaction.clone()),
                    });

                out_messages.push(Box::new(ParsedMessage {
                    info: out_msg_info,
                    dst_in_current_shard,
                    cell: out_msg_cell.into_inner(),
                    special_origin: None,
                    block_seqno: Some(collation_data.block_id_short.seqno),
                    from_same_shard: Some(true),
                    ext_msg_chain_time: None,
                }));
            }
            MsgInfo::ExtOut(_) => {
                let out_msg = OutMsg::External(OutMsgExternal {
                    out_msg: out_msg_cell,
                    transaction: executed.transaction.clone(),
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

    collation_data
        .block_limit
        .total_items
        .saturating_add_assign(1);

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
                    message: Lazy::from_raw(in_msg.cell)?,
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
                in_msg: Lazy::from_raw(in_msg.cell)?,
                transaction,
            }))?
        }
        // Dequeued messages have a dedicated `InMsg` type
        (MsgInfo::Int(IntMsgInfo { fwd_fee, .. }), _)
        // check if the message is dequeued or moved from previous collation
            if in_msg.block_seqno.unwrap_or_default() < collation_data.block_id_short.seqno =>
        {
            collation_data.execute_count_int += 1;

            let from_same_shard = in_msg.from_same_shard.unwrap_or_default();

            let envelope = Lazy::new(&MsgEnvelope {
                // NOTE: `cur_addr` is not used in current routing between shards logic
                cur_addr: if from_same_shard {
                    IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                } else {
                    IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                },
                next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                fwd_fee_remaining: fwd_fee,
                message: Lazy::from_raw(in_msg.cell)?,
            })?;

            let in_msg = InMsg::Final(InMsgFinal {
                in_msg_envelope: envelope.clone(),
                transaction,
                fwd_fee,
            });
            import_fees = in_msg.compute_fees()?;

            let in_msg = Lazy::new(&in_msg)?;

            if from_same_shard {
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
                message: Lazy::from_raw(in_msg.cell)?,
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

    collation_data
        .block_limit
        .total_items
        .saturating_add_assign(1);

    collation_data.in_msgs.insert(in_msg_hash, PreparedInMsg {
        in_msg,
        import_fees,
    });

    Ok(())
}
