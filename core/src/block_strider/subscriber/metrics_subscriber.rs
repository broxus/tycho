use anyhow::Result;
use futures_util::future::{BoxFuture, FutureExt};
use tycho_block_util::block::BlockStuff;
use tycho_types::models::*;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

#[derive(Debug, Clone, Copy)]
pub struct MetricsSubscriber;

impl BlockSubscriber for MetricsSubscriber {
    type Prepared = ();

    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = BoxFuture<'static, Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block(
        &self,
        cx: &BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'_> {
        handle_block_fut(cx.block.clone())
    }
}

impl StateSubscriber for MetricsSubscriber {
    type HandleStateFut<'a> = BoxFuture<'static, Result<()>>;

    fn handle_state(&self, cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
        handle_block_fut(cx.block.clone())
    }
}

fn handle_block_fut(block: BlockStuff) -> BoxFuture<'static, Result<()>> {
    let histogram = HistogramGuard::begin("tycho_core_metrics_subscriber_handle_block_time");

    rayon_run(move || {
        // NOTE: Move histogram into the closure to ensure it's dropped
        // after the block is handled. We started recording it earlier
        // to also include the time spent on `rayon_run` overhead.
        let _histogram = histogram;

        if let Err(e) = handle_block(&block) {
            tracing::error!("failed to handle block: {e:?}");
        }
        Ok(())
    })
    .boxed()
}

fn handle_block(block: &BlockStuff) -> Result<()> {
    let block_id = block.id();
    let info = block.load_info()?;
    let extra = block.load_extra()?;

    let mut in_msg_count: u32 = 0;
    for descr in extra.in_msg_description.load()?.iter() {
        let (_, _, in_msg) = descr?;
        in_msg_count += matches!(
            in_msg,
            InMsg::External(_) | InMsg::Immediate(_) | InMsg::Final(_)
        ) as u32;
    }

    let mut out_msgs_count: u32 = 0;
    for descr in extra.out_msg_description.load()?.iter() {
        let (_, _, out_msg) = descr?;
        out_msgs_count += matches!(out_msg, OutMsg::New(_) | OutMsg::Immediate(_)) as u32;
    }

    let mut transaction_count = 0u32;
    let mut message_count = 0u32;
    let mut ext_message_count = 0u32;
    let mut account_blocks_count = 0u32;
    let mut contract_deployments = 0u32;
    let mut contract_destructions = 0u32;
    let mut total_gas_used = 0;

    let account_blocks = extra.account_blocks.load()?;
    for entry in account_blocks.iter() {
        let (_, _, account_block) = entry?;
        account_blocks_count += 1;

        for entry in account_block.transactions.iter() {
            let (_, _, tx) = entry?;
            let tx = tx.load()?;

            transaction_count += 1;
            message_count += tx.in_msg.is_some() as u32 + tx.out_msg_count.into_inner() as u32;

            if let Some(in_msg) = &tx.in_msg {
                ext_message_count += in_msg.parse::<MsgType>()?.is_external_in() as u32;
            }

            let was_active = tx.orig_status == AccountStatus::Active;
            let is_active = tx.end_status == AccountStatus::Active;

            contract_deployments += (!was_active && is_active) as u32;
            contract_destructions += (was_active && !is_active) as u32;

            total_gas_used += 'gas: {
                match tx.load_info()? {
                    TxInfo::Ordinary(info) => {
                        if let ComputePhase::Executed(phase) = &info.compute_phase {
                            break 'gas phase.gas_used.into_inner();
                        }
                    }
                    TxInfo::TickTock(info) => {
                        if let ComputePhase::Executed(phase) = &info.compute_phase {
                            break 'gas phase.gas_used.into_inner();
                        }
                    }
                };

                0
            };
        }
    }

    let out_in_message_ratio = if in_msg_count > 0 {
        out_msgs_count as f64 / in_msg_count as f64
    } else {
        0.0
    };
    let out_message_account_ratio = if account_blocks_count > 0 {
        out_msgs_count as f64 / account_blocks_count as f64
    } else {
        0.0
    };

    let labels = &[("workchain", block_id.shard.workchain().to_string())];
    metrics::histogram!("tycho_bc_software_version", labels).record(info.gen_software.version);
    metrics::histogram!("tycho_bc_in_msg_count", labels).record(in_msg_count);
    metrics::histogram!("tycho_bc_out_msg_count", labels).record(out_msgs_count);

    metrics::counter!("tycho_bc_txs_total", labels).increment(transaction_count as _);
    metrics::counter!("tycho_bc_msgs_total", labels).increment(message_count as _);
    metrics::counter!("tycho_bc_ext_msgs_total", labels).increment(ext_message_count as _);

    metrics::counter!("tycho_bc_contract_deploy_total", labels)
        .increment(contract_deployments as _);
    metrics::counter!("tycho_bc_contract_delete_total", labels)
        .increment(contract_destructions as _);

    metrics::histogram!("tycho_bc_total_gas_used", labels).record(total_gas_used as f64);
    metrics::histogram!("tycho_bc_out_in_msg_ratio", labels).record(out_in_message_ratio);
    metrics::histogram!("tycho_bc_out_msg_acc_ratio", labels).record(out_message_account_ratio);

    Ok(())
}
