use std::iter;
use std::ops::RangeToInclusive;

use anyhow::{Context, Result};
use indexmap::IndexMap;

use crate::mempool::{MempoolAnchorId, StateUpdateContext};
use crate::tracing_targets;

#[derive(Default)]
pub struct StateUpdateQueue {
    by_seqno: IndexMap<u32, StateUpdateContext, ahash::RandomState>,
    last_drained_seq_no: Option<u32>,
}

impl StateUpdateQueue {
    pub fn push(&mut self, ctx: StateUpdateContext) {
        if let Some(ctx) = self.by_seqno.insert(ctx.mc_block_id.seqno, ctx) {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                mc_block_id = %ctx.mc_block_id.as_short_id(),
                top_processed_to_anchor_id = ctx.top_processed_to_anchor_id,
                "state update context was updated in queue by seqno"
            );
        };
    }

    pub fn drain(
        &mut self,
        prefix: RangeToInclusive<MempoolAnchorId>,
    ) -> Result<Vec<StateUpdateContext>> {
        // TODO report signed block seq_no to mempool, not its anchor - and remove this search
        let end_seqno = (self.by_seqno.values())
            .filter_map(|ctx| {
                (ctx.top_processed_to_anchor_id == prefix.end).then_some(ctx.mc_block_id.seqno)
            })
            .max()
            .with_context(|| {
                format!(
                    "state update context with top_processed_to_anchor_id={} \
                     was not reported to mempool adapter",
                    prefix.end
                )
            })?;

        let end_index = self.by_seqno.get_index_of(&end_seqno).with_context(|| {
            format!(
                "state update context with seq_no={end_seqno} was not reported to mempool adapter",
            )
        })?;

        let result = (self.by_seqno)
            .splice(..=end_index, iter::empty())
            .map(|(_k, v)| v)
            .collect::<Vec<_>>();

        let seq_nos = (self.last_drained_seq_no.into_iter())
            .chain(result.iter().map(|ctx| ctx.mc_block_id.seqno))
            .collect::<Vec<_>>();

        anyhow::ensure!(
            seq_nos.windows(2).all(|w| w[0] < w[1]),
            "mc block seq_no insertion order is not strictly increasing: {seq_nos:?}"
        );

        self.last_drained_seq_no =
            (result.last().map(|ctx| ctx.mc_block_id.seqno)).or(self.last_drained_seq_no);

        self.shrink();

        Ok(result)
    }

    fn shrink(&mut self) {
        if self.by_seqno.capacity() > self.by_seqno.len().saturating_mul(4) {
            (self.by_seqno).shrink_to(self.by_seqno.len().saturating_mul(2));
        }
    }
}
