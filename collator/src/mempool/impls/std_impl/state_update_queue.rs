use std::ops::RangeToInclusive;

use anyhow::{Context, Result};
use indexmap::IndexMap;

use crate::mempool::{DebugStateUpdateContext, StateUpdateContext};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

#[derive(Default)]
pub struct StateUpdateQueue {
    by_seqno: IndexMap<BlockSeqno, StateUpdateContext, ahash::RandomState>,
    last_drained_seqno: Option<BlockSeqno>,
}

impl StateUpdateQueue {
    pub fn push(&mut self, ctx: StateUpdateContext) {
        if let Some(ctx) = self.by_seqno.insert(ctx.mc_block_id.seqno, ctx) {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                removed = ?DebugStateUpdateContext(&ctx),
                "state update context was updated in queue by seqno"
            );
        };
    }

    pub fn drain(
        &mut self,
        range: RangeToInclusive<BlockSeqno>,
    ) -> Result<Vec<StateUpdateContext>> {
        let end_index = (self.by_seqno.get_index_of(&range.end)).with_context(|| {
            format!(
                "state update context with seq_no={} was not reported to mempool adapter",
                range.end
            )
        })?;

        let result = (self.by_seqno)
            .drain(..=end_index)
            .map(|(_k, v)| v)
            .collect::<Vec<_>>();

        let seqnos = (self.last_drained_seqno.into_iter())
            .chain(result.iter().map(|ctx| ctx.mc_block_id.seqno))
            .collect::<Vec<_>>();

        anyhow::ensure!(
            seqnos.windows(2).all(|w| w[0] < w[1]), // unique with `last_drained_seqno`
            "mc block seq_no insertion order is not strictly increasing: {seqnos:?}"
        );

        self.last_drained_seqno =
            (result.last().map(|ctx| ctx.mc_block_id.seqno)).max(self.last_drained_seqno);

        self.shrink();

        Ok(result)
    }

    fn shrink(&mut self) {
        if self.by_seqno.capacity() > self.by_seqno.len().saturating_mul(4) {
            (self.by_seqno).shrink_to(self.by_seqno.len().saturating_mul(2));
        }
    }
}
