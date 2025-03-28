use std::cmp;
use std::collections::BTreeMap;

use anyhow::Result;

use crate::mempool::{DebugStateUpdateContext, StateUpdateContext};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

#[derive(Default)]
pub struct StateUpdateQueue {
    unsigned: BTreeMap<BlockSeqno, StateUpdateContext>,
    drained: BlockSeqno, // only signed versions are reported to mempool
    signed: BlockSeqno,  // seqno=0 is stateinit
}

impl StateUpdateQueue {
    pub fn push(&mut self, ctx: StateUpdateContext) -> Result<Option<StateUpdateContext>> {
        let new_seqno = ctx.mc_block_id.seqno;
        match new_seqno.cmp(&self.drained) {
            cmp::Ordering::Less => {
                tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    last_drained = self.drained,
                    last_signed = self.signed,
                    ignored = ?DebugStateUpdateContext(&ctx),
                    "cannot accept mc state update after newer one was passed to mempool"
                );
                return Ok(None);
            }
            cmp::Ordering::Equal => {
                if new_seqno == 0 {
                    return Ok(Some(ctx)); // stateinit special case
                }
                tracing::error!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    last_drained = self.drained,
                    last_signed = self.signed,
                    ignored = ?DebugStateUpdateContext(&ctx),
                    "another version with same seqno is already reported to mempool"
                );
                return Ok(None);
            }
            cmp::Ordering::Greater => {} // ok
        }
        if new_seqno <= self.signed {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                new_seqno,
                last_drained = self.drained,
                last_signed = self.signed,
                "mc state update received after being singed"
            );
            self.drained = new_seqno;
            return Ok(Some(ctx));
        }
        if let Some(ctx) = self.unsigned.insert(new_seqno, ctx) {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                removed = ?DebugStateUpdateContext(&ctx),
                "state update context was updated in queue by seqno"
            );
        };
        Ok(None)
    }

    pub fn signed(&mut self, signed: BlockSeqno) -> Result<Vec<StateUpdateContext>> {
        if self.signed < signed {
            self.signed = signed;
            self.drain()
        } else if signed == 0 {
            Ok(Vec::new()) // stateinit returned from `push()`, do not warn
        } else {
            tracing::warn!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                new = signed,
                prev = self.signed,
                "signed mc block seqno was reported out of order"
            );
            Ok(Vec::new())
        }
    }

    fn drain(&mut self) -> Result<Vec<StateUpdateContext>> {
        anyhow::ensure!(
            self.drained <= self.signed,
            "coding error: drained unsigned state update ctx; \
             last drained seqno={} last signed seqno={}",
            self.drained,
            self.signed,
        );

        if self.drained == self.signed {
            return Ok(Vec::new());
        }

        let mut vec =
            Vec::with_capacity(((self.signed - self.drained) as usize).min(self.unsigned.len()));
        while let Some(entry) = self.unsigned.first_entry() {
            if *entry.key() > self.signed {
                break;
            }
            vec.push(entry.remove());
        }

        if let Some(first_seqno) = (vec.first()).map(|ctx| ctx.mc_block_id.seqno) {
            anyhow::ensure!(
                self.drained < first_seqno,
                "coding error: cannot report mc state update with repeating or lower seqno; \
                 prev drained seqno = {}, new drained seqno {first_seqno}",
                self.drained
            );
        }

        if let Some(last_seqno) = (vec.last()).map(|ctx| ctx.mc_block_id.seqno) {
            self.drained = last_seqno;
        }

        Ok(vec)
    }
}
