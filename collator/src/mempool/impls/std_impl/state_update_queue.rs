use std::cmp;
use std::collections::BTreeMap;

use anyhow::Result;

use crate::mempool::{DebugStateUpdateContext, StateUpdateContext};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

#[derive(Default)]
pub struct StateUpdateQueue {
    unsigned: BTreeMap<BlockSeqno, Box<StateUpdateContext>>,
    drained: BlockSeqno, // only signed versions are reported to mempool
    signed: BlockSeqno,  // seqno=0 is stateinit
}

impl StateUpdateQueue {
    pub fn push(
        &mut self,
        ctx: Box<StateUpdateContext>,
    ) -> Result<Option<Box<StateUpdateContext>>> {
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

    #[allow(clippy::vec_box, reason = "caller site shouldn't re-box")]
    pub fn signed(&mut self, signed: BlockSeqno) -> Result<Vec<Box<StateUpdateContext>>> {
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

    #[allow(clippy::vec_box, reason = "caller site shouldn't re-box")]
    fn drain(&mut self) -> Result<Vec<Box<StateUpdateContext>>> {
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

        let vec = (self.unsigned)
            .extract_if(..=self.signed, |_, _| true)
            .map(|(_, value)| value)
            .collect::<Vec<_>>();

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

#[cfg(all(test, feature = "test"))]
mod test {
    use std::sync::Arc;

    use rand::random;
    use tracing_subscriber::filter::LevelFilter;
    use tycho_consensus::test_utils::default_test_config;
    use tycho_types::cell::HashBytes;
    use tycho_types::models::{
        BlockId, ConsensusInfo, ShardIdent, ValidatorDescription, ValidatorSet,
    };

    use super::*;
    use crate::test_utils::try_init_test_tracing;

    #[tokio::test]
    async fn drain_last_version_when_signed() -> Result<()> {
        try_init_test_tracing(LevelFilter::DEBUG);
        const SEQNO: u32 = 10;

        let mut state_update_queue = StateUpdateQueue::default();

        let original = state_update(SEQNO, 6);
        assert!(state_update_queue.push(original)?.is_none());

        let replacement = state_update(SEQNO, 9);
        assert!(state_update_queue.push(replacement)?.is_none());

        let drained = state_update_queue.signed(SEQNO)?;

        assert_eq!(drained.len(), 1, "signed seqno must drain one update");
        let drained = drained.first().unwrap();

        assert_eq!(drained.top_processed_to_anchor_id, 9, "must drain last");

        Ok(())
    }

    #[tokio::test]
    async fn sign_drains_previous_items() -> Result<()> {
        try_init_test_tracing(LevelFilter::DEBUG);

        let mut state_update_queue = StateUpdateQueue::default();

        assert!(state_update_queue.push(state_update(10, 7))?.is_none());
        assert!(state_update_queue.push(state_update(11, 7))?.is_none());
        assert!(state_update_queue.push(state_update(12, 7))?.is_none());

        let drained = state_update_queue.signed(11)?;

        assert_eq!(drained.len(), 2, "must drain up to signed inclusive");
        let drained = drained.iter().map(|ctx| ctx.mc_block_id.seqno);
        assert_eq!(drained.collect::<Vec<_>>(), vec![10, 11]);

        let last = state_update_queue.signed(12)?;
        assert_eq!(last.len(), 1, "first drain must keep next until signed");
        assert_eq!(last.first().unwrap().mc_block_id.seqno, 12);

        Ok(())
    }

    fn state_update(seqno: u32, top_processed_to_anchor_id: u32) -> Box<StateUpdateContext> {
        Box::new(StateUpdateContext {
            mc_block_id: BlockId {
                shard: ShardIdent::MASTERCHAIN,
                seqno,
                root_hash: Default::default(),
                file_hash: Default::default(),
            },
            mc_block_chain_time: seqno as u64,
            top_processed_to_anchor_id,
            consensus_info: ConsensusInfo::default(),
            consensus_config: default_test_config().conf.consensus,
            shuffle_validators: false,
            prev_validator_set: None,
            current_validator_set: (
                HashBytes([random(); 32]),
                Arc::new(ValidatorSet {
                    utime_since: 0,
                    utime_until: u32::MAX,
                    main: 1.try_into().unwrap(),
                    total_weight: 1,
                    list: vec![ValidatorDescription {
                        public_key: HashBytes([random(); 32]),
                        weight: 1,
                        adnl_addr: None,
                        mc_seqno_since: 0,
                        prev_total_weight: 0,
                    }],
                }),
            ),
            next_validator_set: None,
        })
    }
}
