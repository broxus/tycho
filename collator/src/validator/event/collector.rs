use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwap;
use tracing::instrument;
use tycho_network::PeerId;
use tycho_types::models::BlockIdShort;
use tycho_util::{DashMapEntry, FastDashMap};

use crate::tracing_targets;
use crate::validator::ValidationSessionId;
use crate::validator::event::EventError::{SessionAlreadyExists, SessionNotFound};
use crate::validator::event::{SessionCtx, SigStatus, SignatureEvent, ValidationEvents};

type PeerMap = HashMap<PeerId, SigStatus>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PeerStat {
    pub valid: u32,
    pub invalid: u32,
}

pub struct ValidationEventCollector {
    committed: ArcSwap<BTreeMap<BlockIdShort, Arc<PeerMap>>>,
    pending:
        FastDashMap<ValidationSessionId, FastDashMap<BlockIdShort, FastDashMap<PeerId, SigStatus>>>,
}

impl Default for ValidationEventCollector {
    fn default() -> Self {
        Self {
            committed: ArcSwap::from_pointee(BTreeMap::new()),
            pending: FastDashMap::default(),
        }
    }
}

// Public API for the collector

impl ValidationEventCollector {
    /// peer -> (valid, invalid) statistics for blocks in the given range.
    pub fn stats_for_blocks(
        &self,
        range: RangeInclusive<BlockIdShort>,
    ) -> HashMap<PeerId, PeerStat> {
        let mut out = HashMap::<PeerId, PeerStat>::new();
        let snap = self.committed.load();

        for (_, peers) in snap.range(range) {
            for (&peer, &status) in peers.iter() {
                let entry = out.entry(peer).or_default();
                match status {
                    SigStatus::Valid => entry.valid += 1,
                    SigStatus::Invalid => entry.invalid += 1,
                }
            }
        }
        out
    }

    /// Remove all blocks in the given range from the committed map.
    #[instrument(skip(self), fields(?range))]
    pub fn truncate_range(&self, range: RangeInclusive<BlockIdShort>) {
        tracing::debug!(target: tracing_targets::VALIDATOR, "truncate_range");
        self.committed.rcu(|cur| {
            let mut new = (**cur).clone();
            new.retain(|&blk, _| !range.contains(&blk));
            Arc::new(new)
        });
    }
}

// Implementation of the `ValidationEvents` trait for the collector

impl ValidationEvents for ValidationEventCollector {
    #[instrument(skip(self), fields(?ctx))]
    fn on_session_open(&self, ctx: &SessionCtx) -> Result<()> {
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_session_open");
        match self.pending.entry(ctx.session_id) {
            DashMapEntry::Occupied(_) => Err(SessionAlreadyExists(ctx.session_id).into()),
            DashMapEntry::Vacant(v) => {
                v.insert(FastDashMap::default());
                Ok(())
            }
        }
    }

    #[instrument(skip(self), fields(?ctx))]
    fn on_session_drop(&self, ctx: &SessionCtx) -> Result<()> {
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_session_drop");
        if self.pending.remove(&ctx.session_id).is_some() {
            Ok(())
        } else {
            Err(SessionNotFound(ctx.session_id).into())
        }
    }

    #[instrument(skip(self), fields(?ev))]
    fn on_signature_event(&self, ev: &SignatureEvent) -> Result<()> {
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_signature_event");
        let session = self
            .pending
            .get(&ev.ctx.session_id)
            .ok_or(SessionNotFound(ev.ctx.session_id))?;

        let bucket = session
            .entry(ev.block_id_short)
            .or_insert_with(FastDashMap::default);

        match bucket.entry(ev.peer_id) {
            DashMapEntry::Occupied(mut e)
                if *e.get() == SigStatus::Invalid && ev.status == SigStatus::Valid =>
            {
                e.insert(SigStatus::Valid);
            }
            DashMapEntry::Vacant(e) => {
                e.insert(ev.status);
            }
            DashMapEntry::Occupied(_) => {
                // not inserting if the status is already set
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(?ctx, ?block_id_short))]
    fn on_validation_skipped(&self, ctx: &SessionCtx, block_id_short: BlockIdShort) -> Result<()> {
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_validation_skipped");
        let session = self
            .pending
            .get(&ctx.session_id)
            .ok_or(SessionNotFound(ctx.session_id))?;

        session.remove(&block_id_short);
        Ok(())
    }

    #[instrument(skip(self), fields(?ctx, ?block_id_short))]
    fn on_validation_complete(&self, ctx: &SessionCtx, block_id_short: BlockIdShort) -> Result<()> {
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_validation_complete");
        let session = self
            .pending
            .get(&ctx.session_id)
            .ok_or(SessionNotFound(ctx.session_id))?;

        let Some((_k, bucket)) = session.remove(&block_id_short) else {
            tracing::debug!(target: tracing_targets::VALIDATOR, "no signatures for block, skipping");
            return Ok(());
        };

        let peer_map: PeerMap = bucket.iter().map(|e| (*e.key(), *e.value())).collect();
        let arc_pm = Arc::new(peer_map);

        self.committed.rcu(|cur| {
            let mut new = (**cur).clone();
            if new.insert(block_id_short, arc_pm.clone()).is_some() {
                tracing::error!(target: tracing_targets::VALIDATOR,
                               %block_id_short, "block already present in committed");
            }
            Arc::new(new)
        });
        Ok(())
    }
}

// ---------- tests ----------

#[cfg(test)]
mod tests {
    use tycho_types::models::ShardIdent;

    use super::*;
    use crate::validator::event::SigStatus::{Invalid, Valid};

    const S: ValidationSessionId = (1, 1);

    fn ctx() -> SessionCtx {
        SessionCtx { session_id: S }
    }

    #[test]

    fn basic_flow() {
        let c = ValidationEventCollector::default();
        // open
        c.on_session_open(&ctx()).unwrap();

        let peer1 = PeerId([1u8; 32]);
        let peer2 = PeerId([2u8; 32]);

        let block_id_short = BlockIdShort::from((ShardIdent::default(), 10));
        // signatures (block 10, peers A/B)
        let ev_a = SignatureEvent {
            ctx: ctx(),
            block_id_short,
            peer_id: peer1,
            status: Valid,
        };
        let ev_b = SignatureEvent {
            ctx: ctx(),
            block_id_short,
            peer_id: peer2,
            status: Invalid,
        };
        c.on_signature_event(&ev_a).unwrap();
        c.on_signature_event(&ev_b).unwrap();

        // complete block
        c.on_validation_complete(&ctx(), block_id_short).unwrap();

        // stats over exact block
        let stats = c.stats_for_blocks(block_id_short..=block_id_short);
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[&peer1].valid, 1);
        assert_eq!(stats[&peer2].invalid, 1);

        // truncate that block
        c.truncate_range(BlockIdShort::from((block_id_short.shard, 0))..=block_id_short);
        let stats = c.stats_for_blocks(
            BlockIdShort::from((block_id_short.shard, 0))
                ..=BlockIdShort::from((block_id_short.shard, 20)),
        );
        assert!(stats.is_empty());
    }

    #[test]
    fn skip_validation_drops_bucket() {
        let peer1 = PeerId([1u8; 32]);

        let block_id_short = BlockIdShort::from((ShardIdent::default(), 10));

        let c = ValidationEventCollector::default();
        c.on_session_open(&ctx()).unwrap();
        let ev = SignatureEvent {
            ctx: ctx(),
            block_id_short,
            peer_id: peer1,
            status: Valid,
        };
        c.on_signature_event(&ev).unwrap();
        // skip -> bucket removed, no stats
        c.on_validation_skipped(&ctx(), block_id_short).unwrap();
        c.on_validation_complete(&ctx(), block_id_short).unwrap(); // nothing happens
        assert!(
            c.stats_for_blocks(
                BlockIdShort::from((block_id_short.shard, 0))
                    ..=BlockIdShort::from((block_id_short.shard, 10))
            )
            .is_empty()
        );
    }

    #[test]

    fn duplicate_session_errors() {
        let c = ValidationEventCollector::default();
        assert!(c.on_session_open(&ctx()).is_ok());
        assert!(c.on_session_open(&ctx()).is_err());
        assert!(c.on_session_drop(&ctx()).is_ok());
        assert!(c.on_session_drop(&ctx()).is_err());
    }
}
