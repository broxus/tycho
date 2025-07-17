use std::collections::{BTreeMap, HashMap};
use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwap;
use tracing::instrument;
use tycho_network::PeerId;
use tycho_types::models::{BlockId, BlockIdShort};
use tycho_util::metrics::HistogramGuard;
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
    pending: FastDashMap<ValidationSessionId, FastDashMap<BlockId, FastDashMap<PeerId, SigStatus>>>,
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
        range: impl RangeBounds<BlockIdShort>,
    ) -> HashMap<PeerId, PeerStat> {
        let _histogram =
            HistogramGuard::begin("tycho_validator_collector_get_stats_for_blocks_time");
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
    pub fn truncate_range(&self, range: impl RangeBounds<BlockIdShort> + std::fmt::Debug) {
        let _histogram = HistogramGuard::begin("tycho_validator_collector_truncate_range_time");
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
        let _histogram = HistogramGuard::begin("tycho_validator_collector_on_signature_event_time");
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_signature_event");
        let Some(session) = self.pending.get(&ev.ctx.session_id) else {
            tracing::warn!(
                target: tracing_targets::VALIDATOR,
                "session not found, ignoring signature event"
            );
            return Ok(());
        };

        let bucket = session
            .entry(ev.block_id)
            .or_insert_with(FastDashMap::default);

        // The validator requests another validator before obtaining a valid signature,
        // so if we already have a valid signature from another validator,
        // we will not get an invalid signature because we will stop requesting
        // the signature from that validator
        match bucket.entry(ev.peer_id) {
            DashMapEntry::Vacant(e) => {
                e.insert(ev.status);
            }
            DashMapEntry::Occupied(mut e) => {
                if *e.get() == SigStatus::Invalid && ev.status == SigStatus::Valid {
                    e.insert(SigStatus::Valid);
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(?ctx, ?block_id))]
    fn on_validation_skipped(&self, ctx: &SessionCtx, block_id: &BlockId) -> Result<()> {
        tracing::debug!(target: tracing_targets::VALIDATOR, "on_validation_skipped");
        if let Some(session) = self.pending.get(&ctx.session_id) {
            session.remove(block_id);
        } else {
            tracing::warn!(
                target: tracing_targets::VALIDATOR,
                "session not found, skipping validation_skipped event"
            );
        }
        Ok(())
    }

    #[instrument(skip(self), fields(?ctx, ?block_id))]
    fn on_validation_complete(&self, ctx: &SessionCtx, block_id: &BlockId) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_validator_collector_on_validation_complete_time");

        tracing::debug!(target: tracing_targets::VALIDATOR, "on_validation_complete");
        let Some(session) = self.pending.get(&ctx.session_id) else {
            tracing::warn!(
                target: tracing_targets::VALIDATOR,
                "session not found, ignoring validation_complete event"
            );
            return Ok(());
        };

        let Some((_k, bucket)) = session.remove(block_id) else {
            tracing::debug!(target: tracing_targets::VALIDATOR, "no signatures for block, skipping");
            return Ok(());
        };

        let peer_map: PeerMap = bucket.iter().map(|e| (*e.key(), *e.value())).collect();
        let arc_pm = Arc::new(peer_map);

        self.committed.rcu(|cur| {
            let mut new = (**cur).clone();
            let block_id_short = block_id.as_short_id();
            if new.insert(block_id_short, arc_pm.clone()).is_some() {
                tracing::error!(target: tracing_targets::VALIDATOR,
                               %block_id_short, "block already present in committed");
            }
            Arc::new(new)
        });

        // TODO: -- DEBUG LOGIC
        // push stats to a metrics collector
        // clean stats every 50 blocks

        if block_id.seqno % 50 == 0 {
            let end = block_id.seqno.saturating_sub(50);
            let end_short = BlockIdShort::from((block_id.shard, end));
            self.truncate_range(..=end_short);
        }

        let end_block_id = BlockIdShort::from((block_id.shard, block_id.seqno + 1));
        let stats = self.stats_for_blocks(BlockIdShort::from((block_id.shard, 0))..=end_block_id);

        let valid_sigs: u32 = stats.values().map(|stat| stat.valid).sum();
        let invalid_sigs: u32 = stats.values().map(|stat| stat.invalid).sum();

        let labels: [(&str, String); 1] = [("workchain", block_id.shard.workchain().to_string())];

        metrics::gauge!("tycho_validator_collector_valid_sigs_total_count", &labels)
            .set(valid_sigs as f64);

        metrics::gauge!(
            "tycho_validator_collector_invalid_sigs_total_count",
            &labels
        )
        .set(invalid_sigs as f64);

        // TODO: -- DEBUG LOGIC END

        Ok(())
    }
}

// ---------- tests ----------

#[cfg(test)]
mod tests {
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

        let block_id = BlockId {
            shard: Default::default(),
            seqno: 10,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };
        // signatures (block 10, peers A/B)
        let ev_a = SignatureEvent {
            ctx: ctx(),
            block_id,
            peer_id: peer1,
            status: Valid,
        };
        let ev_b = SignatureEvent {
            ctx: ctx(),
            block_id,
            peer_id: peer2,
            status: Invalid,
        };
        c.on_signature_event(&ev_a).unwrap();
        c.on_signature_event(&ev_b).unwrap();

        // complete block
        c.on_validation_complete(&ctx(), &block_id).unwrap();

        // stats over exact block
        let stats = c.stats_for_blocks(..=block_id.as_short_id());
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[&peer1].valid, 1);
        assert_eq!(stats[&peer2].invalid, 1);

        // truncate that block
        c.truncate_range(..=block_id.as_short_id());
        let stats = c.stats_for_blocks(..=BlockIdShort::from((block_id.shard, 20)));
        assert!(stats.is_empty());
    }

    #[test]
    fn skip_validation_drops_bucket() {
        let peer1 = PeerId([1u8; 32]);

        let block_id = BlockId {
            shard: Default::default(),
            seqno: 10,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };

        let c = ValidationEventCollector::default();
        c.on_session_open(&ctx()).unwrap();
        let ev = SignatureEvent {
            ctx: ctx(),
            block_id,
            peer_id: peer1,
            status: Valid,
        };
        c.on_signature_event(&ev).unwrap();
        // skip -> bucket removed, no stats
        c.on_validation_skipped(&ctx(), &block_id).unwrap();
        c.on_validation_complete(&ctx(), &block_id).unwrap(); // nothing happens
        assert!(
            c.stats_for_blocks(..=BlockIdShort::from((block_id.shard, 10)))
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
