use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::instrument;
use tycho_slasher_traits::{
    SessionStartedEvent, SignatureStatus, ValidationEvent, ValidationSessionId,
    ValidatorEventsListener,
};
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, ShardIdent};
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap};

// Gauges
const METRIC_SLASHER_PENDING_BLOCKS: &str = "tycho_slasher_pending_blocks";
const METRIC_SLASHER_COMPLETE_BLOCKS: &str = "tycho_slasher_complete_blocks";
const METRIC_SLASHER_LATEST_COMPLETE_BLOCK: &str = "tycho_slasher_latest_complete_block";
const METRIC_SLASHER_BLOCKS_TAKEN_UNTIL: &str = "tycho_slasher_blocks_taken_until";

#[derive(Default)]
pub struct ValidatorEventsCollector {
    latest_complete_block: watch::Sender<u32>,
    pending: FastDashMap<ValidationSessionId, PendingBlocks>,
    complete: Mutex<BTreeMap<u32, CompleteBlock>>,
}

struct PendingBlocks {
    peer_ids: Arc<[HashBytes]>,
    peer_id_to_index: FastHashMap<HashBytes, usize>,
    pending_blocks: FastDashMap<BlockId, PendingBlock>,
}

struct PendingBlock {
    peer_signatures: Box<[AtomicSignatureState]>,
}

pub struct CompleteBlock {
    pub seqno: u32,
    pub root_hash: HashBytes,
    pub file_hash: HashBytes,
    pub session_id: ValidationSessionId,
    pub peer_ids: Arc<[HashBytes]>,
    pub peer_signatures: Box<[SignatureState]>,
}

#[derive(Default)]
#[repr(transparent)]
struct AtomicSignatureState(AtomicU8);

#[repr(transparent)]
pub struct SignatureState(u8);

// === Collector Impl ===

impl ValidatorEventsCollector {
    pub fn subscribe_to_latest_block_seqno(&self) -> watch::Receiver<u32> {
        self.latest_complete_block.subscribe()
    }

    /// Reads all complete blocks including the `until_seqno`.
    pub fn take_batch(&self, until_seqno: u32, buffer: &mut Vec<CompleteBlock>) {
        let mut complete = self.complete.lock();

        let consumed = if until_seqno == u32::MAX {
            std::mem::take(&mut *complete)
        } else {
            let remaining = complete.split_off(&(until_seqno + 1));
            std::mem::replace(&mut *complete, remaining)
        };
        metrics::gauge!(METRIC_SLASHER_COMPLETE_BLOCKS).set(complete.len() as f64);
        drop(complete);

        metrics::gauge!(METRIC_SLASHER_BLOCKS_TAKEN_UNTIL).set(until_seqno);

        buffer.extend(consumed.into_values());
    }

    fn update_latest_complete_block_seqno(&self, seqno: u32) {
        self.latest_complete_block.send_if_modified(|value| {
            let changed = *value < seqno;
            if changed {
                *value = seqno;
                metrics::gauge!(METRIC_SLASHER_LATEST_COMPLETE_BLOCK).set(seqno);
            }
            changed
        });
    }
}

impl ValidatorEventsListener for ValidatorEventsCollector {
    #[instrument(skip_all, fields(session_id = ?event.session_id))]
    fn on_session_started(&self, event: SessionStartedEvent<'_>) {
        tracing::debug!("on_session_open");

        let validator_count = event.validators.len();
        let mut peer_id_to_index =
            FastHashMap::with_capacity_and_hasher(validator_count, Default::default());
        let mut peer_ids = Vec::with_capacity(validator_count);
        for validator in event.validators {
            if peer_id_to_index
                .insert(validator.public_key, peer_ids.len())
                .is_none()
            {
                peer_ids.push(validator.public_key);
            }
        }

        if let DashMapEntry::Vacant(v) = self.pending.entry(event.session_id) {
            v.insert(PendingBlocks {
                peer_ids: Arc::from(peer_ids),
                peer_id_to_index,
                pending_blocks: Default::default(),
            });
        } else {
            tracing::warn!("duplicate session");
        }
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_session_dropped(&self, sid: ValidationSessionId) {
        tracing::debug!("on_session_drop");
        if let Some((_, entry)) = self.pending.remove(&sid) {
            let removed_count = entry.pending_blocks.len();
            metrics::gauge!(METRIC_SLASHER_PENDING_BLOCKS).decrement(removed_count as f64);
        }
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_validation_started(&self, sid: ValidationSessionId, block_id: &BlockId) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        tracing::debug!(%block_id, "on_validation_started");
        let Some(session) = self.pending.get(&sid) else {
            tracing::warn!("session not found, ignoring new validation start");
            return;
        };

        let mut vec = Vec::with_capacity(session.peer_ids.len());
        vec.resize_with(session.peer_ids.len(), AtomicSignatureState::default);

        if let DashMapEntry::Vacant(v) = session.pending_blocks.entry(*block_id) {
            v.insert(PendingBlock {
                peer_signatures: vec.into_boxed_slice(),
            });
            metrics::gauge!(METRIC_SLASHER_PENDING_BLOCKS).increment(1);
        } else {
            tracing::warn!("duplicate block validation");
        }
    }

    #[instrument(skip_all, fields(session_id = ?event.session_id))]
    fn on_validation_event(&self, event: ValidationEvent<'_>) {
        if !event.block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        tracing::debug!(?event, "on_validation_event");
        let Some(session) = self.pending.get(&event.session_id) else {
            tracing::warn!("session not found, ignoring signature event");
            return;
        };

        let Some(idx) = session.peer_id_to_index.get(event.peer_id) else {
            tracing::warn!(
                block_id = %event.block_id,
                peer_id = %event.peer_id,
                "unknown peer id for validation session"
            );
            return;
        };

        let Some(block) = session.pending_blocks.get(event.block_id) else {
            tracing::warn!(
                block_id = %event.block_id,
                "no pending validation for the block"
            );
            return;
        };

        block.peer_signatures[*idx].receive(event.signature_status);
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_validation_skipped(&self, sid: ValidationSessionId, block_id: &BlockId) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        scopeguard::defer! {
            self.update_latest_complete_block_seqno(block_id.seqno);
        }

        tracing::debug!(%block_id, "on_validation_skipped");
        let Some(session) = self.pending.get(&sid) else {
            tracing::warn!("session not found, skipping validation_skipped event");
            return;
        };

        let was_pending = session.pending_blocks.remove(block_id).is_some();
        drop(session);

        if was_pending {
            metrics::gauge!(METRIC_SLASHER_PENDING_BLOCKS).decrement(1);
        }
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_validation_complete(&self, sid: ValidationSessionId, block_id: &BlockId) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        scopeguard::defer! {
            self.update_latest_complete_block_seqno(block_id.seqno);
        }

        tracing::debug!(%block_id, "on_validation_complete");
        let Some(session) = self.pending.get(&sid) else {
            tracing::warn!("session not found, ignoring validation_complete event");
            return;
        };

        let Some((_, block)) = session.pending_blocks.remove(block_id) else {
            tracing::warn!("no signatures found for a complete session");
            return;
        };

        let peer_ids = session.peer_ids.clone();
        drop(session);

        metrics::gauge!(METRIC_SLASHER_PENDING_BLOCKS).decrement(1);

        let block = CompleteBlock {
            seqno: block_id.seqno,
            root_hash: block_id.root_hash,
            file_hash: block_id.file_hash,
            session_id: sid,
            peer_ids,
            peer_signatures: AtomicSignatureState::freeze_boxed_slice(block.peer_signatures),
        };

        let mut complete = self.complete.lock();

        // FIXME: Is this really needed? Can we even start validating block from the future first?
        if block_id.seqno <= *self.latest_complete_block.borrow() {
            tracing::info!("skipping an old validation result");
            return;
        }

        complete.insert(block.seqno, block);
    }
}

// === Signature State ===

impl CompleteBlock {
    pub fn full_block_id(&self) -> BlockId {
        BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: self.seqno,
            root_hash: self.root_hash,
            file_hash: self.file_hash,
        }
    }
}

impl AtomicSignatureState {
    fn freeze_boxed_slice(items: Box<[Self]>) -> Box<[SignatureState]> {
        const {
            assert!(std::mem::size_of::<Self>() == std::mem::size_of::<SignatureState>());
            assert!(std::mem::align_of::<Self>() == std::mem::align_of::<SignatureState>());
        }

        let raw = Box::into_raw(items);

        // SAFETY: `Self` has the same layout as `SignatureState`.
        unsafe { Box::from_raw(raw as *mut [SignatureState]) }
    }

    #[inline]
    fn receive(&self, status: SignatureStatus) {
        let flag = match status {
            SignatureStatus::Valid => SignatureState::RECEIVED_VALID_FLAG,
            SignatureStatus::Invalid => SignatureState::RECEIVED_INVALID_FLAG,
        };
        self.0.fetch_or(flag, Ordering::Relaxed);
    }
}

impl SignatureState {
    const RECEIVED_VALID_FLAG: u8 = 0b01;
    const RECEIVED_INVALID_FLAG: u8 = 0b10;

    #[inline]
    pub fn as_u8(&self) -> u8 {
        self.0
    }

    #[inline]
    pub fn has_valid_signature(&self) -> bool {
        self.0 & Self::RECEIVED_VALID_FLAG != 0
    }

    #[inline]
    pub fn has_invalid_signature(&self) -> bool {
        self.0 & Self::RECEIVED_INVALID_FLAG != 0
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     const S: ValidationSessionId = (1, 1);

//     #[test]

//     fn basic_flow() {
//         let c = ValidatorEventsCollector::default();
//         // open
//         c.on_session_started(&ctx()).unwrap();

//         let peer1 = PeerId([1u8; 32]);
//         let peer2 = PeerId([2u8; 32]);

//         let block_id = BlockId {
//             shard: Default::default(),
//             seqno: 10,
//             root_hash: Default::default(),
//             file_hash: Default::default(),
//         };
//         // signatures (block 10, peers A/B)
//         let ev_a = SignatureEvent {
//             ctx: ctx(),
//             block_id,
//             peer_id: peer1,
//             status: Valid,
//         };
//         let ev_b = SignatureEvent {
//             ctx: ctx(),
//             block_id,
//             peer_id: peer2,
//             status: Invalid,
//         };
//         c.on_signature_received(&ev_a).unwrap();
//         c.on_signature_received(&ev_b).unwrap();

//         // complete block
//         c.on_validation_complete(&ctx(), &block_id).unwrap();

//         // stats over exact block
//         let stats = c.stats_for_blocks(..=block_id.as_short_id());
//         assert_eq!(stats.len(), 2);
//         assert_eq!(stats[&peer1].valid, 1);
//         assert_eq!(stats[&peer2].invalid, 1);

//         // truncate that block
//         c.truncate_range(..=block_id.as_short_id());
//         let stats = c.stats_for_blocks(..=BlockIdShort::from((block_id.shard, 20)));
//         assert!(stats.is_empty());
//     }

//     #[test]
//     fn skip_validation_drops_bucket() {
//         let peer1 = PeerId([1u8; 32]);

//         let block_id = BlockId {
//             shard: Default::default(),
//             seqno: 10,
//             root_hash: Default::default(),
//             file_hash: Default::default(),
//         };

//         let c = ValidatorEventMetricsCollector::default();
//         c.on_session_started(&ctx()).unwrap();
//         let ev = SignatureEvent {
//             ctx: ctx(),
//             block_id,
//             peer_id: peer1,
//             status: Valid,
//         };
//         c.on_signature_received(&ev).unwrap();
//         // skip -> bucket removed, no stats
//         c.on_validation_skipped(&ctx(), &block_id).unwrap();
//         c.on_validation_complete(&ctx(), &block_id).unwrap(); // nothing happens
//         assert!(
//             c.stats_for_blocks(..=BlockIdShort::from((block_id.shard, 10)))
//                 .is_empty()
//         );
//     }

//     #[test]

//     fn duplicate_session_errors() {
//         let c = ValidatorEventMetricsCollector::default();
//         assert!(c.on_session_started(&ctx()).is_ok());
//         assert!(c.on_session_started(&ctx()).is_err());
//         assert!(c.on_session_dropped(&ctx()).is_ok());
//         assert!(c.on_session_dropped(&ctx()).is_err());
//     }
// }
