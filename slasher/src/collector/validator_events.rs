use std::collections::BTreeMap;

use parking_lot::Mutex;
use tracing::instrument;
use tycho_network::PeerId;
use tycho_slasher_traits::{
    SessionStartedEvent, SignatureStatus, ValidationEvent, ValidationSessionId,
    ValidatorEventsListener,
};
use tycho_types::models::BlockId;
use tycho_util::{DashMapEntry, FastDashMap};

pub struct SessionStats {
    pub session_id: ValidationSessionId,
    pub blocks: Mutex<BTreeMap<u32, CollectedBlockSignatures>>,
}

pub struct CollectedBlockSignatures {
    pub block_id: BlockId,
    // TODO: Replace `PeerId` with a validator index.
    pub peer_signatures: FastDashMap<PeerId, SignatureStatus>,
}

#[derive(Default)]
pub struct ValidatorEventsCollector {
    pending: FastDashMap<ValidationSessionId, PendingBlockSignatures>,
}

type PendingBlockSignatures = FastDashMap<BlockId, FastDashMap<PeerId, SignatureStatus>>;

impl ValidatorEventsListener for ValidatorEventsCollector {
    #[instrument(skip_all, fields(session_id = ?event.session_id))]
    fn on_session_started(&self, event: SessionStartedEvent<'_>) {
        tracing::debug!("on_session_open");
        if let DashMapEntry::Vacant(v) = self.pending.entry(event.session_id) {
            v.insert(Default::default());
        } else {
            tracing::warn!("duplicate session");
        }
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_session_dropped(&self, sid: ValidationSessionId) {
        tracing::debug!("on_session_drop");
        self.pending.remove(&sid);
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_validation_started(&self, sid: ValidationSessionId, block_id: &BlockId) {
        tracing::debug!(%block_id, "on_validation_started");
    }

    #[instrument(skip_all, fields(session_id = ?event.session_id))]
    fn on_validation_event(&self, event: ValidationEvent<'_>) {
        tracing::debug!(?event, "on_validation_event");
        let Some(session) = self.pending.get(&event.session_id) else {
            tracing::warn!("session not found, ignoring signature event");
            return;
        };

        let bucket = session
            .entry(*event.block_id)
            .or_insert_with(FastDashMap::default);

        // The validator requests another validator before obtaining a valid signature,
        // so if we already have a valid signature from another validator,
        // we will not get an invalid signature because we will stop requesting
        // the signature from that validator
        match bucket.entry(*event.peer_id) {
            DashMapEntry::Vacant(e) => {
                e.insert(event.signature_status);
            }
            DashMapEntry::Occupied(mut e) => {
                if *e.get() == SignatureStatus::Invalid
                    && event.signature_status == SignatureStatus::Valid
                {
                    e.insert(SignatureStatus::Valid);
                }
            }
        };
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_validation_skipped(&self, sid: ValidationSessionId, block_id: &BlockId) {
        tracing::debug!(%block_id, "on_validation_skipped");
        if let Some(session) = self.pending.get(&sid) {
            session.remove(block_id);
        } else {
            tracing::warn!("session not found, skipping validation_skipped event");
        }
    }

    #[instrument(skip_all, fields(session_id = ?sid))]
    fn on_validation_complete(&self, sid: ValidationSessionId, block_id: &BlockId) {
        tracing::debug!(%block_id, "on_validation_complete");
        let Some(session) = self.pending.get(&sid) else {
            tracing::warn!("session not found, ignoring validation_complete event");
            return;
        };

        let Some((_k, _bucket)) = session.remove(block_id) else {
            tracing::warn!("no signatures found for a complete session");
            return;
        };

        // let valid_sigs: u32 = stats.values().map(|stat| stat.valid).sum();
        // let invalid_sigs: u32 = stats.values().map(|stat| stat.invalid).sum();

        // let labels: [(&str, String); 1] = [("workchain", block_id.shard.workchain().to_string())];

        // metrics::gauge!("tycho_validator_collector_valid_sigs_total_count", &labels)
        //     .set(valid_sigs as f64);

        // metrics::gauge!(
        //     "tycho_validator_collector_invalid_sigs_total_count",
        //     &labels
        // )
        // .set(invalid_sigs as f64);
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
