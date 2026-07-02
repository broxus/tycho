use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::instrument;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{
    AnchorStats, ReceivedSignature, ValidationSessionId, ValidatorEventsListener,
};
use tycho_types::models::{BlockId, BlockIdShort, IndexedValidatorDescription};
use tycho_types::prelude::*;
use tycho_util::{DashMapEntry, FastDashMap};

use crate::bc::BlocksBatch;

const INIT_QUEUE_CAPACITY: usize = 3;
const METRIC_ACTIVE_SESSIONS: &str = "tycho_slasher_active_sessions";
const METRIC_SESSION_INIT_QUEUE_LEN: &str = "tycho_slasher_session_init_queue_len";

pub trait BlockBatchesStore {
    fn known_batch_size(&self) -> AtomicU32;
}

pub struct ValidatorEventsCollector {
    default_batch_size: AtomicU32,
    sessions: FastDashMap<ValidationSessionId, SessionState>,
    init_queue: Mutex<VecDeque<ValidatorSessionInfo>>,
    init_queue_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct ValidatorSessionInfo {
    pub vset_hash: HashBytes,
    pub session_id: ValidationSessionId,
    pub first_mc_seqno: u32,
    pub own_validator_idx: u16,
    pub validators: Arc<[IndexedValidatorDescription]>,
}

struct SessionState {
    batch_size: NonZeroU32,
    /// Maps each subset item with its original vset index.
    validator_indices: Box<[u16]>,
    current_batch: BlocksBatch,
    /// Imported anchors can arrive one batch window before block callbacks rotate the session.
    next_batch: BlocksBatch,
    first_seqno: u32,
    next_expected_seqno: u32,
    complete_batches: Option<mpsc::UnboundedSender<BlocksBatch>>,
}

pub type BlocksBatchTx = mpsc::UnboundedSender<BlocksBatch>;
pub type BlocksBatchRx = mpsc::UnboundedReceiver<BlocksBatch>;

// === Collector impl ===

impl ValidatorEventsCollector {
    pub fn new(default_batch_size: NonZeroU32) -> Self {
        let init_queue_capacity = INIT_QUEUE_CAPACITY;
        let init_queue = Mutex::new(VecDeque::with_capacity(init_queue_capacity));
        metrics::gauge!(METRIC_ACTIVE_SESSIONS).set(0);
        metrics::gauge!(METRIC_SESSION_INIT_QUEUE_LEN).set(0);

        Self {
            default_batch_size: AtomicU32::new(default_batch_size.get()),
            sessions: Default::default(),
            init_queue,
            init_queue_capacity,
        }
    }

    pub fn pop_session_to_init(&self, mc_seqno: u32) -> Option<ValidatorSessionInfo> {
        let mut queue = self.init_queue.lock().unwrap();
        if let Some(info) = queue.front()
            && info.first_mc_seqno > mc_seqno
        {
            return None;
        }
        let result = queue.pop_front();
        metrics::gauge!(METRIC_SESSION_INIT_QUEUE_LEN).set(queue.len() as f64);
        result
    }

    fn push_session_to_init(&self, info: ValidatorSessionInfo) {
        let mut items = self.init_queue.lock().unwrap();
        if items.len() >= self.init_queue_capacity
            && let Some(info) = items.pop_front()
        {
            tracing::warn!(
                session_id = ?info.session_id,
                "session info dropped from init queue"
            );
        }
        items.push_back(info);
        metrics::gauge!(METRIC_SESSION_INIT_QUEUE_LEN).set(items.len() as f64);
    }

    pub fn set_default_batch_size(&self, batch_size: NonZeroU32) {
        self.default_batch_size
            .store(batch_size.get(), Ordering::Release);

        for mut session in self.sessions.iter_mut() {
            // TODO: Split or grow the previous batch to not discard events.
            if session.batch_size != batch_size {
                session.batch_size = batch_size;
                session.current_batch = BlocksBatch::new(
                    session.align_seqno(session.next_expected_seqno),
                    batch_size,
                    &session.validator_indices,
                );
            }
        }
    }

    pub fn init_session(
        &self,
        session_id: ValidationSessionId,
        batch_size: NonZeroU32,
        complete_batches: BlocksBatchTx,
    ) -> bool {
        let Some(mut session) = self.sessions.get_mut(&session_id) else {
            return false;
        };

        // Reset the current batch if its size has changed.
        // TODO: Split or grow the previous batch to not discard events.
        if session.batch_size != batch_size {
            session.batch_size = batch_size;
            let next_expected_seqno = session.next_expected_seqno;
            session.reset_batches(next_expected_seqno);
        }

        session.complete_batches = Some(complete_batches);

        true
    }

    pub fn skip_session(&self, session_id: ValidationSessionId) -> bool {
        let removed = self.sessions.remove(&session_id).is_some();
        if removed {
            metrics::gauge!(METRIC_ACTIVE_SESSIONS).set(self.sessions.len() as f64);
        }
        removed
    }
}

impl ValidatorEventsListener for ValidatorEventsCollector {
    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_session_started(
        &self,
        session_id: ValidationSessionId,
        first_mc_seqno: u32,
        own_validator_idx: u16,
        vset_hash: &HashBytes,
        validators: &[IndexedValidatorDescription],
    ) {
        tracing::debug!(first_mc_seqno, "on_session_started");

        let validator_indices = validators
            .iter()
            .map(|item| item.validator_idx)
            .collect::<Box<[_]>>();

        let batch_size = NonZeroU32::new(self.default_batch_size.load(Ordering::Acquire)).unwrap();
        let current_batch = BlocksBatch::new(first_mc_seqno, batch_size, &validator_indices);
        let next_batch =
            BlocksBatch::new(current_batch.seqno_after(), batch_size, &validator_indices);

        let validators = Arc::<[IndexedValidatorDescription]>::from(validators);

        if let DashMapEntry::Vacant(v) = self.sessions.entry(session_id) {
            v.insert(SessionState {
                batch_size,
                validator_indices,
                current_batch,
                next_batch,
                first_seqno: first_mc_seqno,
                next_expected_seqno: first_mc_seqno,
                // Will be initialized later via `init_session`.
                complete_batches: None,
            });

            self.push_session_to_init(ValidatorSessionInfo {
                vset_hash: *vset_hash,
                session_id,
                first_mc_seqno,
                own_validator_idx,
                validators,
            });
            metrics::gauge!(METRIC_ACTIVE_SESSIONS).set(self.sessions.len() as f64);
        } else {
            tracing::warn!("duplicate session");
        }
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_session_finished(&self, session_id: ValidationSessionId) {
        tracing::debug!("on_session_finished");
        if let Some((_, session)) = self.sessions.remove(&session_id)
            && let Err(e) = session.commit_final_batch()
        {
            tracing::warn!("failed to commit blocks batch on finish: {e:?}");
        }
        metrics::gauge!(METRIC_ACTIVE_SESSIONS).set(self.sessions.len() as f64);
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_anchor_import(
        &self,
        session_id: ValidationSessionId,
        block_id: &BlockIdShort,
        anchor_id: u32,
        anchor_stats: AnchorStats,
    ) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        tracing::debug!(
            %block_id,
            "on_anchor_import"
        );
        let Some(mut session) = self.sessions.get_mut(&session_id) else {
            tracing::warn!("session not found, ignoring on_anchor_import event");
            return;
        };
        session.push_anchor_stats(block_id.seqno, anchor_id, &anchor_stats);
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_block_validated(
        &self,
        session_id: ValidationSessionId,
        block_id: &BlockId,
        signatures: Arc<[ReceivedSignature]>,
    ) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        tracing::debug!(%block_id, "on_block_validated");
        let Some(mut session) = self.sessions.get_mut(&session_id) else {
            tracing::warn!("session not found, ignoring on_block_validated event");
            return;
        };
        session.handle_block(block_id.seqno, Some(signatures.as_ref()));
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_block_skipped(&self, session_id: ValidationSessionId, block_id: &BlockId) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        tracing::debug!(%block_id, "on_block_skipped");
        let Some(mut session) = self.sessions.get_mut(&session_id) else {
            tracing::warn!("session not found, ignoring on_block_skipped event");
            return;
        };
        session.handle_block(block_id.seqno, None);
    }
}

// === Validator session info impl ===

impl ValidatorSessionInfo {
    pub fn can_participate(&self, public_key: &ed25519::PublicKey) -> bool {
        let Some(desc) = self
            .validators
            .iter()
            .find(|item| item.validator_idx == self.own_validator_idx)
        else {
            return false;
        };

        public_key.as_bytes() == desc.public_key.as_array()
    }
}

// === Session state impl ===

impl SessionState {
    fn push_anchor_stats(
        &mut self,
        seqno: u32,
        anchor_id: u32,
        anchor_stats: &AnchorStats,
    ) -> bool {
        if self.current_batch.contains_seqno(seqno) {
            (self.current_batch).push_anchor_stats(anchor_id, anchor_stats)
        } else if self.next_batch.contains_seqno(seqno) {
            (self.next_batch).push_anchor_stats(anchor_id, anchor_stats)
        } else {
            tracing::warn!(
                anchor_id,
                seqno,
                current_batch_seqnos = ?self.current_batch.seqno_range(),
                next_batch_seqnos = ?self.next_batch.seqno_range(),
                "anchor import outside block batches"
            );
            false
        }
    }

    fn handle_block(&mut self, seqno: u32, signatures: Option<&[ReceivedSignature]>) -> bool {
        let batches = match self.try_advance_current_batch(seqno) {
            AdvanceBlockStatus::TooOld => return false,
            AdvanceBlockStatus::Unchanged => [None, None],
            AdvanceBlockStatus::Rotated { first, second } => [Some(first), second],
        };

        let event_type = match signatures {
            Some(signatures) => {
                self.current_batch.commit_signatures(seqno, signatures);
                "validated"
            }
            None => "skipped",
        };

        for (batch, ith) in batches.into_iter().flatten().zip(["1st", "2nd"]) {
            if let Err(e) = self.commit_batch(batch) {
                tracing::error!(event_type, "{ith} blocks batch failed to commit: {e:?}");
            }
        }
        true
    }

    fn try_advance_current_batch(&mut self, seqno: u32) -> AdvanceBlockStatus {
        if seqno < self.next_expected_seqno {
            return AdvanceBlockStatus::TooOld;
        } else if self.current_batch.contains_seqno(seqno) {
            return AdvanceBlockStatus::Unchanged;
        }

        self.next_expected_seqno = seqno + 1;

        if self.next_batch.contains_seqno(seqno) {
            let next = self.make_batch(self.next_batch.seqno_after());
            let current = std::mem::replace(&mut self.next_batch, next);

            AdvanceBlockStatus::Rotated {
                first: std::mem::replace(&mut self.current_batch, current),
                second: None,
            }
        } else {
            self.reset_batches(seqno)
        }
    }

    fn commit_batch(&self, batch: BlocksBatch) -> Result<()> {
        Self::commit_batch_impl(&self.complete_batches, batch)
    }

    fn commit_final_batch(self) -> Result<()> {
        Self::commit_batch_impl(&self.complete_batches, self.current_batch)?;
        Self::commit_batch_impl(&self.complete_batches, self.next_batch)
    }

    fn commit_batch_impl(
        complete_batches: &Option<BlocksBatchTx>,
        batch: BlocksBatch,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let Some(tx) = complete_batches else {
            // Something is really broken if ~100 blocks were not enough to initialize session.
            anyhow::bail!("not initialized");
        };

        if tx.send(batch).is_err() {
            anyhow::bail!("channel closed");
        }
        Ok(())
    }

    fn reset_batches(&mut self, seqno: u32) -> AdvanceBlockStatus {
        let current = self.make_batch(self.align_seqno(seqno));
        let next = self.make_batch(current.seqno_after());

        AdvanceBlockStatus::Rotated {
            first: std::mem::replace(&mut self.current_batch, current),
            second: Some(std::mem::replace(&mut self.next_batch, next)),
        }
    }

    fn make_batch(&self, start_seqno: u32) -> BlocksBatch {
        BlocksBatch::new(start_seqno, self.batch_size, &self.validator_indices)
    }

    fn align_seqno(&self, seqno: u32) -> u32 {
        assert!(seqno >= self.first_seqno);

        // Example:
        // batch_size = 100
        // first_seqno = 101
        // seqno = 250
        // result = 250 - (250 - 101) % 100 = 201
        seqno - (seqno - self.first_seqno) % self.batch_size.get()
    }
}

enum AdvanceBlockStatus {
    TooOld,
    Unchanged,
    Rotated {
        first: BlocksBatch,
        second: Option<BlocksBatch>,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tycho_slasher_traits::AnchorPeerStats;
    use tycho_types::models::{ShardIdent, ValidatorDescription};

    use super::*;

    const FIRST_SEQNO: u32 = 100;
    const BATCH_SIZE: u32 = 3;

    #[test]
    fn next_window_anchor_import_survives_rotation() {
        let (collector, mut rx, session_id) = make_collector();

        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO + BATCH_SIZE),
            10,
            stats(&[51, 52, 53]),
        );
        assert!(drain_batches(&mut rx).is_empty());

        collector.on_block_skipped(session_id, &master(FIRST_SEQNO + BATCH_SIZE));
        assert!(drain_batches(&mut rx).is_empty());

        collector.on_session_finished(session_id);
        let batches = drain_batches(&mut rx);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].start_seqno, FIRST_SEQNO + BATCH_SIZE);
        assert_eq!(batches[0].anchor_range, Some(10..=10));
        assert_eq!(points(&batches[0]), vec![(0, 51), (1, 52), (2, 53)]);
    }

    #[test]
    fn final_flush_commits_current_and_next_anchor_batches() {
        let (collector, mut rx, session_id) = make_collector();

        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO),
            10,
            stats(&[51, 52, 53]),
        );
        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO + BATCH_SIZE),
            11,
            stats(&[61, 62, 63]),
        );

        collector.on_session_finished(session_id);
        let batches = drain_batches(&mut rx);

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].start_seqno, FIRST_SEQNO);
        assert_eq!(batches[0].anchor_range, Some(10..=10));
        assert_eq!(points(&batches[0]), vec![(0, 51), (1, 52), (2, 53)]);
        assert_eq!(batches[1].start_seqno, FIRST_SEQNO + BATCH_SIZE);
        assert_eq!(batches[1].anchor_range, Some(11..=11));
        assert_eq!(points(&batches[1]), vec![(0, 61), (1, 62), (2, 63)]);
    }

    #[test]
    fn far_jump_flushes_old_next_batch_before_reset() {
        let (collector, mut rx, session_id) = make_collector();

        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO + BATCH_SIZE),
            10,
            stats(&[51, 52, 53]),
        );

        collector.on_block_skipped(session_id, &master(FIRST_SEQNO + 3 * BATCH_SIZE));
        let batches = drain_batches(&mut rx);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].start_seqno, FIRST_SEQNO + BATCH_SIZE);
        assert_eq!(batches[0].anchor_range, Some(10..=10));
        assert_eq!(points(&batches[0]), vec![(0, 51), (1, 52), (2, 53)]);
    }

    #[test]
    fn duplicate_anchor_import_is_not_counted_twice() {
        let (collector, mut rx, session_id) = make_collector();

        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO),
            10,
            stats(&[51, 52, 53]),
        );
        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO),
            10,
            stats(&[61, 62, 63]),
        );

        collector.on_session_finished(session_id);
        let batches = drain_batches(&mut rx);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].anchor_range, Some(10..=10));
        assert_eq!(points(&batches[0]), vec![(0, 51), (1, 52), (2, 53)]);
    }

    #[test]
    fn anchor_import_outside_live_windows_is_rejected() {
        let (collector, mut rx, session_id) = make_collector();

        collector.on_anchor_import(
            session_id,
            &master_short(FIRST_SEQNO + 2 * BATCH_SIZE),
            10,
            stats(&[51, 52, 53]),
        );

        collector.on_session_finished(session_id);

        assert!(drain_batches(&mut rx).is_empty());
    }

    fn master(seqno: u32) -> BlockId {
        BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        }
    }

    fn master_short(seqno: u32) -> BlockIdShort {
        BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno,
        }
    }

    fn stats(points: &[u16]) -> AnchorStats {
        AnchorStats(Arc::from(
            points
                .iter()
                .copied()
                .map(|points_proven| AnchorPeerStats { points_proven })
                .collect::<Vec<_>>(),
        ))
    }

    fn drain_batches(rx: &mut BlocksBatchRx) -> Vec<BlocksBatch> {
        let mut result = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            result.push(batch);
        }
        result
    }

    fn points(batch: &BlocksBatch) -> Vec<(u16, u16)> {
        (batch.observed)
            .iter()
            .map(|item| (item.validator_idx, item.points_proven))
            .collect()
    }

    fn make_collector() -> (ValidatorEventsCollector, BlocksBatchRx, ValidationSessionId) {
        let collector = ValidatorEventsCollector::new(NonZeroU32::new(BATCH_SIZE).unwrap());
        let session_id = ValidationSessionId {
            catchain_seqno: 1,
            vset_switch_round: 2,
        };
        let validators = make_validators();

        collector.on_session_started(session_id, FIRST_SEQNO, 0, &HashBytes::ZERO, &validators);

        let (tx, rx) = mpsc::unbounded_channel();
        assert!(collector.init_session(session_id, NonZeroU32::new(BATCH_SIZE).unwrap(), tx,));

        (collector, rx, session_id)
    }

    fn make_validators() -> Vec<IndexedValidatorDescription> {
        [2, 0, 1]
            .into_iter()
            .scan(0, |prev_total_weight, validator_idx| {
                let desc = ValidatorDescription {
                    public_key: HashBytes([validator_idx as u8 + 1; 32]),
                    weight: 1,
                    adnl_addr: None,
                    mc_seqno_since: FIRST_SEQNO,
                    prev_total_weight: *prev_total_weight,
                };
                *prev_total_weight += desc.weight;

                Some(IndexedValidatorDescription {
                    desc,
                    validator_idx,
                })
            })
            .collect()
    }
}
