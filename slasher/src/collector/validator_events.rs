use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use anyhow::Result;
use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use tracing::instrument;
use tycho_slasher_traits::{ReceivedSignature, ValidationSessionId, ValidatorEventsListener};
use tycho_types::dict;
use tycho_types::models::{BlockId, ValidatorDescription};
use tycho_types::prelude::*;
use tycho_util::{DashMapEntry, FastDashMap, FastHashMap};

use crate::util::AtomicBitSet;

// Gauges
const METRIC_SLASHER_PENDING_BLOCKS: &str = "tycho_slasher_pending_blocks";
const METRIC_SLASHER_COMPLETE_BLOCKS: &str = "tycho_slasher_complete_blocks";
const METRIC_SLASHER_LATEST_COMPLETE_BLOCK: &str = "tycho_slasher_latest_complete_block";
const METRIC_SLASHER_BLOCKS_TAKEN_UNTIL: &str = "tycho_slasher_blocks_taken_until";

#[derive(Default)]
pub struct ValidatorEventsCollector {
    default_batch_size: AtomicUsize,
    sessions: FastDashMap<ValidationSessionId, SessionState>,
}

struct SessionState {
    batch_size: usize,
    validator_count: usize,
    current_batch: ArcSwap<BlocksBatch>,
    latest_seqno: AtomicU32,
    complete_batches: Option<mpsc::Sender<Cell>>,
}

struct BlocksBatch {
    start_seqno: u32,
    committed_blocks: AtomicBitSet,
    signatures_history: Box<[AtomicBitSet]>,
}

// === Collector impl ===

impl ValidatorEventsCollector {
    pub fn new(default_batch_size: usize) -> Self {
        Self {
            default_batch_size: AtomicUsize::new(default_batch_size),
            sessions: Default::default(),
        }
    }

    pub fn set_default_batch_size(&self, batch_size: usize) {
        self.default_batch_size.store(batch_size, Ordering::Release);
    }

    pub fn init_session(
        &self,
        session_id: ValidationSessionId,
        batch_size: usize,
        complete_batches: mpsc::Sender<Cell>,
    ) -> bool {
        let Some(mut session) = self.sessions.get_mut(&session_id) else {
            return false;
        };

        // Reset the current batch if its size has changed.
        // TODO: Split or grow the previous batch to not discard events.
        if session.batch_size != batch_size {
            session.batch_size = batch_size;
            session.current_batch.store(Arc::new(BlocksBatch::new(
                session.latest_seqno.load(Ordering::Acquire),
                batch_size,
                session.validator_count,
            )));
        }

        session.complete_batches = Some(complete_batches);

        true
    }

    pub fn skip_session(&self, session_id: ValidationSessionId) -> bool {
        self.sessions.remove(&session_id).is_some()
    }
}

impl ValidatorEventsListener for ValidatorEventsCollector {
    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_session_started(
        &self,
        session_id: ValidationSessionId,
        first_mc_seqno: u32,
        validators: &[ValidatorDescription],
    ) {
        tracing::debug!(first_mc_seqno, "on_session_open");

        let validator_count = validators.len();
        let mut peer_id_to_index =
            FastHashMap::with_capacity_and_hasher(validator_count, Default::default());
        let mut peer_ids = Vec::with_capacity(validator_count);
        for validator in validators {
            if peer_id_to_index
                .insert(validator.public_key, peer_ids.len())
                .is_none()
            {
                peer_ids.push(validator.public_key);
            }
        }

        if let DashMapEntry::Vacant(v) = self.pending.entry(session_id) {
            v.insert(PendingBlocks {
                peer_ids: Arc::from(peer_ids),
                peer_id_to_index,
                pending_blocks: Default::default(),
            });
        } else {
            tracing::warn!("duplicate session");
        }
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_session_finished(&self, session_id: ValidationSessionId) {
        tracing::debug!("on_session_drop");
        if let Some((_, entry)) = self.pending.remove(&session_id) {
            let removed_count = entry.pending_blocks.len();
            metrics::gauge!(METRIC_SLASHER_PENDING_BLOCKS).decrement(removed_count as f64);
        }
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

        scopeguard::defer! {
            self.update_latest_complete_block_seqno(block_id.seqno);
        }

        tracing::debug!(%block_id, "on_validation_complete");
        let Some(session) = self.pending.get(&session_id) else {
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
            session_id,
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

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_block_skipped(&self, session_id: ValidationSessionId, block_id: &BlockId) {
        if !block_id.is_masterchain() {
            // Ignore for non-masterchain blocks (just in case).
            return;
        }

        scopeguard::defer! {
            self.update_latest_complete_block_seqno(block_id.seqno);
        }

        tracing::debug!(%block_id, "on_validation_skipped");
        let Some(session) = self.pending.get(&session_id) else {
            tracing::warn!("session not found, skipping validation_skipped event");
            return;
        };

        let was_pending = session.pending_blocks.remove(block_id).is_some();
        drop(session);

        if was_pending {
            metrics::gauge!(METRIC_SLASHER_PENDING_BLOCKS).decrement(1);
        }
    }
}

// === Blocks batch impl ===

impl BlocksBatch {
    fn new(start_seqno: u32, len: usize, validator_count: usize) -> Self {
        Self {
            start_seqno,
            committed_blocks: AtomicBitSet::with_capacity(len),
            signatures_history: (0..validator_count)
                .into_iter()
                .map(|_| AtomicBitSet::with_capacity(len * 2))
                .collect::<Box<[_]>>(),
        }
    }

    pub fn start_seqno(&self) -> u32 {
        self.start_seqno
    }

    pub fn seqno_after(&self) -> u32 {
        self.start_seqno
            .saturating_add(self.committed_blocks.len() as u32)
    }

    pub fn contains_seqno(&self, seqno: u32) -> bool {
        (self.start_seqno..self.seqno_after()).contains(&seqno)
    }

    fn commit_signatures(
        &mut self,
        mut seqno: u32,
        signatures: &[ReceivedSignature],
    ) -> Result<()> {
        anyhow::ensure!(
            self.contains_seqno(seqno),
            "seqno is out of range: got {seqno}, expected {}..{}",
            self.start_seqno,
            self.seqno_after(),
        );
        anyhow::ensure!(
            signatures.len() == self.signatures_history.len(),
            "signature count mismatch: got {}, expected {}",
            signatures.len(),
            self.signatures_history.len(),
        );
        seqno -= self.start_seqno;

        self.committed_blocks.set(seqno as usize, true);
        for (history, received) in std::iter::zip(&mut self.signatures_history, signatures) {
            let idx = (seqno as usize) * 2;
            history.set(idx, received.has_invalid_signature());
            history.set(idx + 1, received.has_valid_signature());
        }

        Ok(())
    }

    fn build_cell(&self) -> Result<Cell, tycho_types::error::Error> {
        let cx = Cell::empty_context();
        let mut b = CellBuilder::new();
        b.store_u32(self.start_seqno)?;
        self.committed_blocks.store_into(&mut b, cx)?;

        let Some(dict_root) = dict::build_dict_from_sorted_iter(
            self.signatures_history
                .iter()
                .enumerate()
                .map(|(idx, bitset)| (idx as u16, bitset)),
            cx,
        )?
        else {
            return Err(tycho_types::error::Error::InvalidData);
        };
        b.store_reference(dict_root)?;
        b.build_ext(cx)
    }
}
