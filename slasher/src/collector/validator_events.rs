use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tracing::instrument;
use tycho_slasher_traits::{ReceivedSignature, ValidationSessionId, ValidatorEventsListener};
use tycho_types::models::{BlockId, IndexedValidatorDescription};
use tycho_types::prelude::*;
use tycho_util::{DashMapEntry, FastDashMap};

use crate::bc::BlocksBatch;

#[derive(Default)]
pub struct ValidatorEventsCollector {
    default_batch_size: AtomicU32,
    sessions: FastDashMap<ValidationSessionId, SessionState>,
}

struct SessionState {
    batch_size: NonZeroU32,
    /// Maps each subset item with its original vset index.
    validator_indices: Box<[u16]>,
    current_batch: BlocksBatch,
    first_seqno: u32,
    next_expected_seqno: u32,
    complete_batches: Option<mpsc::UnboundedSender<Cell>>,
}

// === Collector impl ===

impl ValidatorEventsCollector {
    pub fn new(default_batch_size: NonZeroU32) -> Self {
        Self {
            default_batch_size: AtomicU32::new(default_batch_size.get()),
            sessions: Default::default(),
        }
    }

    pub fn set_default_batch_size(&self, batch_size: NonZeroU32) {
        self.default_batch_size
            .store(batch_size.get(), Ordering::Release);
    }

    pub fn init_session(
        &self,
        session_id: ValidationSessionId,
        batch_size: NonZeroU32,
        complete_batches: mpsc::UnboundedSender<Cell>,
    ) -> bool {
        let Some(mut session) = self.sessions.get_mut(&session_id) else {
            return false;
        };

        // Reset the current batch if its size has changed.
        // TODO: Split or grow the previous batch to not discard events.
        if session.batch_size != batch_size {
            session.batch_size = batch_size;
            session.current_batch = BlocksBatch::new(
                session.align_seqno(session.next_expected_seqno),
                batch_size,
                &session.validator_indices,
            );
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
        validators: &[IndexedValidatorDescription],
    ) {
        tracing::debug!(first_mc_seqno, "on_session_open");

        let validator_indices = validators
            .iter()
            .map(|item| item.validator_idx)
            .collect::<Box<[_]>>();

        let batch_size = NonZeroU32::new(self.default_batch_size.load(Ordering::Acquire)).unwrap();
        let current_batch = BlocksBatch::new(first_mc_seqno, batch_size, &validator_indices);

        if let DashMapEntry::Vacant(v) = self.sessions.entry(session_id) {
            v.insert(SessionState {
                batch_size,
                validator_indices,
                current_batch,
                first_seqno: first_mc_seqno,
                next_expected_seqno: first_mc_seqno,
                // Will be initialized later via `init_session`.
                complete_batches: None,
            });
        } else {
            tracing::warn!("duplicate session");
        }
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_session_finished(&self, session_id: ValidationSessionId) {
        tracing::debug!("on_session_drop");
        if let Some((_, session)) = self.sessions.remove(&session_id)
            && let Err(e) = session.commit_batch(&session.current_batch)
        {
            tracing::warn!("failed to commit blocks batch on finish: {e:?}");
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

        tracing::debug!(%block_id, "on_validation_complete");
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

// === Session state impl ===

impl SessionState {
    fn handle_block(&mut self, seqno: u32, signatures: Option<&[ReceivedSignature]>) -> bool {
        let to_commit = match self.try_advance_current_batch(seqno) {
            AdvanceBlockStatus::TooOld => return false,
            AdvanceBlockStatus::Unchanged => None,
            AdvanceBlockStatus::Replaced(batch) => Some(batch),
        };

        let event_type = match signatures {
            Some(signatures) => {
                self.current_batch
                    .commit_signatures(seqno, signatures)
                    .expect("ranges must be consistent");
                "validated"
            }
            None => "skipped",
        };

        if let Some(batch) = to_commit
            && let Err(e) = self.commit_batch(&batch)
        {
            tracing::error!(event_type, "failed to commit blocks batch: {e:?}");
        }
        true
    }

    fn try_advance_current_batch(&mut self, seqno: u32) -> AdvanceBlockStatus {
        if seqno < self.next_expected_seqno {
            return AdvanceBlockStatus::TooOld;
        } else if self.current_batch.contains_seqno(seqno) {
            return AdvanceBlockStatus::Unchanged;
        }

        let start_seqno = self.align_seqno(seqno);
        let prev_batch = std::mem::replace(
            &mut self.current_batch,
            BlocksBatch::new(start_seqno, self.batch_size, &self.validator_indices),
        );
        self.next_expected_seqno = seqno + 1;

        AdvanceBlockStatus::Replaced(prev_batch)
    }

    fn commit_batch(&self, batch: &BlocksBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let cell = batch
            .build_cell()
            .context("failed to pack batch into a cell")?;

        let Some(tx) = &self.complete_batches else {
            anyhow::bail!("not initialized");
        };

        if tx.send(cell).is_err() {
            anyhow::bail!("channel closed");
        }
        Ok(())
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
    Replaced(BlocksBatch),
}
