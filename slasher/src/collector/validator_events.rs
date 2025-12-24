use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::instrument;
use tycho_crypto::ed25519;
use tycho_slasher_traits::{ReceivedSignature, ValidationSessionId, ValidatorEventsListener};
use tycho_types::models::{BlockId, IndexedValidatorDescription};
use tycho_util::{DashMapEntry, FastDashMap};

use crate::bc::BlocksBatch;

const INIT_QUEUE_CAPACITY: usize = 3;

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
        queue.pop_front()
    }

    fn push_session_to_init(&self, info: ValidatorSessionInfo) {
        let mut items = self.init_queue.lock().unwrap();
        if items.len() >= self.init_queue_capacity
            && let Some(info) = items.pop_front()
        {
            tracing::warn!(session_id = ?info.session_id, "session info dropped from init queue");
        }
        items.push_back(info);
    }

    pub fn set_default_batch_size(&self, batch_size: NonZeroU32) {
        self.default_batch_size
            .store(batch_size.get(), Ordering::Release);
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
        own_validator_idx: u16,
        validators: &[IndexedValidatorDescription],
    ) {
        tracing::debug!(first_mc_seqno, "on_session_open");

        let validator_indices = validators
            .iter()
            .map(|item| item.validator_idx)
            .collect::<Box<[_]>>();

        let batch_size = NonZeroU32::new(self.default_batch_size.load(Ordering::Acquire)).unwrap();
        let current_batch = BlocksBatch::new(first_mc_seqno, batch_size, &validator_indices);

        let validators = Arc::<[IndexedValidatorDescription]>::from(validators);

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

            self.push_session_to_init(ValidatorSessionInfo {
                session_id,
                first_mc_seqno,
                own_validator_idx,
                validators,
            });
        } else {
            tracing::warn!("duplicate session");
        }
    }

    #[instrument(skip_all, fields(session_id = ?session_id))]
    fn on_session_finished(&self, session_id: ValidationSessionId) {
        tracing::debug!("on_session_drop");
        if let Some((_, session)) = self.sessions.remove(&session_id)
            && let Err(e) = session.commit_final_batch()
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
    fn handle_block(&mut self, seqno: u32, signatures: Option<&[ReceivedSignature]>) -> bool {
        let to_commit = match self.try_advance_current_batch(seqno) {
            AdvanceBlockStatus::TooOld => return false,
            AdvanceBlockStatus::Unchanged => None,
            AdvanceBlockStatus::Replaced(batch) => Some(batch),
        };

        let event_type = match signatures {
            Some(signatures) => {
                self.current_batch.commit_signatures(seqno, signatures);
                "validated"
            }
            None => "skipped",
        };

        if let Some(batch) = to_commit
            && let Err(e) = self.commit_batch(batch)
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

    fn commit_batch(&self, batch: BlocksBatch) -> Result<()> {
        Self::commit_batch_impl(&self.complete_batches, batch)
    }

    fn commit_final_batch(self) -> Result<()> {
        Self::commit_batch_impl(&self.complete_batches, self.current_batch)
    }

    fn commit_batch_impl(
        complete_batches: &Option<BlocksBatchTx>,
        batch: BlocksBatch,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let Some(tx) = complete_batches else {
            anyhow::bail!("not initialized");
        };

        if tx.send(batch).is_err() {
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
