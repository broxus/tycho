use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use tycho_types::models::{BlockId, ValidatorDescription};

// TODO: Decide how to be with this collator-defined type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValidationSessionId {
    /// Validation round seqno.
    pub seqno: u32,
    /// Validator subset short seqno.
    pub short_hash: u32,
}

// TEMP
impl From<(u32, u32)> for ValidationSessionId {
    #[inline]
    fn from(value: (u32, u32)) -> Self {
        Self {
            seqno: value.0,
            short_hash: value.1,
        }
    }
}

// TEMP
impl Ord for ValidationSessionId {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.seqno, self.short_hash).cmp(&(other.seqno, other.short_hash))
    }
}

// TEMP
impl PartialOrd for ValidationSessionId {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct ValidatorEvents {
    listener: Arc<dyn ValidatorEventsListener>,
}

impl ValidatorEvents {
    pub fn new(recorder: Arc<dyn ValidatorEventsListener>) -> Self {
        Self { listener: recorder }
    }

    pub fn begin_session(
        &self,
        session_id: ValidationSessionId,
        first_mc_seqno: u32,
        validators: &[ValidatorDescription],
    ) -> ValidatorSessionScope {
        self.listener
            .on_session_started(session_id, first_mc_seqno, validators);
        ValidatorSessionScope {
            recorder: self.listener.clone(),
            session_id,
            validator_count: validators.len(),
            is_sealed: AtomicBool::new(false),
        }
    }
}

pub struct ValidatorSessionScope {
    recorder: Arc<dyn ValidatorEventsListener>,
    session_id: ValidationSessionId,
    validator_count: usize,
    is_sealed: AtomicBool,
}

impl ValidatorSessionScope {
    pub fn begin_block(&self, block_id: &BlockId) -> BlockValidationScope {
        BlockValidationScope {
            recorder: self.recorder.clone(),
            session_id: self.session_id,
            block_id: *block_id,
            signature_slots: vec![0; self.validator_count]
                .into_iter()
                .map(AtomicU8::new)
                .collect::<Box<[_]>>(),
            is_sealed: AtomicBool::new(false),
        }
    }

    pub fn finish(&self) {
        if self.seal() {
            self.recorder.on_session_finished(self.session_id);
        }
    }

    fn seal(&self) -> bool {
        !self.is_sealed.swap(true, Ordering::Release)
    }
}

impl Drop for ValidatorSessionScope {
    fn drop(&mut self) {
        self.finish();
    }
}

pub struct BlockValidationScope {
    recorder: Arc<dyn ValidatorEventsListener>,
    session_id: ValidationSessionId,
    block_id: BlockId,
    signature_slots: Box<[AtomicU8]>,
    is_sealed: AtomicBool,
}

impl BlockValidationScope {
    pub fn session_id(&self) -> ValidationSessionId {
        self.session_id
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn receive_signature(&self, validator_idx: u16, is_valid: bool) -> bool {
        let mask = if is_valid {
            ReceivedSignature::VALID_SIGNATURE_BIT
        } else {
            ReceivedSignature::INVALID_SIGNATURE_BIT
        };

        if let Some(status) = self.signature_slots.get(validator_idx as usize) {
            status.fetch_or(mask, Ordering::Release) & mask == 0
        } else {
            false
        }
    }

    pub fn commit(&self) -> bool {
        if self.seal() {
            // TODO: Use some unsafe magic to make this closer to a NOOP.
            let mut signatures = Arc::new_uninit_slice(self.signature_slots.len());
            for (res, slot) in std::iter::zip(
                Arc::get_mut(&mut signatures).unwrap(),
                &self.signature_slots,
            ) {
                *res = MaybeUninit::new(ReceivedSignature(slot.load(Ordering::Acquire)));
            }
            // SAFETY: All items were initialized.
            let signatures = unsafe { signatures.assume_init() };

            self.recorder
                .on_block_validated(self.session_id, &self.block_id, signatures);
            true
        } else {
            false
        }
    }

    pub fn discard(&self) -> bool {
        if self.seal() {
            self.recorder
                .on_block_skipped(self.session_id, &self.block_id);
            true
        } else {
            false
        }
    }

    fn seal(&self) -> bool {
        !self.is_sealed.swap(true, Ordering::Release)
    }
}

impl Drop for BlockValidationScope {
    fn drop(&mut self) {
        self.discard();
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ReceivedSignature(u8);

impl ReceivedSignature {
    const VALID_SIGNATURE_BIT: u8 = 0b01;
    const INVALID_SIGNATURE_BIT: u8 = 0b10;

    pub fn has_valid_signature(&self) -> bool {
        self.0 & Self::VALID_SIGNATURE_BIT != 0
    }

    pub fn has_invalid_signature(&self) -> bool {
        self.0 & Self::INVALID_SIGNATURE_BIT != 0
    }
}

/// Unified event-sink interface for the validator.
///
/// Implementations can decide whether to perform work inline or forward the
/// event into an async task / channel. No async methods are used here to keep
/// the trait usable in both sync and async contexts.
pub trait ValidatorEventsListener: Send + Sync + 'static {
    /// Called exactly once when a new validation session is created.
    fn on_session_started(
        &self,
        session_id: ValidationSessionId,
        first_mc_seqno: u32,
        validators: &[ValidatorDescription],
    );

    /// Called when the session is complete.
    fn on_session_finished(&self, session_id: ValidationSessionId);

    /// Called when validation is complete for a block.
    fn on_block_validated(
        &self,
        session_id: ValidationSessionId,
        block_id: &BlockId,
        signatures: Arc<[ReceivedSignature]>,
    );

    /// Called when validation is skipped for a block.
    fn on_block_skipped(&self, session_id: ValidationSessionId, block_id: &BlockId);
}

#[derive(Debug, Clone, Copy)]
pub struct NoopValidatorEventsRecorder;

impl ValidatorEventsListener for NoopValidatorEventsRecorder {
    fn on_session_started(
        &self,
        _session_id: ValidationSessionId,
        _first_mc_seqno: u32,
        _validators: &[ValidatorDescription],
    ) {
    }

    fn on_session_finished(&self, _session_id: ValidationSessionId) {}

    fn on_block_validated(
        &self,
        _session_id: ValidationSessionId,
        _block_id: &BlockId,
        _signatures: Arc<[ReceivedSignature]>,
    ) {
    }

    fn on_block_skipped(&self, _session_id: ValidationSessionId, _block_id: &BlockId) {}
}

macro_rules! impl_recorder_for_tuples {
    ($(($($ty:ident: $n:tt),+)),*$(,)?) => {
        $(impl<$($ty),+> ValidatorEventsListener for ($($ty,)+)
        where
            $($ty: ValidatorEventsListener,)+
        {
            fn on_session_started(
                &self,
                session_id: ValidationSessionId,
                first_mc_seqno: u32,
                validators: &[ValidatorDescription],
            ) {
                $(self.$n.on_session_started(session_id, first_mc_seqno, validators);)+
            }

            fn on_session_finished(&self, session_id: ValidationSessionId) {
                $(self.$n.on_session_finished(session_id);)+
            }

            fn on_block_validated(
                &self,
                session_id: ValidationSessionId,
                block_id: &BlockId,
                signatures: Arc<[ReceivedSignature]>,
            ) {
                impl_recorder_for_tuples!(@call_on_validated self, session_id, block_id, signatures, $($n)+);
            }

            fn on_block_skipped(&self, session_id: ValidationSessionId, block_id: &BlockId) {
                $(self.$n.on_block_skipped(session_id, block_id);)+
            }
        })*
    };

    (@call_on_validated $self:ident, $sid:ident, $block_id:ident, $signatures:ident, $n:tt $($rest:tt)+) => {
        $self.$n.on_block_validated($sid, $block_id, $signatures.clone());
        impl_recorder_for_tuples!(@call_on_validated $self, $sid, $block_id, $signatures, $($rest)+)
    };
    (@call_on_validated $self:ident, $sid:ident, $block_id:ident, $signatures:ident, $n:tt) => {
        $self.$n.on_block_validated($sid, $block_id, $signatures);
    };
}

impl_recorder_for_tuples! {
    (T0: 0),
    (T0: 0, T1: 1),
    (T0: 0, T1: 1, T2: 2),
    (T0: 0, T1: 1, T2: 2, T3: 3),
    (T0: 0, T1: 1, T2: 2, T3: 3, T4: 4),
    (T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5),
    (T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6),
}
