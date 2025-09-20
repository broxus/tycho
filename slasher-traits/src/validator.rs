use std::sync::Arc;

use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, ShardIdent, ValidatorDescription};

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

/// Unified event-sink interface for the validator.
///
/// Implementations can decide whether to perform work inline or forward the
/// event into an async task / channel.  No async methods are used here to keep
/// the trait usable in both sync and async contexts.
pub trait ValidatorEventsListener: Send + Sync + 'static {
    /// Called exactly once when a new validation session is created.
    fn on_session_started(&self, event: SessionStartedEvent<'_>);

    /// Called when the session dropped.
    fn on_session_dropped(&self, sid: ValidationSessionId);

    /// Called when block validation is started.
    fn on_validation_started(&self, sid: ValidationSessionId, block_id: &BlockId);

    /// Called for every signature event.
    ///
    /// Each unique (`block_id`, `peer_id`) pair is reported at most twice:
    /// * first with [`SignatureStatus::Invalid`], if the first check failed;
    /// * later with [`SignatureStatus::Valid`], if a correct signature is eventually received.
    fn on_validation_event(&self, event: ValidationEvent<'_>);

    /// Called when validation is skipped for a block.
    fn on_validation_skipped(&self, sid: ValidationSessionId, block_id: &BlockId);

    /// Called when validation is completed for a block.
    fn on_validation_complete(&self, sid: ValidationSessionId, block_id: &BlockId);
}

#[derive(Debug, Clone, Copy)]
pub struct SessionStartedEvent<'a> {
    /// Session id.
    pub session_id: ValidationSessionId,
    /// Session shard.
    pub shard_ident: ShardIdent,
    /// Validation range start.
    pub start_block_seqno: u32,
    /// Validator set entries (in the same order).
    pub validators: &'a [ValidatorDescription],
}

/// A single signature-related event.
#[derive(Debug, Clone, Copy)]
pub struct ValidationEvent<'a> {
    /// Session id.
    pub session_id: ValidationSessionId,
    /// Validated block id.
    pub block_id: &'a BlockId,
    /// Validator whose signature we processed (may be our own node).
    pub peer_id: &'a HashBytes,
    /// Whether the signature was valid or invalid.
    pub signature_status: SignatureStatus,
    /// Whether the signature was received before the validation start.
    pub from_cache: bool,
}

/// Result of signature verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureStatus {
    /// Signature has been verified and is correct.
    Valid,
    /// Signature is present but failed verification.
    Invalid,
}

// === Basic Impl ===

macro_rules! impl_listener_for_wrappers {
    ($($t:ident => $ty:ty),*$(,)?) => {
        $(impl<$t: ValidatorEventsListener> ValidatorEventsListener for $ty {
            fn on_session_started(&self, event: SessionStartedEvent<'_>) {
                <$t>::on_session_started(self, event);
            }

            fn on_session_dropped(&self, sid: ValidationSessionId) {
                <$t>::on_session_dropped(self, sid)
            }

            fn on_validation_started(&self, sid: ValidationSessionId, block_id: &BlockId) {
                <$t>::on_validation_started(self, sid, block_id)
            }

            fn on_validation_event(&self, event: ValidationEvent<'_>) {
                <$t>::on_validation_event(self, event)
            }

            fn on_validation_skipped(&self, sid: ValidationSessionId, block_id: &BlockId) {
                <$t>::on_validation_skipped(self, sid, block_id)
            }

            fn on_validation_complete(&self, sid: ValidationSessionId, block_id: &BlockId) {
                <$t>::on_validation_complete(self, sid, block_id)
            }
        })*
    };
}

impl_listener_for_wrappers! {
    T => Box<T>,
    T => Arc<T>,
}

#[derive(Debug, Clone, Copy)]
pub struct NoopValidatorEventsListener;

impl ValidatorEventsListener for NoopValidatorEventsListener {
    fn on_session_started(&self, _event: SessionStartedEvent<'_>) {}
    fn on_session_dropped(&self, _sid: ValidationSessionId) {}
    fn on_validation_started(&self, _sid: ValidationSessionId, _block_id: &BlockId) {}
    fn on_validation_event(&self, _event: ValidationEvent<'_>) {}
    fn on_validation_skipped(&self, _sid: ValidationSessionId, _block_id: &BlockId) {}
    fn on_validation_complete(&self, _sid: ValidationSessionId, _block_id: &BlockId) {}
}

macro_rules! impl_listener_for_tuples {
    ($(($($ty:ident: $n:tt),+)),*$(,)?) => {
        $(impl<$($ty),+> ValidatorEventsListener for ($($ty,)+)
        where
            $($ty: ValidatorEventsListener,)+
        {
            fn on_session_started(&self, event: SessionStartedEvent<'_>) {
                $(self.$n.on_session_started(event);)+
            }

            fn on_session_dropped(&self, sid: ValidationSessionId) {
                $(self.$n.on_session_dropped(sid);)+
            }

            fn on_validation_started(&self, sid: ValidationSessionId, block_id: &BlockId) {
                $(self.$n.on_validation_started(sid, block_id);)+
            }

            fn on_validation_event(&self, event: ValidationEvent<'_>) {
                $(self.$n.on_validation_event(event);)+
            }

            fn on_validation_skipped(&self, sid: ValidationSessionId, block_id: &BlockId) {
                $(self.$n.on_validation_skipped(sid, block_id);)+
            }

            fn on_validation_complete(&self, sid: ValidationSessionId, block_id: &BlockId) {
                $(self.$n.on_validation_complete(sid, block_id);)+
            }
        })*
    };
}

impl_listener_for_tuples! {
    (T0: 0),
    (T0: 0, T1: 1),
    (T0: 0, T1: 1, T2: 2),
    (T0: 0, T1: 1, T2: 2, T3: 3),
    (T0: 0, T1: 1, T2: 2, T3: 3, T4: 4),
    (T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5),
    (T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6),
}
