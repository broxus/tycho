pub mod collector;
pub mod dispatcher;

use anyhow::Result;
use tycho_network::PeerId;
use tycho_types::models::BlockId;

use crate::validator::ValidationSessionId;

/// Immutable context shared by every block inside a single validation session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionCtx {
    pub session_id: ValidationSessionId,
}

/// Result of signature verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigStatus {
    /// Signature has been verified and is correct.
    Valid,
    /// Signature is present but failed verification.
    Invalid,
}

/// A single signature-related event.
#[derive(Debug, Clone)]
pub struct SignatureEvent {
    /// Session context
    pub ctx: SessionCtx,
    /// Validating block id
    pub block_id: BlockId,
    /// Validator whose signature we processed (may be our own node).
    pub peer_id: PeerId,
    /// Whether the signature was valid or invalid.
    pub status: SigStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("session not found for id {0:?}")]
    SessionNotFound(ValidationSessionId),

    #[error("session already exists for id {0:?}")]
    SessionAlreadyExists(ValidationSessionId),
}

/// Unified event-sink interface for the validator.
///
/// Implementations can decide whether to perform work inline or forward the
/// event into an async task / channel.  No async methods are used here to keep
/// the trait usable in both sync and async contexts.
pub trait ValidationEvents: Send + Sync + 'static {
    /// Called exactly once when a new validation session is created.
    fn on_session_open(&self, ctx: &SessionCtx) -> Result<()>;

    /// Called when the session dropped.
    fn on_session_drop(&self, ctx: &SessionCtx) -> Result<()>;

    /// Called for every signature event.
    ///
    /// Each unique (`block_id`, `peer_id`) pair is reported at most twice:
    /// * first with `SigStatus::Invalid`, if the first check failed;
    /// * later with `SigStatus::Valid`, if a correct signature is eventually received.
    fn on_signature_event(&self, ev: &SignatureEvent) -> Result<()>;

    /// Called when validation is skipped for a block.
    fn on_validation_skipped(&self, ctx: &SessionCtx, block_id: &BlockId) -> Result<()>;

    /// Called when validation is completed for a block.
    fn on_validation_complete(&self, ctx: &SessionCtx, block_id: &BlockId) -> Result<()>;
}
