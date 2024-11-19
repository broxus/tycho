use crate::mempool::MempoolAnchorId;

#[derive(Debug)]
pub enum CollationCancelReason {
    AnchorNotFound(MempoolAnchorId),
    NextAnchorNotFound(MempoolAnchorId),
    ExternalCancel,
}

#[derive(thiserror::Error, Debug)]
pub enum CollatorError {
    #[error("Cancelled(reason: {0:?})")]
    Cancelled(CollationCancelReason),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}
