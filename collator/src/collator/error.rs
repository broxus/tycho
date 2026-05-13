use tycho_types::models::BlockIdShort;

use crate::mempool::MempoolAnchorId;

#[derive(Debug)]
pub enum CollationAbortReason {
    AnchorNotFound(MempoolAnchorId),
    NextAnchorNotFound(MempoolAnchorId),
    DiffNotFoundInQueue(BlockIdShort),
}

#[derive(thiserror::Error, Debug)]
pub enum CollatorError {
    #[error("Aborted(reason: {0:?})")]
    Aborted(CollationAbortReason),
    #[error("Cancelled externally")]
    Cancelled,
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl From<tycho_types::error::Error> for CollatorError {
    fn from(value: tycho_types::error::Error) -> Self {
        Self::Anyhow(value.into())
    }
}
