use everscale_types::models::BlockIdShort;

use crate::mempool::MempoolAnchorId;

#[derive(Debug)]
pub enum CollationCancelReason {
    AnchorNotFound(MempoolAnchorId),
    NextAnchorNotFound(MempoolAnchorId),
    ExternalCancel,
    DiffNotFoundInQueue(BlockIdShort),
}

#[derive(thiserror::Error, Debug)]
pub enum CollatorError {
    #[error("Cancelled(reason: {0:?})")]
    Cancelled(CollationCancelReason),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl From<everscale_types::error::Error> for CollatorError {
    fn from(value: everscale_types::error::Error) -> Self {
        Self::Anyhow(value.into())
    }
}
