use crate::mempool::MempoolAnchorId;

#[derive(Debug)]
pub enum CollationCancelReason {
    AnchorNotFound(MempoolAnchorId),
    NextAnchorNotFound(MempoolAnchorId),
}

#[derive(Debug)]
pub enum CollatorError {
    Cancelled(CollationCancelReason),
    Anyhow(anyhow::Error),
}

impl From<anyhow::Error> for CollatorError {
    fn from(value: anyhow::Error) -> Self {
        Self::Anyhow(value)
    }
}

impl From<CollatorError> for anyhow::Error {
    fn from(value: CollatorError) -> Self {
        match value {
            CollatorError::Anyhow(error) => error,
            CollatorError::Cancelled(reason) => anyhow::anyhow!("Cancelled(reason: {:?}", reason),
        }
    }
}
