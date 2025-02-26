use crate::dag::HistoryConflict;
use crate::effects::Cancelled;

#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("Mempool engine task was cancelled")]
    Cancelled,
    #[error("{0}")]
    HistoryConflict(HistoryConflict),
}

impl From<Cancelled> for EngineError {
    fn from(_: Cancelled) -> Self {
        Self::Cancelled
    }
}

impl From<HistoryConflict> for EngineError {
    fn from(err: HistoryConflict) -> Self {
        Self::HistoryConflict(err)
    }
}
