use tl_proto::TlError;

use crate::models::PointIntegrityError;

/// Helper for the result of broadcast and signature queries
pub enum QueryError {
    Network(anyhow::Error),
    TlError(TlError),
}

/// Helper for the result of point-by-id query
pub enum PointByIdQueryError {
    Network(anyhow::Error),
    BadPoint(PointIntegrityError),
    TlError(TlError),
}

impl From<TlError> for PointByIdQueryError {
    fn from(error: TlError) -> Self {
        Self::TlError(error)
    }
}

impl From<PointIntegrityError> for PointByIdQueryError {
    fn from(error: PointIntegrityError) -> Self {
        Self::BadPoint(error)
    }
}
