use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;
#[cfg(feature = "server")]
use {anyhow::Result, futures_util::future::BoxFuture};

#[cfg(feature = "server")]
pub trait MempoolService: Send + Sync + 'static {
    /// Lists persisted moderator journal records of all types.
    /// Default order is descending unless `asc=true`.
    /// Point key with `stored=true` can be used to retrieve full point with a separate call.
    fn list_events(
        &self,
        req: ListEventsRequest,
    ) -> BoxFuture<'static, Result<Vec<MempoolEventDisplay>>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListEventsRequest {
    pub count: u16,
    pub page: u32,
    pub asc: bool,
    pub with_ids: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MempoolEventDisplay {
    pub created: u64,
    pub seq_no: u32,
    pub peer_id: HashBytes,
    pub points: usize,
    pub kind: String,
    pub message: String,
    pub point_refs: Option<Vec<StoredPointKeyRef>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredPointKeyRef {
    pub stored: bool,
    pub key: PointKey,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PointKey(pub u32, pub HashBytes);
