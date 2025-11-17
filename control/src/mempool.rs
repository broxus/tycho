use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;
#[cfg(feature = "server")]
use {anyhow::Result, futures_util::future::BoxFuture, std::ops::Range};

#[cfg(feature = "server")]
pub trait MempoolService: Send + Sync + 'static {
    /// Dumps the current in-memory moderator ban state.
    fn dump_bans(&self) -> Result<Vec<DumpBansItem>>;

    /// Dumps the in-memory moderator event/toleration cache, not persisted journal rows.
    fn dump_events(&self, req: DumpEventsRequest) -> Result<String>;

    /// Queues a manual ban and waits for persisted completion while the client stays connected.
    /// After the node accepts the request, it cannot be cancelled (by timeout, disconnect, etc.).
    fn manual_ban(&self, req: BanRequest) -> BoxFuture<'static, Result<String>>;

    /// Queues a manual unban and waits for persisted completion while the client stays connected.
    /// Live visibility can lag because the peer must resolve again after unban.
    /// After the node accepts the request, it cannot be cancelled (by timeout, disconnect, etc.).
    fn manual_unban(&self, peer_id: HashBytes) -> BoxFuture<'static, Result<()>>;

    // TODO list banned from known peers in case ban was applied but record was not set

    /// Lists persisted moderator journal records of all types.
    /// Default order is descending unless `asc=true`.
    /// Point key with `stored=true` can be used to retrieve full point with a separate call.
    fn list_events(
        &self,
        req: ListEventsRequest,
    ) -> BoxFuture<'static, Result<Vec<MempoolEventDisplay>>>;

    // TODO async fn get_event_point(key: PointKey) -> boc / parsed

    /// Deletes persisted moderator journal data only; does not mutate in-mem moderator state.
    /// After the node accepts the request, it cannot be cancelled (by timeout, disconnect, etc.).
    fn delete_events(&self, millis: Range<u64>) -> BoxFuture<'static, Result<()>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DumpBansItem {
    pub peer_id: HashBytes,
    pub until_millis: u64,
    pub created_millis: u64,
    pub record_seq_no: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DumpEventsRequest {
    pub peer_id: Option<HashBytes>,
    pub pretty: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BanRequest {
    pub peer_id: HashBytes,
    pub duration: Duration,
    pub pretty: bool,
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
