use std::ops::Range;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;

pub trait MempoolService: Send + Sync + 'static {
    fn dump_bans(&self) -> Result<Vec<DumpBansItem>>;

    fn dump_events(&self, req: DumpEventsRequest) -> Result<String>;

    fn manual_ban(&self, req: BanRequest) -> Result<String>;

    /// has a delayed effect (peer must resolve), unlike `ban` that is immediately noticeable,
    /// so we return some status as a result
    fn manual_unban(&self, peer_id: HashBytes) -> BoxFuture<'static, Result<()>>;

    // TODO list banned from known peers in case ban was applied but record was not set

    /// default order is descending:. asc==false
    fn list_events(
        &self,
        req: ListEventsRequest,
    ) -> BoxFuture<'static, Result<Vec<MempoolEventDisplay>>>;

    // TODO async fn get_event_point(key: PointKey) -> boc / parsed

    fn delete_events(&self, millis: Range<u64>) -> BoxFuture<'static, Result<()>>;

    fn get_event_point(&self, point_key: PointKey) -> BoxFuture<'static, Result<Bytes>>;
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
    pub point_keys: Option<Vec<PointKey>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PointKey(pub u32, pub HashBytes);
