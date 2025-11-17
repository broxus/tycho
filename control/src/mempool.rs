use std::ops::Range;
use std::time::Duration;

use anyhow::Result;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;

pub trait MempoolService: Send + Sync + 'static {
    fn list_banned(&self) -> Vec<HashBytes>;

    fn ban_cache_dump(&self, req: BanCacheDumpRequest) -> Result<String>;

    fn manual_ban(&self, req: BanRequest) -> Result<String>;

    /// has a delayed effect (peer must resolve), unlike `ban` that is immediately noticeable,
    /// so we return some status as a result
    fn manual_unban(&self, peer_id: HashBytes, force: bool) -> BoxFuture<'static, Result<()>>;

    // TODO list banned from known peers in case ban was applied but record was not set

    /// default order is descending:. asc==false
    fn list_events(
        &self,
        req: ListEventsRequest,
    ) -> BoxFuture<'static, Result<Vec<MempoolEventDisplay>>>;

    // TODO async fn get_event_point(key: PointKey) -> boc / parsed

    fn delete_events(&self, millis: Range<u64>) -> BoxFuture<'static, Result<()>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BanCacheDumpRequest {
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

impl PointKey {
    const BYTES: usize = 4 + 32;

    pub fn from_bytes(bytes: &[u8; Self::BYTES]) -> Self {
        let mut round = [0_u8; 4];
        round.copy_from_slice(&bytes[..4]);
        PointKey(
            u32::from_be_bytes(round),
            HashBytes::from_slice(&bytes[4..]),
        )
    }
    pub fn to_bytes(&self) -> [u8; Self::BYTES] {
        let mut buf = [0; _];
        buf[..4].copy_from_slice(&self.0.to_be_bytes());
        buf[4..].copy_from_slice(&self.1[..]);
        buf
    }
}
