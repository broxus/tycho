use tycho_storage::kv::{NamedTables, TableContext};
use weedb::WeeDb;

use super::tables;

pub type MempoolDb = WeeDb<MempoolTables>;

impl NamedTables for MempoolTables {
    const NAME: &'static str = "mempool";
}

weedb::tables! {
    /// Default column family contains at most single row: overlay id.
    /// Overlay id defines data version: data will be removed on mismatch during boot.
    /// - Key: `overlay id: [u8; 32]`
    /// - Value: None
    pub struct MempoolTables<TableContext> {
        pub points: tables::Points,
        pub points_info: tables:: PointsInfo,
        pub points_status: tables::PointsStatus,
    }
}
