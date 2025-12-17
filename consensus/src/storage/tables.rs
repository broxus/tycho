use tycho_storage::kv::{
    DEFAULT_MIN_BLOB_SIZE, NamedTables, TableContext, optimize_for_point_lookup,
};
use weedb::rocksdb::{DBCompressionType, Options};
use weedb::{ColumnFamily, ColumnFamilyOptions};

use super::status_flags;

impl NamedTables for MempoolTables {
    const NAME: &'static str = "mempool";
}

weedb::tables! {
    /// Default column family contains at most single row: overlay id.
    /// Overlay id defines data version: data will be removed on mismatch during boot.
    /// - Key: `overlay id: [u8; 32]`
    /// - Value: None
    pub struct MempoolTables<TableContext> {
        pub points: Points,
        pub points_info: PointsInfo,
        pub points_status: PointsStatus,
    }
}

/// Stores mempool point data
/// - Key: [`crate::models::PointKey`]
/// - Value: [`crate::models::Point`]
pub struct Points;

impl ColumnFamily for Points {
    const NAME: &'static str = "points";
}

impl ColumnFamilyOptions<TableContext> for Points {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        optimize_for_point_lookup(opts, ctx);
        opts.set_disable_auto_compactions(true);

        opts.set_enable_blob_files(true);
        opts.set_enable_blob_gc(false); // manual
        opts.set_min_blob_size(DEFAULT_MIN_BLOB_SIZE);
        opts.set_blob_compression_type(DBCompressionType::None);
    }
}

/// Stores truncated mempool point data
/// - Key: [`crate::models::PointKey`]
/// - Value: [`crate::models::PointInfo`]
pub struct PointsInfo;

impl ColumnFamily for PointsInfo {
    const NAME: &'static str = "points_info";
}

impl ColumnFamilyOptions<TableContext> for PointsInfo {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        optimize_for_point_lookup(opts, ctx);
        opts.set_disable_auto_compactions(true);
    }
}

/// Stores mempool point flags
/// - Key: [`crate::models::PointKey`]
/// - Value: [`crate::models::PointStatusStored`]
///   - also see  [`crate::models::point_status::StatusFlags::try_from_stored`]
pub struct PointsStatus;

impl ColumnFamily for PointsStatus {
    const NAME: &'static str = "points_status";
}

impl ColumnFamilyOptions<TableContext> for PointsStatus {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        optimize_for_point_lookup(opts, ctx);
        opts.set_disable_auto_compactions(true);

        opts.set_merge_operator_associative("points_status_merge", status_flags::merge);
    }
}
