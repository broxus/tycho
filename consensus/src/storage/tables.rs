use bytesize::ByteSize;
use tycho_storage::kv::{
    DEFAULT_MIN_BLOB_SIZE, NamedTables, TableContext, optimize_for_point_lookup,
};
use weedb::rocksdb::{DBCompressionType, MergeOperands, Options};
use weedb::{ColumnFamily, ColumnFamilyOptions};

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

        let write_buffer_size = ByteSize::mib(64);
        let write_buffer_count = 6u64;
        opts.set_write_buffer_size(write_buffer_size.as_u64() as _);
        opts.set_min_write_buffer_number_to_merge(1);
        opts.set_max_write_buffer_number(write_buffer_count as _);
        ctx.track_buffer_usage(
            write_buffer_size,
            ByteSize(write_buffer_size.as_u64() * write_buffer_count),
        );

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
/// - Value: [`crate::models::point_status::PointStatusStored`]
pub struct PointsStatus;

impl ColumnFamily for PointsStatus {
    const NAME: &'static str = "points_status";
}

impl ColumnFamilyOptions<TableContext> for PointsStatus {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        optimize_for_point_lookup(opts, ctx);
        opts.set_disable_auto_compactions(true);

        opts.set_merge_operator_associative("points_status_merge", points_status_merge);
    }
}

fn points_status_merge(
    key: &[u8],
    stored: Option<&[u8]>,
    new_status_queue: &MergeOperands,
) -> Option<Vec<u8>> {
    crate::models::point_status::merge_bytes(key, stored.into_iter().chain(new_status_queue))
}
