use tycho_storage::tables::{optimize_for_point_lookup, DEFAULT_MIN_BLOB_SIZE};
use weedb::rocksdb::{DBCompressionType, Options};
use weedb::{Caches, ColumnFamily, ColumnFamilyOptions};

use super::point_status;

/// Stores mempool point data
/// - Key: `round: u32, digest: [u8; 32]`
/// - Value: `Point`
pub struct Points;

impl ColumnFamily for Points {
    const NAME: &'static str = "points";
}

impl ColumnFamilyOptions<Caches> for Points {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_point_lookup(opts, caches);
        opts.set_disable_auto_compactions(true);

        opts.set_enable_blob_files(true);
        opts.set_enable_blob_gc(false); // manual
        opts.set_min_blob_size(DEFAULT_MIN_BLOB_SIZE);
        opts.set_blob_compression_type(DBCompressionType::None);
    }
}

/// Stores truncated mempool point data
/// - Key: `round: u32, digest: [u8; 32]`
/// - Value: `PointInfo`
pub struct PointsInfo;

impl ColumnFamily for PointsInfo {
    const NAME: &'static str = "points_info";
}

impl ColumnFamilyOptions<Caches> for PointsInfo {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_point_lookup(opts, caches);
        opts.set_disable_auto_compactions(true);
    }
}

/// Stores mempool point flags
/// - Key: `round: u32, digest: [u8; 32]` as in [`Points`]
/// - Value: [`PointStatusStored`]
///   - also see  [`crate::point_status::StatusFlags::try_from_stored`]
///
/// [`PointStatusStored`]: crate::storage::PointStatusStored
pub struct PointsStatus;

impl ColumnFamily for PointsStatus {
    const NAME: &'static str = "points_status";
}

impl ColumnFamilyOptions<Caches> for PointsStatus {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_point_lookup(opts, caches);
        opts.set_disable_auto_compactions(true);

        opts.set_merge_operator_associative("points_status_merge", point_status::merge);
    }
}
