use tycho_storage::kv::{
    DEFAULT_MIN_BLOB_SIZE, Migrations, NamedTables, TableContext, WithMigrations,
    optimize_for_point_lookup,
};
use tycho_util::sync::CancellationFlag;
use weedb::rocksdb::{DBCompressionType, MergeOperands, Options};
use weedb::{ColumnFamily, ColumnFamilyOptions, DefaultVersionProvider, MigrationError, Semver};

impl NamedTables for ModeratorTables {
    const NAME: &'static str = "moderator";
}

weedb::tables! {
    pub struct ModeratorTables<TableContext> {
        pub journal: Journal,
        pub journal_points: JournalPoints,
    }
}

/// Stores mempool moderation-related events
/// - Key: [`crate::moderator::RecordKey`]
/// - Value: [`crate::moderator::RecordValue`]
pub struct Journal;

impl ColumnFamily for Journal {
    const NAME: &'static str = "journal";
}

impl ColumnFamilyOptions<TableContext> for Journal {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        optimize_for_point_lookup(opts, ctx);
        opts.set_disable_auto_compactions(true);
    }
}

/// Mempool points linked to events are stored across versions
/// - Key: [`crate::models::PointKey`]
/// - Value: [`crate::moderator::JournalPoint`]
pub struct JournalPoints;

impl ColumnFamily for JournalPoints {
    const NAME: &'static str = "journal_points";
}

impl ColumnFamilyOptions<TableContext> for JournalPoints {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        optimize_for_point_lookup(opts, ctx);
        opts.set_disable_auto_compactions(true);

        opts.set_enable_blob_files(true);
        opts.set_enable_blob_gc(true);
        opts.set_min_blob_size(DEFAULT_MIN_BLOB_SIZE);
        opts.set_blob_compression_type(DBCompressionType::Zstd);

        opts.set_merge_operator_associative("journal_points_merge", journal_points_merge);
    }
}

fn journal_points_merge(
    key: &[u8],
    stored: Option<&[u8]>,
    new_status_queue: &MergeOperands,
) -> Option<Vec<u8>> {
    crate::moderator::JournalPoint::merge_bytes(key, stored.into_iter().chain(new_status_queue))
}

impl WithMigrations for ModeratorTables {
    const VERSION: Semver = [0, 0, 1];

    type VersionProvider = DefaultVersionProvider;

    fn new_version_provider() -> Self::VersionProvider {
        DefaultVersionProvider
    }

    fn register_migrations(
        _migrations: &mut Migrations<Self::VersionProvider, Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        // migrations.register([0, 0, 1], [0, 0, 2], move |db| Ok(()))?;
        Ok(())
    }
}
