use futures_util::TryFutureExt;
use tycho_storage::kv::{
    Migrations, NamedTables, StateVersionProvider, TableContext, WithMigrations,
};
use tycho_util::sync::CancellationFlag;
use weedb::rocksdb::{WaitForCompactOptions, WriteBatch};
use weedb::{BoundedCfHandle, MigrationError, Semver, VersionProvider, WeeDb};

pub type SlasherDb = WeeDb<SlasherTables>;

impl NamedTables for SlasherTables {
    const NAME: &'static str = "slasher";
}

impl WithMigrations for SlasherTables {
    const VERSION: Semver = [0, 2, 0];

    type VersionProvider = StateVersionProvider<tables::State>;

    fn new_version_provider() -> Self::VersionProvider {
        StateVersionProvider::new::<Self>()
    }

    fn register_migrations(
        migrations: &mut Migrations<Self::VersionProvider, Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        migrations.register([0, 1, 0], Self::VERSION, wipe_db)?;
        Ok(())
    }
}

weedb::tables! {
    pub struct SlasherTables<TableContext> {
        pub state: tables::State,
        pub vset_state: tables::VsetState,
        pub vset_sessions: tables::VsetSessions,
        pub block_batches: tables::BlockBatches,
    }
}

pub mod tables {
    use tycho_storage::kv::{
        TableContext, default_block_based_table_factory, optimize_for_point_lookup,
        zstd_block_based_table_factory,
    };
    use weedb::rocksdb::Options;
    use weedb::{ColumnFamily, ColumnFamilyOptions};

    /// Stores generic node parameters.
    pub struct State;

    impl ColumnFamily for State {
        const NAME: &'static str = "state";
    }

    impl ColumnFamilyOptions<TableContext> for State {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);

            opts.set_optimize_filters_for_hits(true);
            optimize_for_point_lookup(opts, ctx);
        }
    }

    /// Info about validator sets.
    ///
    /// - Key: `vset_hash: 32 bytes, key: u8`.
    pub struct VsetState;

    impl VsetState {
        pub const KEY_LEN: usize = 32 + 1;

        pub const KEY_TYPE_INFO: u8 = 0;
        pub const KEY_TYPE_REPORT: u8 = 1;
    }

    impl ColumnFamily for VsetState {
        const NAME: &'static str = "vset_state";
    }

    impl ColumnFamilyOptions<TableContext> for VsetState {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);
        }
    }

    /// Maps validator sessions to vset.
    ///
    /// - Key: `vset_hash: 32 bytes, session_id: (catchain_seqno u32 BE, vset_switch_round u32 BE)`
    /// - Value: `start_seqno: u32 LE`
    pub struct VsetSessions;

    impl VsetSessions {
        pub const KEY_LEN: usize = 32 + 4 + 4;
    }

    impl ColumnFamily for VsetSessions {
        const NAME: &'static str = "vset_sessions";
    }

    impl ColumnFamilyOptions<TableContext> for VsetSessions {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);
        }
    }

    /// Block batches submitted by validators
    /// - Key: `vset_hash: 32 bytes, validator_idx: u16 BE, start_block: u32 BE`
    /// - Value: blocks batch
    pub struct BlockBatches;

    impl BlockBatches {
        pub const KEY_LEN: usize = 32 + 2 + 4;
    }

    impl ColumnFamily for BlockBatches {
        const NAME: &'static str = "block_batches";
    }

    impl ColumnFamilyOptions<TableContext> for BlockBatches {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            zstd_block_based_table_factory(opts, ctx);
        }
    }
}

pub(super) trait SlasherDbExt {
    fn normalize_version(&self) -> impl Future<Output = anyhow::Result<()>>;
}

impl SlasherDbExt for SlasherDb {
    fn normalize_version(&self) -> impl Future<Output = anyhow::Result<()>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || normalize_version(&db))
            .unwrap_or_else(|e| Err(anyhow::anyhow!("normalize slasher db {e}")))
    }
}

fn normalize_version(db: &SlasherDb) -> anyhow::Result<()> {
    fn is_table_empty<T>(table: &weedb::Table<T>) -> anyhow::Result<bool>
    where
        T: weedb::ColumnFamily,
    {
        let mut iterator = table.raw_iterator();
        iterator.seek_to_first();
        iterator.status()?;
        Ok(iterator.item().is_none())
    }

    let provider = SlasherTables::new_version_provider();

    if provider.get_version(db.raw())?.is_some() {
        return Ok(());
    }

    if is_table_empty(&db.vset_state)?
        && is_table_empty(&db.vset_sessions)?
        && is_table_empty(&db.block_batches)?
    {
        return Ok(());
    }

    tracing::warn!("normalizing DB version for slasher");
    provider.set_version(db.raw(), [0, 1, 0])?;
    Ok(())
}

fn wipe_db(db: &SlasherDb) -> Result<(), MigrationError> {
    tracing::warn!("wiping slasher DB for incompatible storage format change");

    let mut batch = WriteBatch::default();

    batch.delete_range_cf(
        &db.vset_state.cf(),
        [0; tables::VsetState::KEY_LEN],
        [u8::MAX; tables::VsetState::KEY_LEN],
    );

    batch.delete_range_cf(
        &db.vset_sessions.cf(),
        [0; tables::VsetSessions::KEY_LEN],
        [u8::MAX; tables::VsetSessions::KEY_LEN],
    );
    batch.delete_range_cf(
        &db.block_batches.cf(),
        [0; tables::BlockBatches::KEY_LEN],
        [u8::MAX; tables::BlockBatches::KEY_LEN],
    );

    let compact = |cf: &BoundedCfHandle<'_>| {
        db.rocksdb()
            .compact_range_cf::<&[u8], &[u8]>(cf, None, None);
    };
    compact(&db.vset_state.cf());
    compact(&db.vset_sessions.cf());
    compact(&db.block_batches.cf());

    db.rocksdb().write(batch)?;

    let mut opts = WaitForCompactOptions::default();
    opts.set_flush(true);
    db.rocksdb().wait_for_compact(&opts)?;

    Ok(())
}
