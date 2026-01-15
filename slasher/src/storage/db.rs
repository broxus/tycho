use tycho_storage::kv::{
    Migrations, NamedTables, StateVersionProvider, TableContext, WithMigrations,
};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, WeeDb};

pub type SlasherDb = WeeDb<SlasherTables>;

impl NamedTables for SlasherTables {
    const NAME: &'static str = "slasher";
}

impl WithMigrations for SlasherTables {
    const VERSION: Semver = [0, 1, 0];

    type VersionProvider = StateVersionProvider<tables::State>;

    fn new_version_provider() -> Self::VersionProvider {
        StateVersionProvider::new::<Self>()
    }

    fn register_migrations(
        _migrations: &mut Migrations<Self::VersionProvider, Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        Ok(())
    }
}

// TODO: Add a table for temp batches.
weedb::tables! {
    pub struct SlasherTables<TableContext> {
        pub state: tables::State,
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

    /// Stores generic node parameters
    /// - Key: `...`
    /// - Value: `...`
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

    /// Code hash with account address
    /// - Key: `session_id: (u32 BE, u32 BE), validator_idx: u16 BE, start_block: u32 BE`
    /// - Value: blocks batch
    pub struct BlockBatches;

    impl BlockBatches {
        pub const KEY_LEN: usize = 4 + 4 + 2 + 4;
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
