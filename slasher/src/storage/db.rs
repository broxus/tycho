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

weedb::tables! {
    pub struct SlasherTables<TableContext> {
        pub state: tables::State,
        pub vset_epochs: tables::VsetEpochs,
        pub session_meta: tables::SessionMeta,
        pub block_batches: tables::BlockBatches,
        pub session_reports: tables::SessionReports,
        pub vset_reports: tables::VsetReports,
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

    /// Stores validator-set epochs keyed by their start validation session.
    pub struct VsetEpochs;

    impl VsetEpochs {
        pub const KEY_LEN: usize = 4 + 4;
    }

    impl ColumnFamily for VsetEpochs {
        const NAME: &'static str = "vset_epochs";
    }

    impl ColumnFamilyOptions<TableContext> for VsetEpochs {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);

            opts.set_optimize_filters_for_hits(true);
            optimize_for_point_lookup(opts, ctx);
        }
    }

    /// Stores session metadata grouped by epoch.
    pub struct SessionMeta;

    impl SessionMeta {
        pub const KEY_LEN: usize = VsetEpochs::KEY_LEN + 4 + 4;
    }

    impl ColumnFamily for SessionMeta {
        const NAME: &'static str = "session_meta";
    }

    impl ColumnFamilyOptions<TableContext> for SessionMeta {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);

            opts.set_optimize_filters_for_hits(true);
            optimize_for_point_lookup(opts, ctx);
        }
    }

    /// Block batches submitted by validators
    /// - Key: `session_id: (catchain_seqno u32 BE, vset_switch_round u32 BE), validator_idx: u16 BE, start_block: u32 BE`
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

    /// Cached analyzer result for a single validation session.
    pub struct SessionReports;

    impl SessionReports {
        pub const KEY_LEN: usize = 4 + 4;
    }

    impl ColumnFamily for SessionReports {
        const NAME: &'static str = "session_reports";
    }

    impl ColumnFamilyOptions<TableContext> for SessionReports {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);

            opts.set_optimize_filters_for_hits(true);
            optimize_for_point_lookup(opts, ctx);
        }
    }

    /// Final analyzer result for a closed validator-set epoch.
    pub struct VsetReports;

    impl ColumnFamily for VsetReports {
        const NAME: &'static str = "vset_reports";
    }

    impl ColumnFamilyOptions<TableContext> for VsetReports {
        fn options(opts: &mut Options, ctx: &mut TableContext) {
            default_block_based_table_factory(opts, ctx);

            opts.set_optimize_filters_for_hits(true);
            optimize_for_point_lookup(opts, ctx);
        }
    }
}
