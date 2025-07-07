use tycho_storage::kv::{
    Migrations, NamedTables, StateVersionProvider, TableContext, WithMigrations,
};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, VersionProvider, WeeDb};

use super::tables;

pub type CoreDb = WeeDb<CoreTables>;

pub trait CoreDbExt {
    fn normalize_version(&self) -> anyhow::Result<()>;
}

impl CoreDbExt for CoreDb {
    // TEMP: Set a proper version on start. Remove on testnet reset.
    fn normalize_version(&self) -> anyhow::Result<()> {
        let provider = CoreTables::new_version_provider();

        // Check if there is NO VERSION
        if provider.get_version(self.raw())?.is_some() {
            return Ok(());
        }

        // Check if the DB is NOT EMPTY
        {
            let mut block_handles_iter = self.block_handles.raw_iterator();
            block_handles_iter.seek_to_first();
            block_handles_iter.status()?;
            if block_handles_iter.item().is_none() {
                return Ok(());
            }
        }

        // Set the initial version
        tracing::warn!("normalizing DB version");
        provider.set_version(self.raw(), [0, 0, 1])?;
        Ok(())
    }
}

impl NamedTables for CoreTables {
    const NAME: &'static str = "base";
}

impl WithMigrations for CoreTables {
    const VERSION: Semver = [0, 0, 3];

    type VersionProvider = StateVersionProvider<tables::State>;

    fn new_version_provider() -> Self::VersionProvider {
        StateVersionProvider::new::<Self>()
    }

    fn register_migrations(
        migrations: &mut Migrations<Self::VersionProvider, Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        migrations.register([0, 0, 2], [0, 0, 3], core_migrations::v_0_0_2_to_v_0_0_3)?;

        Ok(())
    }
}

weedb::tables! {
    pub struct CoreTables<TableContext> {
        pub state: tables::State,
        pub archives: tables::Archives,
        pub archive_block_ids: tables::ArchiveBlockIds,
        pub block_handles: tables::BlockHandles,
        pub key_blocks: tables::KeyBlocks,
        pub full_block_ids: tables::FullBlockIds,
        pub shard_states: tables::ShardStates,
        pub cells: tables::Cells,
        pub temp_cells: tables::TempCells,
        pub block_connections: tables::BlockConnections,

        // tables are empty, but they cannot be deleted because they are in a storage config
        _shard_internal_messages: tables::ShardInternalMessagesOld,
        _int_msg_stats_uncommited: tables::InternalMessageStatsUncommitedOld,
        _shard_int_msgs_uncommited: tables::ShardInternalMessagesUncommitedOld,
        _internal_message_stats: tables::InternalMessageStatsOld,
    }
}

mod core_migrations {
    use std::time::Instant;
    
    use weedb::rocksdb::CompactOptions;

    use super::*;

    // todo: should we also drop it cause we are migrating via resync
    pub fn v_0_0_2_to_v_0_0_3(db: &CoreDb) -> Result<(), MigrationError> {
        let mut opts = CompactOptions::default();
        opts.set_exclusive_manual_compaction(true);
        let null = Option::<&[u8]>::None;

        let started_at = Instant::now();
        tracing::info!("started cells compaction");
        db.cells
            .db()
            .compact_range_cf_opt(&db.cells.cf(), null, null, &opts);
        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "finished cells compaction"
        );

        Ok(())
    }
}
