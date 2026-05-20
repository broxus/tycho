use tycho_storage::kv::{
    Migrations, NamedTables, StateVersionProvider, TableContext, WithMigrations,
};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, VersionProvider, WeeDb};

use super::shard_state::db_state::CountersStore;
use super::tables;
mod migrations;

pub type CoreDb = WeeDb<CoreTables>;
pub type CellsDb = WeeDb<CellsTables>;

#[tracing::instrument(skip_all, fields(db = CellsTables::NAME))]
pub(super) async fn apply_cells_migrations(
    db: CellsDb,
    cell_counters: CountersStore,
) -> Result<CountersStore, MigrationError> {
    let cancelled = CancellationFlag::new();

    tracing::info!("started");
    scopeguard::defer! {
        cancelled.cancel();
    }

    let span = tracing::Span::current();
    let cancelled = cancelled.clone();
    tokio::task::spawn_blocking(move || {
        let cell_counters = std::sync::Arc::new(std::sync::Mutex::new(cell_counters));
        let _span = span.enter();

        let guard = scopeguard::guard((), |_| {
            tracing::warn!("cancelled");
        });

        // Cells migrations are not exposed through generic `ApplyMigrations`
        // because v2->v3 needs counter snapshot state.
        let mut migrations = weedb::Migrations::with_target_version_and_provider(
            CellsTables::VERSION,
            CellsTables::new_version_provider(),
        );
        let cancelled_v1_to_v2 = cancelled.clone();
        migrations.register([0, 0, 1], [0, 0, 2], move |db| {
            migrations::cells_v1_to_v2(db, &cancelled_v1_to_v2)
        })?;
        let cancelled_v2_to_v3 = cancelled;
        let cell_counters_for_migration = cell_counters.clone();
        migrations.register([0, 0, 2], [0, 0, 3], move |db| {
            let mut cell_counters = cell_counters_for_migration.lock().unwrap();
            migrations::cells_v2_to_v3(db, &mut cell_counters, &cancelled_v2_to_v3)
        })?;
        db.apply(migrations)?;

        scopeguard::ScopeGuard::into_inner(guard);
        tracing::info!("finished");
        let cell_counters = match std::sync::Arc::try_unwrap(cell_counters) {
            Ok(cell_counters) => cell_counters,
            Err(_) => unreachable!("migration closure must be dropped"),
        };
        Ok(cell_counters.into_inner().unwrap())
    })
    .await
    .map_err(|e| MigrationError::Custom(e.into()))?
}

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

        if is_table_empty(&self.block_handles)? {
            return Ok(());
        }

        // Set the initial version
        tracing::warn!("normalizing DB version for core");
        provider.set_version(self.raw(), [0, 0, 1])?;
        Ok(())
    }
}

impl CoreDbExt for CellsDb {
    fn normalize_version(&self) -> anyhow::Result<()> {
        let provider = CellsTables::new_version_provider();

        // Check if there is NO VERSION
        if provider.get_version(self.raw())?.is_some() {
            return Ok(());
        }

        if is_table_empty(&self.cells)? {
            return Ok(());
        }

        // Set the initial version
        tracing::warn!("normalizing DB version for cells");
        provider.set_version(self.raw(), [0, 0, 1])?;
        Ok(())
    }
}

impl NamedTables for CoreTables {
    const NAME: &'static str = "core";
}

impl WithMigrations for CoreTables {
    const VERSION: Semver = [0, 0, 4];

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
    pub struct CoreTables<TableContext> {
        pub state: tables::State,
        pub archive_block_ids: tables::ArchiveBlockIds,
        pub archive_events: tables::ArchiveEvents,
        pub block_handles: tables::BlockHandles,
        pub key_blocks: tables::KeyBlocks,
        pub full_block_ids: tables::FullBlockIds,
        pub block_connections: tables::BlockConnections,
    }
}

weedb::tables! {
    pub struct CellsTables<TableContext> {
        pub state: tables::State,

        pub shard_states: tables::ShardStates,
        pub cells: tables::Cells,
        pub temp_cells: tables::TempCells,
    }
}

impl NamedTables for CellsTables {
    const NAME: &'static str = "cells";
}

impl CellsTables {
    const VERSION: Semver = [0, 0, 3];

    fn new_version_provider() -> StateVersionProvider<tables::State> {
        StateVersionProvider::new::<Self>()
    }
}

pub(super) fn is_table_empty<T>(table: &weedb::Table<T>) -> anyhow::Result<bool>
where
    T: weedb::ColumnFamily,
{
    let mut iterator = table.raw_iterator();
    iterator.seek_to_first();
    iterator.status()?;
    Ok(iterator.item().is_none())
}
