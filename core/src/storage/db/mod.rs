use tycho_storage::kv::{
    Migrations, NamedTables, StateVersionProvider, TableContext, WithMigrations,
};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, VersionProvider, WeeDb};

use super::tables;
mod migrations;

pub type CoreDb = WeeDb<CoreTables>;
pub type CellsDb = WeeDb<CellsTables>;

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

        // Check if the DB is NOT EMPTY
        {
            let mut cells_iter = self.cells.raw_iterator();
            cells_iter.seek_to_first();
            cells_iter.status()?;
            if cells_iter.item().is_none() {
                return Ok(());
            }
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

impl WithMigrations for CellsTables {
    const VERSION: Semver = [0, 0, 2];

    type VersionProvider = StateVersionProvider<tables::State>;

    fn new_version_provider() -> Self::VersionProvider {
        StateVersionProvider::new::<Self>()
    }

    fn register_migrations(
        migrations: &mut Migrations<Self::VersionProvider, Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        migrations.register([0, 0, 1], [0, 0, 2], move |db| {
            migrations::cells_v1_to_v2(db, &cancelled)
        })
    }
}
