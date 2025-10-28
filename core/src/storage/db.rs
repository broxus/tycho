use std::sync::Arc;

use tycho_storage::kv::{
    Migrations, NamedTables, StateVersionProvider, StoredValue, TableContext, WithMigrations,
};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, Table, VersionProvider, WeeDb, rocksdb};

use super::block_handle::{BlockFlags, BlockMeta};
use super::tables;

pub type CoreDb = WeeDb<CoreTables>;
pub type CellsDb = WeeDb<CellsTables>;
pub type CellsPartDb = WeeDb<CellsPartTables>;

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
    const VERSION: Semver = [0, 0, 5];

    type VersionProvider = StateVersionProvider<tables::State>;

    fn new_version_provider() -> Self::VersionProvider {
        StateVersionProvider::new::<Self>()
    }

    fn register_migrations(
        migrations: &mut Migrations<Self::VersionProvider, Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        let cancelled = cancelled.clone();
        migrations.register([0, 0, 4], [0, 0, 5], move |db| {
            const BATCH_LIMIT: usize = 1000;

            let mut batch = rocksdb::WriteBatch::default();
            let mut pending = 0;
            let mut updated = 0;

            let mut iter = db.block_handles.raw_iterator();
            iter.seek_to_first();
            iter.status().map_err(MigrationError::DbError)?;
            let mut started = false;
            while {
                if started {
                    iter.next();
                }
                iter.valid()
            } {
                started = true;

                if cancelled.check() {
                    return Err(MigrationError::Custom(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "migration cancelled",
                    ))));
                }

                let key = match iter.key() {
                    Some(key) => key,
                    None => break,
                };
                let value = match iter.value() {
                    Some(value) => value,
                    None => continue,
                };

                let meta = BlockMeta::from_slice(value);
                if !meta.add_flags(BlockFlags::HAS_STATE_PARTS) {
                    continue;
                }

                batch.merge_cf(&db.block_handles.cf(), key, meta.to_vec());

                // write batch when limit reached
                pending += 1;
                updated += 1;
                if pending >= BATCH_LIMIT {
                    db.rocksdb()
                        .write_opt(batch, db.block_handles.write_config())
                        .map_err(MigrationError::DbError)?;
                    batch = rocksdb::WriteBatch::default();
                    pending = 0;
                }
            }

            // write last batch
            if pending > 0 {
                db.rocksdb()
                    .write_opt(batch, db.block_handles.write_config())
                    .map_err(MigrationError::DbError)?;
            }

            tracing::info!(
                updated,
                "migration: added HAS_STATE_PARTS flag to existing block handles"
            );

            Ok(())
        })?;

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
    const VERSION: Semver = [0, 0, 1];

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
    pub struct CellsPartTables<TableContext> {
        pub state: tables::State,

        pub shard_states: tables::ShardStates,
        pub cells: tables::Cells,
        pub temp_cells: tables::TempCells,
    }
}

impl NamedTables for CellsPartTables {
    const NAME: &'static str = "cells-part";
}

impl WithMigrations for CellsPartTables {
    const VERSION: Semver = [0, 0, 1];

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

impl CoreDbExt for CellsPartDb {
    fn normalize_version(&self) -> anyhow::Result<()> {
        let provider = CellsPartTables::new_version_provider();

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
        tracing::warn!("normalizing DB version for cells parts");
        provider.set_version(self.raw(), [0, 0, 1])?;
        Ok(())
    }
}

/// The abstraction over `CellsDb` and `CellsShardDb`
pub(super) trait CellsDbOps: Send + Sync {
    fn shard_states(&self) -> &Table<tables::ShardStates>;
    fn cells(&self) -> &Table<tables::Cells>;
    fn temp_cells(&self) -> &Table<tables::TempCells>;
    fn rocksdb(&self) -> &Arc<rocksdb::DB>;
}

#[derive(Clone)]
pub(super) enum CellStorageDb {
    Main(CellsDb),
    Part(CellsPartDb),
}

impl CellsDbOps for CellStorageDb {
    fn shard_states(&self) -> &Table<tables::ShardStates> {
        match self {
            Self::Main(db) => &db.shard_states,
            Self::Part(db) => &db.shard_states,
        }
    }

    fn cells(&self) -> &Table<tables::Cells> {
        match self {
            Self::Main(db) => &db.cells,
            Self::Part(db) => &db.cells,
        }
    }

    fn temp_cells(&self) -> &Table<tables::TempCells> {
        match self {
            Self::Main(db) => &db.temp_cells,
            Self::Part(db) => &db.temp_cells,
        }
    }

    fn rocksdb(&self) -> &Arc<rocksdb::DB> {
        match self {
            Self::Main(db) => db.rocksdb(),
            Self::Part(db) => db.rocksdb(),
        }
    }
}
