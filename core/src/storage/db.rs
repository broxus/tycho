use tycho_storage::{Migrations, StateVersionProvider, WithMigrations};
use tycho_util::sync::CancellationFlag;
use weedb::{Caches, MigrationError, Semver, VersionProvider, WeeDb};

use super::tables;

pub type CoreDb = WeeDb<CoreTables>;

pub trait CoreDbExt {
    fn normalize_version(&self) -> anyhow::Result<()>;
}

impl CoreDbExt for CoreDb {
    // TEMP: Set a proper version on start. Remove on testnet reset.
    fn normalize_version(&self) -> anyhow::Result<()> {
        let provider = StateVersionProvider {
            db_name: CoreTables::NAME,
        };

        // Check if there is NO VERSION
        if provider.get_version(self.raw())?.is_some() {
            return Ok(());
        }

        // Check if the DB is NOT EMPTY
        {
            let mut package_entires_iter = self.package_entries.raw_iterator();
            package_entires_iter.seek_to_first();
            package_entires_iter.status()?;
            if package_entires_iter.item().is_none() {
                return Ok(());
            }
        }

        // Set the initial version
        tracing::warn!("normalizing DB version");
        provider.set_version(self.raw(), [0, 0, 1])?;
        Ok(())
    }
}

impl WithMigrations for CoreTables {
    const NAME: &'static str = "base";
    const VERSION: Semver = [0, 0, 3];

    fn register_migrations(
        migrations: &mut Migrations<Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        migrations.register([0, 0, 1], [0, 0, 2], move |db| {
            core_migrations::v0_0_1_to_0_0_2(db, cancelled.clone())
        })?;
        migrations.register([0, 0, 2], [0, 0, 3], core_migrations::v_0_0_2_to_v_0_0_3)?;

        Ok(())
    }
}

weedb::tables! {
    pub struct CoreTables<Caches> {
        pub state: tycho_storage::tables::State,
        pub archives: tables::Archives,
        pub archive_block_ids: tables::ArchiveBlockIds,
        pub block_handles: tables::BlockHandles,
        pub key_blocks: tables::KeyBlocks,
        pub full_block_ids: tables::FullBlockIds,
        pub package_entries: tables::PackageEntries,
        pub block_data_entries: tables::BlockDataEntries,
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

    use everscale_types::boc::Boc;
    use tycho_block_util::archive::ArchiveEntryType;
    use tycho_storage::StoredValue;
    use weedb::rocksdb::CompactOptions;

    use super::*;
    use crate::storage::PackageEntryKey;

    pub fn v0_0_1_to_0_0_2(db: &CoreDb, cancelled: CancellationFlag) -> Result<(), MigrationError> {
        let mut block_data_iter = db.package_entries.raw_iterator();
        block_data_iter.seek_to_first();

        tracing::info!("stated migrating package entries");

        let started_at = Instant::now();
        let mut total_processed = 0usize;
        let mut block_ids_created = 0usize;

        let full_block_ids_cf = &db.full_block_ids.cf();
        let mut batch = weedb::rocksdb::WriteBatch::default();
        let mut cancelled = cancelled.debounce(10);
        loop {
            let (key, value) = match block_data_iter.item() {
                Some(item) if !cancelled.check() => item,
                Some(_) => return Err(MigrationError::Custom(anyhow::anyhow!("cancelled").into())),
                None => {
                    block_data_iter.status()?;
                    break;
                }
            };

            'item: {
                let key = PackageEntryKey::from_slice(key);
                if key.ty != ArchiveEntryType::Block {
                    break 'item;
                }

                let file_hash = Boc::file_hash_blake(value);
                batch.put_cf(full_block_ids_cf, key.block_id.to_vec(), file_hash);
                block_ids_created += 1;
            }

            block_data_iter.next();
            total_processed += 1;
        }

        db.rocksdb()
            .write_opt(batch, db.full_block_ids.write_config())?;

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            total_processed,
            block_ids_created,
            "finished migrating package entries"
        );
        Ok(())
    }

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
