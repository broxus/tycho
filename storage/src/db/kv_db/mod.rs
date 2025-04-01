use std::future::Future;
use std::path::Path;

use tycho_util::sync::CancellationFlag;
use weedb::{
    Caches, MigrationError, Semver, Tables, VersionProvider, WeeDb, WeeDbBuilder, WeeDbRaw,
};

pub mod refcount;
pub mod tables;

pub trait WeeDbExt<T: Tables>: Sized {
    fn builder_prepared<P: AsRef<Path>>(path: P, context: T::Context) -> WeeDbBuilder<T>;

    fn apply_migrations(&self) -> impl Future<Output = Result<(), MigrationError>> + Send;
}

impl<T: Tables + 'static> WeeDbExt<T> for WeeDb<T>
where
    Self: WithMigrations,
{
    fn builder_prepared<P: AsRef<Path>>(
        path: P,
        context: <T as Tables>::Context,
    ) -> WeeDbBuilder<T> {
        WeeDbBuilder::new(path, context).with_name(Self::NAME)
    }

    #[tracing::instrument(skip_all, fields(db = Self::NAME))]
    async fn apply_migrations(&self) -> Result<(), MigrationError> {
        let cancelled = CancellationFlag::new();

        tracing::info!("started");
        scopeguard::defer! {
            cancelled.cancel();
        }

        let span = tracing::Span::current();

        let this = self.clone();
        let cancelled = cancelled.clone();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let mut migrations = Migrations::<Self>::with_target_version_and_provider(
                Self::VERSION,
                StateVersionProvider {
                    db_name: Self::NAME,
                },
            );
            Self::register_migrations(&mut migrations, cancelled)?;
            this.apply(migrations)?;

            scopeguard::ScopeGuard::into_inner(guard);
            tracing::info!("finished");
            Ok(())
        })
        .await
        .map_err(|e| MigrationError::Custom(e.into()))?
    }
}

// === Base DB ===

pub type BaseDb = WeeDb<BaseTables>;

pub trait BaseDbExt {
    fn normalize_version(&self) -> anyhow::Result<()>;
}

impl BaseDbExt for BaseDb {
    // TEMP: Set a proper version on start. Remove on testnet reset.
    fn normalize_version(&self) -> anyhow::Result<()> {
        let provider = StateVersionProvider {
            db_name: Self::NAME,
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

impl WithMigrations for BaseDb {
    const NAME: &'static str = "base";
    const VERSION: Semver = [0, 0, 3];

    fn register_migrations(
        migrations: &mut Migrations<Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        migrations.register([0, 0, 1], [0, 0, 2], move |db| {
            base_migrations::v0_0_1_to_0_0_2(db, cancelled.clone())
        })?;
        migrations.register([0, 0, 2], [0, 0, 3], base_migrations::v_0_0_2_to_v_0_0_3)?;

        Ok(())
    }
}

weedb::tables! {
    pub struct BaseTables<Caches> {
        pub state: tables::State,
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
        pub shard_internal_messages: tables::ShardInternalMessages,
        pub internal_message_stats: tables::InternalMessageStats,
        pub internal_message_var: tables::InternalMessageVar,
        pub internal_message_diffs_tail: tables::InternalMessageDiffsTail,
        pub internal_message_diff_info: tables::InternalMessageDiffInfo,
        pub internal_message_commit_pointer: tables::InternalMessageCommitPointer,
        // tables are empty, but they cannot be deleted because they are in a storage config
        _int_msg_stats_uncommited: tables::InternalMessageStatsUncommited,
        _shard_int_msgs_uncommited: tables::ShardInternalMessagesUncommited,
    }
}

mod base_migrations {
    use std::time::Instant;

    use everscale_types::boc::Boc;
    use tycho_block_util::archive::ArchiveEntryType;
    use weedb::rocksdb::CompactOptions;

    use super::*;
    use crate::util::StoredValue;

    pub fn v0_0_1_to_0_0_2(db: &BaseDb, cancelled: CancellationFlag) -> Result<(), MigrationError> {
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
                let key = crate::PackageEntryKey::from_slice(key);
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

    pub fn v_0_0_2_to_v_0_0_3(db: &BaseDb) -> Result<(), MigrationError> {
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

// === RPC DB ===

pub type RpcDb = WeeDb<RpcTables>;

impl WithMigrations for RpcDb {
    const NAME: &'static str = "rpc";
    const VERSION: Semver = [0, 0, 2];

    fn register_migrations(
        migrations: &mut Migrations<Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        migrations.register([0, 0, 1], [0, 0, 2], move |db| {
            rpc_migrations::v0_0_1_to_0_0_2(db, cancelled.clone())
        })?;

        Ok(())
    }
}

weedb::tables! {
    pub struct RpcTables<Caches> {
        pub state: tables::State,
        pub transactions: tables::Transactions,
        pub transactions_by_hash: tables::TransactionsByHash,
        pub transactions_by_in_msg: tables::TransactionsByInMsg,
        pub code_hashes: tables::CodeHashes,
        pub code_hashes_by_address: tables::CodeHashesByAddress,
    }
}

mod rpc_migrations {
    use std::time::Instant;

    use everscale_types::boc::{Boc, BocTag};
    use everscale_types::models::Transaction;
    use tycho_util::sync::CancellationFlag;
    use weedb::MigrationError;

    use crate::{RpcDb, TransactionMask};

    pub fn v0_0_1_to_0_0_2(db: &RpcDb, cancelled: CancellationFlag) -> Result<(), MigrationError> {
        const BATCH_SIZE: usize = 100_000;

        let mut transactions_iter = db.transactions.raw_iterator();
        transactions_iter.seek_to_first();

        tracing::info!("stated migrating transactions");

        let started_at = Instant::now();
        let mut total_processed = 0usize;

        let transactions_cf = &db.transactions.cf();
        let mut batch = weedb::rocksdb::WriteBatch::default();
        let mut cancelled = cancelled.debounce(10);
        let mut buffer = Vec::new();
        loop {
            let (key, value) = match transactions_iter.item() {
                Some(item) if !cancelled.check() => item,
                Some(_) => return Err(MigrationError::Custom(anyhow::anyhow!("cancelled").into())),
                None => {
                    transactions_iter.status()?;
                    break;
                }
            };

            if BocTag::from_bytes(value[0..4].try_into().unwrap()).is_none() {
                // Skip already updated values.
                transactions_iter.next();
                continue;
            }

            let tx_cell = Boc::decode(value).map_err(|_e| {
                MigrationError::Custom(anyhow::anyhow!("invalid transaction in db").into())
            })?;

            let tx_hash = tx_cell.repr_hash();

            let tx = tx_cell.parse::<Transaction>().map_err(|_e| {
                MigrationError::Custom(anyhow::anyhow!("invalid transaction in db").into())
            })?;

            let mut mask = TransactionMask::empty();
            if tx.in_msg.is_some() {
                mask.set(TransactionMask::HAS_MSG_HASH, true);
            }

            let boc_start = if mask.has_msg_hash() { 65 } else { 33 }; // 1 + 32 + (32)

            buffer.clear();
            buffer.reserve(boc_start + value.len());

            buffer.push(mask.bits()); // Mask
            buffer.extend_from_slice(tx_hash.as_slice()); // Tx hash

            if let Some(in_msg) = tx.in_msg {
                buffer.extend_from_slice(in_msg.repr_hash().as_slice()); // InMsg hash
            }

            buffer.extend_from_slice(value); // Tx data

            batch.put_cf(transactions_cf, key, &buffer);

            transactions_iter.next();
            total_processed += 1;

            if total_processed % BATCH_SIZE == 0 {
                db.rocksdb()
                    .write_opt(std::mem::take(&mut batch), db.transactions.write_config())?;
            }
        }

        if !batch.is_empty() {
            db.rocksdb()
                .write_opt(batch, db.transactions.write_config())?;
        }

        tracing::info!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            total_processed,
            "finished migrating transactions"
        );

        Ok(())
    }
}

// === Mempool DB ===

pub type MempoolDb = WeeDb<MempoolTables>;

impl WithMigrations for MempoolDb {
    const NAME: &'static str = "mempool";
    const VERSION: Semver = [0, 0, 1];

    fn register_migrations(
        _migrations: &mut Migrations<Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        // TODO: register migrations here
        Ok(())
    }
}

weedb::tables! {
    /// Default column family contains at most single row: overlay id.
    /// Overlay id defines data version: data will be removed on mismatch during boot.
    /// - Key: `overlay id: [u8; 32]`
    /// - Value: None
    pub struct MempoolTables<Caches> {
        pub points: tables::Points,
        pub points_info: tables:: PointsInfo,
        pub points_status: tables::PointsStatus,
    }
}

// === Migrations stuff ===

trait WithMigrations: Sized {
    const NAME: &'static str;
    const VERSION: Semver;

    fn register_migrations(
        migrations: &mut Migrations<Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError>;
}

type Migrations<D> = weedb::Migrations<StateVersionProvider, D>;

struct StateVersionProvider {
    db_name: &'static str,
}

impl StateVersionProvider {
    const DB_NAME_KEY: &'static [u8] = b"__db_name";
    const DB_VERSION_KEY: &'static [u8] = b"__db_version";
}

impl VersionProvider for StateVersionProvider {
    fn get_version(&self, db: &WeeDbRaw) -> Result<Option<Semver>, MigrationError> {
        let state = db.instantiate_table::<tables::State>();

        if let Some(db_name) = state.get(Self::DB_NAME_KEY)? {
            if db_name.as_ref() != self.db_name.as_bytes() {
                return Err(MigrationError::Custom(
                    format!(
                        "expected db name: {}, got: {}",
                        self.db_name,
                        String::from_utf8_lossy(db_name.as_ref())
                    )
                    .into(),
                ));
            }
        }

        let value = state.get(Self::DB_VERSION_KEY)?;
        match value {
            Some(version) => {
                let slice = version.as_ref();
                slice
                    .try_into()
                    .map_err(|_e| MigrationError::InvalidDbVersion)
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn set_version(&self, db: &WeeDbRaw, version: Semver) -> Result<(), MigrationError> {
        let state = db.instantiate_table::<tables::State>();

        state.insert(Self::DB_NAME_KEY, self.db_name.as_bytes())?;
        state.insert(Self::DB_VERSION_KEY, version)?;
        Ok(())
    }
}
