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
    const VERSION: Semver = [0, 1, 0];

    fn register_migrations(
        _migrations: &mut Migrations<Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        // TODO: register migrations here
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
        pub block_connections: tables::BlockConnections,
    }
}

// === RPC DB ===

pub type RpcDb = WeeDb<RpcTables>;

impl WithMigrations for RpcDb {
    const NAME: &'static str = "rpc";
    const VERSION: Semver = [0, 1, 0];

    fn register_migrations(
        _migrations: &mut Migrations<Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        // TODO: register migrations here
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

// ===  Internal Queue DB ===

pub type InternalQueueDB = WeeDb<InternalQueueTables>;

impl WithMigrations for InternalQueueDB {
    const NAME: &'static str = "int_queue";
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
    pub struct InternalQueueTables<Caches> {
        pub internal_message_var: tables::InternalMessageVar,
        pub internal_message_diffs_tail: tables::InternalMessageDiffsTail,
        pub internal_message_diff_info: tables::InternalMessageDiffInfo,
        pub internal_message_commit_pointer: tables::InternalMessageCommitPointer,
        pub internal_message_stats: tables::InternalMessageStatistics,
        pub shard_internal_messages: tables::ShardInternalMessages,
    }
}

// ===  Cells Db ===

pub type CellsDb = WeeDb<CellsTables>;

impl WithMigrations for CellsDb {
    const NAME: &'static str = "cells";
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
    pub struct CellsTables<Caches> {
        pub cells: tables::Cells,
        pub temp_cells: tables::TempCells,
        pub shard_states: tables::ShardStates,
    }
}
