use std::path::Path;

use weedb::{
    Caches, MigrationError, Semver, Tables, VersionProvider, WeeDb, WeeDbBuilder, WeeDbRaw,
};

pub mod tables;

pub trait WeeDbExt<T: Tables>: Sized {
    fn builder_prepared<P: AsRef<Path>>(path: P, context: T::Context) -> WeeDbBuilder<T>;

    fn apply_migrations(&self) -> Result<(), MigrationError>;
}

impl<T: Tables> WeeDbExt<T> for WeeDb<T>
where
    Self: WithMigrations,
{
    fn builder_prepared<P: AsRef<Path>>(
        path: P,
        context: <T as Tables>::Context,
    ) -> WeeDbBuilder<T> {
        WeeDbBuilder::new(path, context).with_name(Self::NAME)
    }

    fn apply_migrations(&self) -> Result<(), MigrationError> {
        let mut migrations = Migrations::<Self>::with_target_version_and_provider(
            Self::VERSION,
            StateVersionProvider {
                db_name: Self::NAME,
            },
        );
        Self::register_migrations(&mut migrations)?;
        self.apply(migrations)
    }
}

// === Base DB ===

pub type BaseDb = WeeDb<BaseTables>;

impl WithMigrations for BaseDb {
    const NAME: &'static str = "base";
    const VERSION: Semver = [0, 0, 1];

    fn register_migrations(_migrations: &mut Migrations<Self>) -> Result<(), MigrationError> {
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
        pub package_entries: tables::PackageEntries,
        pub block_data_entries: tables::BlockDataEntries,
        pub shard_states: tables::ShardStates,
        pub cell_data: tables::CellData,
        pub cell_refs: tables::CellRefs,
        pub temp_cells: tables::TempCells,
        pub block_connections: tables::BlockConnections,
        pub shards_internal_messages: tables::ShardsInternalMessages,
        pub shards_internal_messages_session: tables::ShardsInternalMessagesSession,
    }
}

// === RPC DB ===

pub type RpcDb = WeeDb<RpcTables>;

impl WithMigrations for RpcDb {
    const NAME: &'static str = "rpc";
    const VERSION: Semver = [0, 0, 1];

    fn register_migrations(_migrations: &mut Migrations<Self>) -> Result<(), MigrationError> {
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

    fn register_migrations(_migrations: &mut Migrations<Self>) -> Result<(), MigrationError> {
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

    fn register_migrations(migrations: &mut Migrations<Self>) -> Result<(), MigrationError>;
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
