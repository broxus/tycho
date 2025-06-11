use tycho_storage::{Migrations, WithMigrations};
use tycho_util::sync::CancellationFlag;
use weedb::{Caches, MigrationError, Semver, WeeDb};

use super::tables;

pub type MempoolDb = WeeDb<MempoolTables>;

impl WithMigrations for MempoolTables {
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
