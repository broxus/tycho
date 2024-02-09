use anyhow::Result;
use weedb::{Migrations, Semver, VersionProvider, WeeDb};

use super::tables;

// declare migrations here as `mod v2_1_x`

const CURRENT_VERSION: Semver = [2, 1, 0];

pub fn apply(db: &WeeDb) -> Result<()> {
    let migrations =
        Migrations::with_target_version_and_provider(CURRENT_VERSION, NodeStateVersionProvider);

    // === register migrations here ===
    // v2_1_1::register(&mut migrations).context("Failed to register migrations")?;

    db.apply(migrations)?;
    Ok(())
}

struct NodeStateVersionProvider;

impl NodeStateVersionProvider {
    const DB_VERSION_KEY: &'static str = "db_version";
}

impl VersionProvider for NodeStateVersionProvider {
    fn get_version(&self, db: &weedb::WeeDb) -> Result<Option<Semver>, weedb::Error> {
        let state = db.instantiate_table::<tables::NodeStates>();

        let value = state.get(Self::DB_VERSION_KEY)?;
        match value {
            Some(version) => {
                let slice = version.as_ref();
                slice
                    .try_into()
                    .map_err(|_| weedb::Error::InvalidDbVersion)
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn set_version(&self, db: &weedb::WeeDb, version: Semver) -> Result<(), weedb::Error> {
        let state = db.instantiate_table::<tables::NodeStates>();

        state.insert(Self::DB_VERSION_KEY, version)?;
        Ok(())
    }
}