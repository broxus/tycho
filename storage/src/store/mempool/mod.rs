use anyhow::{Context, Result};
use weedb::rocksdb::{ReadOptions, WriteBatch};

use crate::MempoolDb;

#[derive(Clone)]
pub struct MempoolStorage {
    pub db: MempoolDb,
}
impl MempoolStorage {
    pub const KEY_LEN: usize = 4 + 32;

    pub async fn new(db: MempoolDb) -> Result<Self> {
        tokio::task::spawn_blocking({
            let db = db.clone();
            move || Self::clean_all(db)
        })
        .await??;

        Ok(Self { db })
    }

    fn clean_all(db: MempoolDb) -> Result<()> {
        let flags_cf = db.tables().points_flags.cf();
        let info_cf = db.tables().points_info.cf();
        let points_cf = db.tables().points.cf();

        let min = [u8::MIN; Self::KEY_LEN];
        let max = [u8::MAX; Self::KEY_LEN];

        db.rocksdb()
            .delete_file_in_range_cf(&flags_cf, min, max)
            .context("delete old point flags")?;

        db.rocksdb()
            .delete_file_in_range_cf(&info_cf, min, max)
            .context("delete old point info")?;

        db.rocksdb()
            .delete_file_in_range_cf(&points_cf, min, max)
            .context("delete old point data")?;

        Ok(())
    }

    pub fn write_batch() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn read_options() -> ReadOptions {
        ReadOptions::default()
    }
}
