use weedb::rocksdb::{ReadOptions, WriteBatch};

use crate::MempoolDb;

#[derive(Clone)]
pub struct MempoolStorage {
    pub db: MempoolDb,
}
impl MempoolStorage {
    pub const KEY_LEN: usize = 4 + 32;

    pub fn new(db: MempoolDb) -> Self {
        Self { db }
    }

    pub fn write_batch() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn read_options() -> ReadOptions {
        ReadOptions::default()
    }
}
