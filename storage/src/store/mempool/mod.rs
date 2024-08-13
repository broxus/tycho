use weedb::rocksdb::WriteBatch;

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

    pub fn new_batch() -> WriteBatch {
        WriteBatch::default()
    }
}
