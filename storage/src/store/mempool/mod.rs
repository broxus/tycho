use crate::MempoolDb;

#[derive(Clone)]
pub struct MempoolStorage {
    pub db: MempoolDb,
}
impl MempoolStorage {
    pub fn new(db: MempoolDb) -> Self {
        Self { db }
    }
}
