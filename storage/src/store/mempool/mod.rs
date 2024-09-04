use crate::MempoolDb;

#[derive(Clone)]
pub struct MempoolStorage {
    pub db: MempoolDb,
}
impl MempoolStorage {
    pub const KEY_LEN: usize = 4 + 32;
}
