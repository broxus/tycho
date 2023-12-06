use mini_moka::sync::Cache;

pub struct Storage {
    _storage: Cache<StorageKeyId, ahash::RandomState>,
}

pub type StorageKeyId = [u8; 32];
