use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
pub use impls::*;
use tycho_block_util::block::DisplayShardPrefix;
use tycho_block_util::state::RefMcStateHandle;
use tycho_types::cell::HashBytes;
use tycho_types::models::BlockId;
use tycho_util::FastHashMap;
use tycho_util::sync::CancellationFlag;

use crate::storage::shard_state::ShardPrefix;

mod impls {
    pub use local_impl::PersistentStateStoragePartLocalImpl;

    mod local_impl;
}

pub trait PersistentStateStoragePart: Send + Sync {
    fn try_reuse_persistent_state(
        &self,
        mc_seqno: u32,
        block_id: BlockId,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send>>;

    fn store_shard_state_part(
        &self,
        cx: StoreStatePartContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoreStatePartResult>>> + Send>>;
}

pub struct StoreStatePartContext {
    pub mc_seqno: u32,
    pub block_id: BlockId,
    pub root_hash: HashBytes,
    pub tracker_handle: RefMcStateHandle,
    pub cancelled: Option<CancellationFlag>,
}

pub struct StoreStatePartResult {
    prefix: ShardPrefix,
    file_size: usize,
}

pub type PersistentStoragePartsMap = FastHashMap<ShardPrefix, Arc<dyn PersistentStateStoragePart>>;

pub trait OptionalPersistentStoragePartsMapExt {
    fn try_as_ref_ext(&self) -> Result<&Arc<PersistentStoragePartsMap>>;
}
impl OptionalPersistentStoragePartsMapExt for Option<Arc<PersistentStoragePartsMap>> {
    fn try_as_ref_ext(&self) -> Result<&Arc<PersistentStoragePartsMap>> {
        let storage_parts = self
            .as_ref()
            .context("persistent parts storage not configured")?;
        Ok(storage_parts)
    }
}

pub trait PersistentStoragePartsMapExt {
    fn try_get_ext(&self, prefix: &ShardPrefix) -> Result<&Arc<dyn PersistentStateStoragePart>>;
}
impl PersistentStoragePartsMapExt for PersistentStoragePartsMap {
    fn try_get_ext(&self, prefix: &ShardPrefix) -> Result<&Arc<dyn PersistentStateStoragePart>> {
        let Some(storage_part) = self.get(prefix) else {
            anyhow::bail!(
                "persistent part storage not configured for shard {}",
                &DisplayShardPrefix(prefix)
            );
        };
        Ok(storage_part)
    }
}
