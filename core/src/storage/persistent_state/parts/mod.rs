use std::fs::File;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
pub use impls::*;
use tycho_block_util::block::{DisplayShardPrefix, ShardPrefix};
use tycho_block_util::state::RefMcStateHandle;
use tycho_types::cell::HashBytes;
use tycho_types::models::BlockId;
use tycho_util::FastHashMap;
use tycho_util::sync::CancellationFlag;

mod impls {
    pub use local_impl::PersistentStateStoragePartLocalImpl;

    mod local_impl;
}

pub trait PersistentStateStoragePart: Send + Sync {
    fn preload_state(&self, mc_seqno: u32, block_id: &BlockId) -> Result<()>;

    fn try_reuse_persistent_state(
        &self,
        mc_seqno: u32,
        block_id: BlockId,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send>>;

    fn store_shard_state_part(
        &self,
        cx: StoreStatePartContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoreStatePartResult>>> + Send>>;

    fn store_shard_state_part_file(
        &self,
        cx: StoreStatePartFileContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoreStatePartResult>>> + Send>>;

    fn state_part_size(&self, block_id: &BlockId) -> Result<Option<NonZeroU64>>;

    #[allow(clippy::type_complexity)]
    fn read_state_part_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>>> + Send>>;
}

pub struct StoreStatePartContext {
    pub mc_seqno: u32,
    pub block_id: BlockId,
    pub root_hash: HashBytes,
    pub tracker_handle: RefMcStateHandle,
    pub cancelled: Option<CancellationFlag>,
}

pub struct StoreStatePartFileContext {
    pub mc_seqno: u32,
    pub block_id: BlockId,
    pub file: File,
    pub cancelled: Option<CancellationFlag>,
}

#[allow(dead_code)]
pub struct StoreStatePartResult {
    pub prefix: ShardPrefix,
    pub file_size: usize,
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
