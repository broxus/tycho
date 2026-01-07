use std::collections::BTreeMap;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use tycho_block_util::block::ShardPrefix;
use tycho_storage::fs::Dir;
use tycho_types::models::BlockId;
use tycho_util::fs::MappedFile;
use tycho_util::{FastDashMap, FastHashSet};

use super::PersistentStateKind;
use crate::storage::shard_state::ShardStatePartInfo;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct CacheKey {
    block_id: BlockId,
    kind: PersistentStateKind,
}

impl From<(BlockId, PersistentStateKind)> for CacheKey {
    #[inline]
    fn from((block_id, kind): (BlockId, PersistentStateKind)) -> Self {
        Self { block_id, kind }
    }
}

impl From<(&BlockId, PersistentStateKind)> for CacheKey {
    #[inline]
    fn from((block_id, kind): (&BlockId, PersistentStateKind)) -> Self {
        Self::from((*block_id, kind))
    }
}

#[derive(Clone, Debug)]
pub struct CachedStatePartsInfo {
    pub split_depth: u8,
    pub parts: Vec<CachedStatePartInfo>,
}

#[derive(Copy, Clone, Debug)]
pub struct CachedStatePartInfo {
    pub prefix: ShardPrefix,
}

impl From<ShardStatePartInfo> for CachedStatePartInfo {
    fn from(value: ShardStatePartInfo) -> Self {
        Self {
            prefix: value.prefix,
        }
    }
}

pub struct CachedState {
    pub mc_seqno: u32,
    pub file: MappedFile,

    pub parts_info: Option<CachedStatePartsInfo>,
}

pub struct DescriptorCache {
    inner: Arc<Inner>,
}

impl DescriptorCache {
    pub fn new(storage_dir: Dir) -> Self {
        Self {
            inner: Arc::new(Inner {
                storage_dir,
                descriptor_cache: Default::default(),
                mc_seqno_to_block_ids: Default::default(),
            }),
        }
    }

    pub fn storage_dir(&self) -> &Dir {
        &self.inner.storage_dir
    }

    #[cfg(test)]
    pub fn mc_states_dir(&self, mc_seqno: u32) -> Dir {
        self.inner.mc_states_dir(mc_seqno)
    }

    #[cfg(test)]
    pub fn mc_seqno_to_block_ids(&self) -> &Mutex<BTreeMap<u32, FastHashSet<BlockId>>> {
        &self.inner.mc_seqno_to_block_ids
    }

    pub fn contains_key(&self, key: &CacheKey) -> bool {
        self.inner.descriptor_cache.contains_key(key)
    }

    pub fn get(&self, key: &CacheKey) -> Option<Arc<CachedState>> {
        self.inner.descriptor_cache.get(key).as_deref().cloned()
    }

    pub fn get_all_states(&self) -> Vec<PersistentState> {
        self.inner
            .descriptor_cache
            .iter()
            .map(|item| PersistentState {
                block_id: item.key().block_id,
                kind: item.key().kind,
                cached: item.value().clone(),
            })
            .collect()
    }

    pub fn prepare_persistent_states_dir(&self, mc_seqno: u32) -> Result<Dir> {
        self.inner.prepare_persistent_states_dir(mc_seqno)
    }

    pub fn remove_outdated_cached_states(
        &self,
        recent_block_id: &BlockId,
        zerostate_seqno: u32,
    ) -> Result<Vec<BlockId>> {
        self.inner
            .remove_outdated_cached_states(recent_block_id, zerostate_seqno)
    }

    pub fn cache_shard_state(
        &self,
        mc_seqno: u32,
        block_id: &BlockId,
        part_shard_prefix: Option<ShardPrefix>,
        parts_info: Option<Vec<ShardStatePartInfo>>,
        part_split_depth: u8,
    ) -> Result<PersistentState> {
        let parts_info = parts_info.map(|v| CachedStatePartsInfo {
            split_depth: part_split_depth,
            parts: v.into_iter().map(CachedStatePartInfo::from).collect(),
        });
        self.cache_state(
            mc_seqno,
            block_id,
            PersistentStateKind::Shard,
            part_shard_prefix,
            parts_info,
        )
    }

    pub fn cache_queue_state(&self, mc_seqno: u32, block_id: &BlockId) -> Result<PersistentState> {
        self.cache_state(mc_seqno, block_id, PersistentStateKind::Queue, None, None)
    }

    pub fn cache_state(
        &self,
        mc_seqno: u32,
        block_id: &BlockId,
        kind: PersistentStateKind,
        part_shard_prefix: Option<ShardPrefix>,
        parts_info: Option<CachedStatePartsInfo>,
    ) -> Result<PersistentState> {
        self.inner
            .cache_state(mc_seqno, block_id, kind, part_shard_prefix, parts_info)
    }

    pub async fn try_reuse_persistent_state(
        &self,
        mc_seqno: u32,
        block_id: BlockId,
        kind: PersistentStateKind,
        part_shard_prefix: Option<ShardPrefix>,
    ) -> Result<Option<ReusePersistentStateResult>> {
        let Some(cached) = self.get(&CacheKey::from((block_id, kind))) else {
            // Nothing to reuse
            return Ok(None);
        };

        if cached.mc_seqno >= mc_seqno {
            // We already have the recent enough state
            return Ok(Some(ReusePersistentStateResult::OldCached(
                PersistentState {
                    block_id,
                    kind,
                    cached,
                },
            )));
        }

        let this = self.inner.clone();

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

            let temp_file =
                states_dir.file(kind.make_temp_file_name(&block_id, part_shard_prefix.as_ref()));
            std::fs::write(temp_file.path(), cached.file.as_slice())?;
            temp_file.rename(kind.make_file_name(&block_id, part_shard_prefix.as_ref()))?;

            let parts_info = cached.parts_info.clone();

            drop(cached);

            let new_cached =
                this.cache_state(mc_seqno, &block_id, kind, part_shard_prefix, parts_info)?;

            Ok(Some(ReusePersistentStateResult::NewCached(new_cached)))
        })
        .await?
    }
}

struct Inner {
    storage_dir: Dir,
    descriptor_cache: FastDashMap<CacheKey, Arc<CachedState>>,
    mc_seqno_to_block_ids: Mutex<BTreeMap<u32, FastHashSet<BlockId>>>,
}

impl Inner {
    fn prepare_persistent_states_dir(&self, mc_seqno: u32) -> Result<Dir> {
        let states_dir = self.mc_states_dir(mc_seqno);
        if !states_dir.path().is_dir() {
            tracing::info!(mc_seqno, "creating persistent state directory");
            states_dir.create_if_not_exists()?;
        }
        Ok(states_dir)
    }

    fn mc_states_dir(&self, mc_seqno: u32) -> Dir {
        Dir::new_readonly(self.storage_dir.path().join(mc_seqno.to_string()))
    }

    /// Removes outdated persistent states (cache and files).
    /// Returns block ids of removed persistent states.
    fn remove_outdated_cached_states(
        &self,
        recent_block_id: &BlockId,
        zerostate_seqno: u32,
    ) -> Result<Vec<BlockId>> {
        let mut removed_states_block_ids = vec![];

        let mut index = self.mc_seqno_to_block_ids.lock();
        index.retain(|&mc_seqno, block_ids| {
            if mc_seqno >= recent_block_id.seqno || mc_seqno <= zerostate_seqno {
                return true;
            }

            for block_id in block_ids.drain() {
                self.clear_cache(&block_id);
                removed_states_block_ids.push(block_id);
            }
            false
        });

        // Remove files
        self.clear_outdated_state_entries(recent_block_id, zerostate_seqno)?;

        Ok(removed_states_block_ids)
    }

    fn clear_outdated_state_entries(
        &self,
        recent_block_id: &BlockId,
        zerostate_seqno: u32,
    ) -> Result<()> {
        let mut directories_to_remove: Vec<PathBuf> = Vec::new();
        let mut files_to_remove: Vec<PathBuf> = Vec::new();

        for entry in self.storage_dir.entries()?.flatten() {
            let path = entry.path();

            if path.is_file() {
                files_to_remove.push(path);
                continue;
            }

            let Ok(name) = entry.file_name().into_string() else {
                directories_to_remove.push(path);
                continue;
            };

            let is_recent = matches!(
                name.parse::<u32>(),
                Ok(seqno) if seqno >= recent_block_id.seqno || seqno <= zerostate_seqno
            );
            if !is_recent {
                directories_to_remove.push(path);
            }
        }

        for dir in directories_to_remove {
            tracing::info!(dir = %dir.display(), "removing an old persistent state directory");
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                tracing::error!(dir = %dir.display(), "failed to remove an old persistent state: {e:?}");
            }
        }

        for file in files_to_remove {
            tracing::info!(file = %file.display(), "removing file");
            if let Err(e) = std::fs::remove_file(&file) {
                tracing::error!(file = %file.display(), "failed to remove file: {e:?}");
            }
        }

        Ok(())
    }

    fn cache_state(
        &self,
        mc_seqno: u32,
        block_id: &BlockId,
        kind: PersistentStateKind,
        part_shard_prefix: Option<ShardPrefix>,
        parts_info: Option<CachedStatePartsInfo>,
    ) -> Result<PersistentState> {
        use std::collections::btree_map;

        use dashmap::mapref::entry::Entry;

        let key = CacheKey::from((block_id, kind));

        let load_mapped = || {
            let mut file = self
                .mc_states_dir(mc_seqno)
                .file(kind.make_file_name(block_id, part_shard_prefix.as_ref()))
                .read(true)
                .open()?;

            // We create a copy of the original file here to make sure
            // that the underlying mapped file will not be changed outside
            // of the node. Otherwise it will randomly fail with exit code 7/BUS.
            let mut temp_file = tempfile::tempfile_in(self.storage_dir.path())
                .context("failed to create a temp file")?;

            // Underlying implementation will call something like `copy_file_range`,
            // and we hope that it will be just COW pages.
            // TODO: Find a way to cancel this operation.
            std::io::copy(&mut file, &mut temp_file).context("failed to copy a temp file")?;
            temp_file.flush()?;
            temp_file.seek(std::io::SeekFrom::Start(0))?;

            MappedFile::from_existing_file(temp_file).context("failed to map a temp file")
        };

        let file =
            load_mapped().with_context(|| format!("failed to cache {kind:?} for {block_id}"))?;

        let new_state = Arc::new(CachedState {
            mc_seqno,
            file,
            parts_info,
        });

        let prev_mc_seqno = match self.descriptor_cache.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(new_state.clone());
                None
            }
            Entry::Occupied(mut entry) => {
                let prev_mc_seqno = entry.get().mc_seqno;
                if mc_seqno <= prev_mc_seqno {
                    // Cache only the most recent block (if changed)
                    return Ok(PersistentState {
                        block_id: *block_id,
                        kind,
                        cached: entry.get().clone(),
                    });
                }

                entry.insert(new_state.clone());
                Some(prev_mc_seqno)
            }
        };

        // Remove previous entry if exists
        {
            let mut index = self.mc_seqno_to_block_ids.lock();
            if let Some(prev_mc_seqno) = prev_mc_seqno
                && let btree_map::Entry::Occupied(mut entry) = index.entry(prev_mc_seqno)
            {
                entry.get_mut().remove(block_id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }

            index.entry(mc_seqno).or_default().insert(*block_id);
        }

        Ok(PersistentState {
            block_id: *block_id,
            kind,
            cached: new_state,
        })
    }

    fn clear_cache(&self, block_id: &BlockId) {
        self.descriptor_cache
            .remove(&CacheKey::from((block_id, PersistentStateKind::Shard)));
        self.descriptor_cache
            .remove(&CacheKey::from((block_id, PersistentStateKind::Queue)));
    }
}

pub enum ReusePersistentStateResult {
    OldCached(PersistentState),
    NewCached(PersistentState),
}

impl ReusePersistentStateResult {
    pub fn state(&self) -> &PersistentState {
        match self {
            Self::OldCached(cached) | Self::NewCached(cached) => cached,
        }
    }

    pub fn into_state(self) -> PersistentState {
        match self {
            Self::OldCached(cached) | Self::NewCached(cached) => cached,
        }
    }
}

#[derive(Clone)]
pub struct PersistentState {
    block_id: BlockId,
    kind: PersistentStateKind,
    cached: Arc<CachedState>,
}

impl PersistentState {
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn kind(&self) -> PersistentStateKind {
        self.kind
    }

    pub fn cached(&self) -> &Arc<CachedState> {
        &self.cached
    }

    pub fn file(&self) -> &MappedFile {
        &self.cached.file
    }

    pub fn mc_seqno(&self) -> u32 {
        self.cached.mc_seqno
    }
}
