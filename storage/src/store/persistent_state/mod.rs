use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{Seek, Write};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwapAny;
use dashmap::DashMap;
use everscale_types::models::{BlockId, PrevBlockRef};
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tokio::time::Instant;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::QueueStateHeader;
use tycho_block_util::state::RefMcStateHandle;
use tycho_util::sync::CancellationFlag;
use tycho_util::FastHashSet;

pub use self::queue_state::reader::{QueueDiffReader, QueueStateReader};
pub use self::queue_state::writer::QueueStateWriter;
pub use self::shard_state::reader::{BriefBocHeader, ShardStateReader};
pub use self::shard_state::writer::ShardStateWriter;
use super::{KeyBlocksDirection, ShardStateStorage};
use crate::db::{BaseDb, FileDb, MappedFile};
use crate::store::{BlockHandle, BlockHandleStorage, BlockStorage};

mod queue_state {
    pub mod reader;
    pub mod writer;
}
mod shard_state {
    pub mod reader;
    pub mod writer;
}

#[cfg(test)]
mod tests;

const BASE_DIR: &str = "states";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PersistentStateKind {
    Shard,
    Queue,
}

impl PersistentStateKind {
    fn make_file_name(&self, block_id: &BlockId) -> PathBuf {
        match self {
            Self::Shard => ShardStateWriter::file_name(block_id),
            Self::Queue => QueueStateWriter::file_name(block_id),
        }
    }

    fn make_temp_file_name(&self, block_id: &BlockId) -> PathBuf {
        match self {
            Self::Shard => ShardStateWriter::temp_file_name(block_id),
            Self::Queue => QueueStateWriter::temp_file_name(block_id),
        }
    }

    fn from_extension(extension: &str) -> Option<Self> {
        match extension {
            ShardStateWriter::FILE_EXTENSION => Some(Self::Shard),
            QueueStateWriter::FILE_EXTENSION => Some(Self::Queue),
            _ => None,
        }
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    block_id: BlockId,
    kind: PersistentStateKind,
}

#[derive(Clone)]
pub struct PersistentStateStorage {
    inner: Arc<Inner>,
}

impl PersistentStateStorage {
    pub fn new(
        db: BaseDb,
        files_dir: &FileDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        shard_state_storage: Arc<ShardStateStorage>,
    ) -> Result<Self> {
        const MAX_PARALLEL_CHUNK_READS: usize = 20;

        let storage_dir = files_dir.create_subdir(BASE_DIR)?;

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                storage_dir,
                block_handles: block_handle_storage,
                blocks: block_storage,
                shard_states: shard_state_storage,
                descriptor_cache: Default::default(),
                mc_seqno_to_block_ids: Default::default(),
                chunks_semaphore: Semaphore::new(MAX_PARALLEL_CHUNK_READS),
                handles_queue: Default::default(),
                oldest_ps_changed: Default::default(),
                oldest_ps_handle: Default::default(),
            }),
        })
    }

    pub fn load_oldest_known_handle(&self) -> Option<BlockHandle> {
        self.inner.oldest_ps_handle.load_full()
    }

    pub fn oldest_known_handle_changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.oldest_ps_changed.notified()
    }

    #[tracing::instrument(skip_all)]
    pub async fn preload(&self) -> Result<()> {
        self.preload_handles_queue()?;
        self.preload_states().await
    }

    fn preload_handles_queue(&self) -> Result<()> {
        let this = self.inner.as_ref();

        let block_handles = this.block_handles.as_ref();

        let mut changed = false;
        let mut prev_utime = 0;
        for block_id in block_handles.key_blocks_iterator(KeyBlocksDirection::ForwardFrom(0)) {
            let block_handle = block_handles
                .load_handle(&block_id)
                .context("key block handle not found")?;

            let gen_utime = block_handle.gen_utime();
            if BlockStuff::compute_is_persistent(gen_utime, prev_utime) {
                prev_utime = gen_utime;

                let mut queue = this.handles_queue.lock();
                if queue.push(block_handle) {
                    this.oldest_ps_handle.store(queue.oldest_known().cloned());
                    changed = true;
                }
            }
        }

        if changed {
            this.oldest_ps_changed.notify_waiters();
        }
        Ok(())
    }

    async fn preload_states(&self) -> Result<()> {
        // For each mc_seqno directory
        let process_states = |this: &Inner, dir: &PathBuf, mc_seqno: u32| -> Result<()> {
            'outer: for entry in std::fs::read_dir(dir)?.flatten() {
                let path = entry.path();
                // Skip subdirectories
                if path.is_dir() {
                    tracing::warn!(path = %path.display(), "unexpected directory");
                    continue;
                }

                'file: {
                    // Try to parse the file name as a block_id
                    let Ok(block_id) = path
                        // TODO should use file_prefix
                        .file_stem()
                        .unwrap_or_default()
                        .to_str()
                        .unwrap_or_default()
                        .parse::<BlockId>()
                    else {
                        break 'file;
                    };

                    let extension = path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .unwrap_or_default();

                    let Some(cache_type) = PersistentStateKind::from_extension(extension) else {
                        break 'file;
                    };

                    this.cache_state(mc_seqno, &block_id, cache_type)?;
                    continue 'outer;
                }
                tracing::warn!(path = %path.display(), "unexpected file");
            }
            Ok(())
        };

        let this = self.inner.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            // For each entry in the storage directory
            'outer: for entry in this.storage_dir.entries()?.flatten() {
                let path = entry.path();
                // Skip files
                if path.is_file() {
                    tracing::warn!(path = %path.display(), "unexpected file");
                    continue;
                }

                'dir: {
                    // Try to parse the directory name as an mc_seqno
                    let Ok(name) = entry.file_name().into_string() else {
                        break 'dir;
                    };
                    let Ok(mc_seqno) = name.parse::<u32>() else {
                        break 'dir;
                    };

                    // Try to load files in the directory as persistent states
                    process_states(&this, &path, mc_seqno)?;
                    continue 'outer;
                }
                tracing::warn!(path = %path.display(), "unexpected directory");
            }

            Ok(())
        })
        .await?
    }

    // NOTE: This is intentionally a method, not a constant because
    // it might be useful to allow configure it during the first run.
    pub fn state_chunk_size(&self) -> NonZeroU32 {
        NonZeroU32::new(STATE_CHUNK_SIZE as _).unwrap()
    }

    pub fn state_exists(&self, block_id: &BlockId, kind: PersistentStateKind) -> bool {
        self.inner.descriptor_cache.contains_key(&CacheKey {
            block_id: *block_id,
            kind,
        })
    }

    pub fn get_state_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Option<PersistentStateInfo> {
        self.inner
            .descriptor_cache
            .get(&CacheKey {
                block_id: *block_id,
                kind,
            })
            .and_then(|cached| {
                let size = NonZeroU64::new(cached.file.length() as u64)?;
                Some(PersistentStateInfo {
                    size,
                    chunk_size: self.state_chunk_size(),
                })
            })
    }

    pub async fn read_state_part(
        &self,
        block_id: &BlockId,
        offset: u64,
        state_kind: PersistentStateKind,
    ) -> Option<Vec<u8>> {
        // NOTE: Should be noop on x64
        let offset = usize::try_from(offset).ok()?;
        let chunk_size = self.state_chunk_size().get() as usize;
        if offset % chunk_size != 0 {
            return None;
        }

        let _permit = self.inner.chunks_semaphore.acquire().await.ok()?;

        let key = CacheKey {
            block_id: *block_id,
            kind: state_kind,
        };
        let cached = self.inner.descriptor_cache.get(&key)?.clone();
        if offset > cached.file.length() {
            return None;
        }

        // NOTE: Cached file is a mapped file, therefore it can take a while to read from it.
        // NOTE: `spawn_blocking` is called here because it is mostly IO-bound operation.
        // TODO: Add semaphore to limit the number of concurrent operations.
        tokio::task::spawn_blocking(move || {
            let end = std::cmp::min(offset.saturating_add(chunk_size), cached.file.length());
            cached.file.as_slice()[offset..end].to_vec()
        })
        .await
        .ok()
    }

    #[tracing::instrument(skip_all, fields(mc_seqno, block_id = %handle.id()))]
    pub async fn store_shard_state(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        tracker_handle: RefMcStateHandle,
    ) -> Result<()> {
        if self
            .try_reuse_persistent_state(mc_seqno, handle, PersistentStateKind::Shard)
            .await?
        {
            return Ok(());
        }

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        let handle = handle.clone();
        let this = self.inner.clone();
        let cancelled = cancelled.clone();
        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            // NOTE: Ensure that the tracker handle will outlive the state writer.
            let _tracker_handle = tracker_handle;

            let root_hash = this.shard_states.load_state_root(handle.id())?;

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

            let cell_writer = ShardStateWriter::new(&this.db, &states_dir, handle.id());
            match cell_writer.write(&root_hash, Some(&cancelled)) {
                Ok(()) => {
                    this.block_handles.set_has_persistent_shard_state(&handle);
                    tracing::info!("persistent shard state saved");
                }
                Err(e) => {
                    // NOTE: We are ignoring an error here. It might be intentional
                    tracing::error!("failed to write persistent shard state: {e:?}");
                }
            }

            this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Shard)?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok(())
        })
        .await?
    }

    #[tracing::instrument(skip_all, fields(mc_seqno, block_id = %handle.id()))]
    pub async fn store_shard_state_file(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        file: File,
    ) -> Result<()> {
        if self
            .try_reuse_persistent_state(mc_seqno, handle, PersistentStateKind::Shard)
            .await?
        {
            return Ok(());
        }

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        let handle = handle.clone();
        let this = self.inner.clone();
        let cancelled = cancelled.clone();
        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

            let cell_writer = ShardStateWriter::new(&this.db, &states_dir, handle.id());
            cell_writer.write_file(file, Some(&cancelled))?;
            this.block_handles.set_has_persistent_shard_state(&handle);
            this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Shard)?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok(())
        })
        .await?
    }

    #[tracing::instrument(skip_all, fields(mc_seqno = mc_seqno, block_id = %block.id()))]
    pub async fn store_queue_state(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        block: BlockStuff,
    ) -> Result<()> {
        if self
            .try_reuse_persistent_state(mc_seqno, handle, PersistentStateKind::Queue)
            .await?
        {
            return Ok(());
        }

        let this = self.inner.clone();

        let shard_ident = handle.id().shard;

        let mut queue_diffs = Vec::new();
        let mut messages = Vec::new();

        let mut top_block_handle = handle.clone();
        let mut top_block = block;

        let mut tail_len = top_block.block().out_msg_queue_updates.tail_len as usize;

        while tail_len > 0 {
            let queue_diff = this.blocks.load_queue_diff(&top_block_handle).await?;
            let top_block_info = top_block.load_info()?;

            let block_extra = top_block.load_extra()?;
            let out_messages = block_extra.load_out_msg_description()?;

            messages.push(queue_diff.zip(&out_messages));
            queue_diffs.push(queue_diff.diff().clone());

            if tail_len == 1 {
                break;
            }

            let prev_block_id = match top_block_info.load_prev_ref()? {
                PrevBlockRef::Single(block_ref) => block_ref.as_block_id(shard_ident),
                PrevBlockRef::AfterMerge { .. } => anyhow::bail!("merge not supported yet"),
            };

            let Some(prev_block_handle) = this.block_handles.load_handle(&prev_block_id) else {
                anyhow::bail!("prev block handle not found for: {prev_block_id}");
            };
            let prev_block = this.blocks.load_block_data(&prev_block_handle).await?;

            top_block_handle = prev_block_handle;
            top_block = prev_block;
            tail_len -= 1;
        }

        let state = QueueStateHeader {
            shard_ident,
            seqno: handle.id().seqno,
            queue_diffs,
        };

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        let handle = handle.clone();
        let cancelled = cancelled.clone();
        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;
            match QueueStateWriter::new(&states_dir, handle.id(), state, messages)
                .write(Some(&cancelled))
            {
                Ok(()) => {
                    this.block_handles.set_has_persistent_queue_state(&handle);
                    tracing::info!("persistent queue state saved");
                }
                Err(e) => {
                    tracing::error!("failed to write persistent queue state: {e:?}");
                }
            }

            this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Queue)?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok(())
        })
        .await?
    }

    pub async fn rotate_persistent_states(&self, top_handle: &BlockHandle) -> Result<()> {
        anyhow::ensure!(
            top_handle.is_masterchain(),
            "top persistent state handle must be in the masterchain"
        );

        {
            tracing::info!(
                mc_block_id = %top_handle.id(),
                "adding new persistent state to the queue"
            );

            let mut queue = self.inner.handles_queue.lock();
            if queue.push(top_handle.clone()) {
                self.inner
                    .oldest_ps_handle
                    .store(queue.oldest_known().cloned());
                self.inner.oldest_ps_changed.notify_waiters();
            }
        }

        tracing::info!("started clearing old persistent state directories");
        let start = Instant::now();
        scopeguard::defer! {
            tracing::info!(
                elapsed = %humantime::format_duration(start.elapsed()),
                "clearing old persistent state directories completed"
            );
        }

        let this = self.inner.clone();
        let mut top_handle = top_handle.clone();
        if top_handle.id().seqno == 0 {
            // Nothing to clear for the zerostate
            return Ok(());
        }

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let block_handles = &this.block_handles;

            let now_utime = top_handle.gen_utime();

            // Find a state before the
            let mut has_suitable = false;
            loop {
                match block_handles.find_prev_persistent_key_block(top_handle.id().seqno) {
                    // Find the newest usable persistent state...
                    Some(handle) if !has_suitable => {
                        has_suitable |= BlockStuff::can_use_for_boot(handle.gen_utime(), now_utime);
                        top_handle = handle;
                    }
                    // ...and return the previous one.
                    Some(handle) => {
                        top_handle = handle;
                        break;
                    }
                    // Or do nothing if not found.
                    None => return Ok(()),
                }
            }

            // Remove cached states
            let mut index = this.mc_seqno_to_block_ids.lock();
            index.retain(|&mc_seqno, block_ids| {
                if mc_seqno >= top_handle.id().seqno || mc_seqno == 0 {
                    return true;
                }

                for block_id in block_ids.drain() {
                    // TODO: Clear flag in block handle
                    this.clear_cache(&block_id);
                }
                false
            });

            // Remove files
            this.clear_outdated_state_entries(top_handle.id())
        })
        .await?
    }

    async fn try_reuse_persistent_state(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        kind: PersistentStateKind,
    ) -> Result<bool> {
        // Check if there is anything to reuse (return false if nothing)
        match kind {
            PersistentStateKind::Shard if !handle.has_persistent_shard_state() => return Ok(false),
            PersistentStateKind::Queue if !handle.has_persistent_queue_state() => return Ok(false),
            _ => {}
        }

        let block_id = *handle.id();

        let Some(cached) = self
            .inner
            .descriptor_cache
            .get(&CacheKey { block_id, kind })
            .map(|r| r.clone())
        else {
            // Nothing to reuse
            return Ok(false);
        };

        if cached.mc_seqno >= mc_seqno {
            // We already have the recent enough state
            return Ok(true);
        }

        let this = self.inner.clone();

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

            let temp_file = states_dir.file(kind.make_temp_file_name(&block_id));
            std::fs::write(temp_file.path(), cached.file.as_slice())?;
            temp_file.rename(kind.make_file_name(&block_id))?;

            drop(cached);

            this.cache_state(mc_seqno, &block_id, kind)?;
            Ok(true)
        })
        .await?
    }
}

struct Inner {
    db: BaseDb,
    storage_dir: FileDb,
    block_handles: Arc<BlockHandleStorage>,
    blocks: Arc<BlockStorage>,
    shard_states: Arc<ShardStateStorage>,
    descriptor_cache: DashMap<CacheKey, Arc<CachedState>>,
    mc_seqno_to_block_ids: Mutex<BTreeMap<u32, FastHashSet<BlockId>>>,
    chunks_semaphore: Semaphore,
    handles_queue: Mutex<HandlesQueue>,
    oldest_ps_changed: Notify,
    oldest_ps_handle: ArcSwapAny<Option<BlockHandle>>,
}

impl Inner {
    fn prepare_persistent_states_dir(&self, mc_seqno: u32) -> Result<FileDb> {
        let states_dir = self.mc_states_dir(mc_seqno);
        if !states_dir.path().is_dir() {
            tracing::info!(mc_seqno, "creating persistent state directory");
            states_dir.create_if_not_exists()?;
        }
        Ok(states_dir)
    }

    fn mc_states_dir(&self, mc_seqno: u32) -> FileDb {
        FileDb::new_readonly(self.storage_dir.path().join(mc_seqno.to_string()))
    }

    fn clear_outdated_state_entries(&self, recent_block_id: &BlockId) -> Result<()> {
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
                Ok(seqno) if seqno >= recent_block_id.seqno || seqno == 0
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
    ) -> Result<()> {
        use std::collections::btree_map;

        use dashmap::mapref::entry::Entry;

        let key = CacheKey {
            block_id: *block_id,
            kind,
        };

        let load_mapped = || {
            let mut file = self
                .mc_states_dir(mc_seqno)
                .file(kind.make_file_name(block_id))
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

        let new_state = Arc::new(CachedState { mc_seqno, file });

        let prev_mc_seqno = match self.descriptor_cache.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(new_state);
                None
            }
            Entry::Occupied(mut entry) => {
                let prev_mc_seqno = entry.get().mc_seqno;
                if mc_seqno <= prev_mc_seqno {
                    // Cache only the most recent block (if changed)
                    return Ok(());
                }

                entry.insert(new_state);
                Some(prev_mc_seqno)
            }
        };

        let mut index = self.mc_seqno_to_block_ids.lock();

        // Remove previous entry if exists
        if let Some(prev_mc_seqno) = prev_mc_seqno {
            if let btree_map::Entry::Occupied(mut entry) = index.entry(prev_mc_seqno) {
                entry.get_mut().remove(block_id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }

        index.entry(mc_seqno).or_default().insert(*block_id);

        Ok(())
    }

    fn clear_cache(&self, block_id: &BlockId) {
        self.descriptor_cache.remove(&CacheKey {
            block_id: *block_id,
            kind: PersistentStateKind::Shard,
        });
        self.descriptor_cache.remove(&CacheKey {
            block_id: *block_id,
            kind: PersistentStateKind::Queue,
        });
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PersistentStateInfo {
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
}

struct CachedState {
    mc_seqno: u32,
    file: MappedFile,
}

#[derive(Default)]
struct HandlesQueue {
    handles: VecDeque<BlockHandle>,
}

impl HandlesQueue {
    fn oldest_known(&self) -> Option<&BlockHandle> {
        self.handles.back()
    }

    fn push(&mut self, new_handle: BlockHandle) -> bool {
        // Allow only new blocks
        if let Some(newest) = self.handles.front() {
            if newest.id().seqno >= new_handle.id().seqno {
                return false;
            }
        }

        // Remove too old states
        let now_utime = new_handle.gen_utime();
        let mut has_suitable = false;
        self.handles.retain(|old_handle| {
            if !has_suitable {
                has_suitable |= BlockStuff::can_use_for_boot(old_handle.gen_utime(), now_utime);
                true
            } else {
                false
            }
        });

        // Add the new one
        self.handles.push_front(new_handle);
        true
    }
}

const STATE_CHUNK_SIZE: u64 = 1024 * 1024; // 1 MB
