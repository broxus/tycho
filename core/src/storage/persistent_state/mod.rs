use std::collections::VecDeque;
use std::fs::File;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapAny};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio::time::Instant;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::QueueStateHeader;
use tycho_block_util::state::RefMcStateHandle;
use tycho_storage::fs::Dir;
use tycho_types::cell::{Cell, CellDescriptor, CellFamily, HashBytes};
use tycho_types::merkle::make_pruned_branch;
use tycho_types::models::{BlockId, PrevBlockRef};
use tycho_util::FastHashMap;
use tycho_util::sync::CancellationFlag;

pub use self::descriptor_cache::PersistentState;
use self::descriptor_cache::{CacheKey, DescriptorCache};
use self::parts::{
    OptionalPersistentStoragePartsMapExt, PersistentStateStoragePartLocalImpl,
    PersistentStoragePartsMap, PersistentStoragePartsMapExt,
};
pub use self::queue_state::reader::{QueueDiffReader, QueueStateReader};
pub use self::queue_state::writer::QueueStateWriter;
pub use self::shard_state::reader::{BriefBocHeader, ShardStateReader};
pub use self::shard_state::writer::ShardStateWriter;
use super::shard_state::{CellShardRouter, ShardStatePartInfo};
use super::{
    BlockHandle, BlockHandleStorage, BlockStorage, CellsDb, KeyBlocksDirection, ShardStateStorage,
};
use crate::storage::persistent_state::descriptor_cache::ReusePersistentStateResult;
use crate::storage::persistent_state::parts::StoreStatePartContext;
use crate::storage::shard_state::ShardPrefix;

mod queue_state {
    pub mod reader;
    pub mod writer;
}
mod shard_state {
    pub mod reader;
    pub mod writer;
}

mod descriptor_cache;
mod parts;

#[cfg(test)]
mod tests;

const BASE_DIR: &str = "states";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PersistentStateKind {
    Shard,
    Queue,
}

impl PersistentStateKind {
    pub fn make_file_name(
        &self,
        block_id: &BlockId,
        part_shard_prefix: Option<&ShardPrefix>,
    ) -> PathBuf {
        match self {
            Self::Shard => ShardStateWriter::file_name(block_id, part_shard_prefix),
            Self::Queue => QueueStateWriter::file_name(block_id),
        }
    }

    pub fn make_temp_file_name(
        &self,
        block_id: &BlockId,
        part_shard_prefix: Option<&ShardPrefix>,
    ) -> PathBuf {
        match self {
            Self::Shard => ShardStateWriter::temp_file_name(block_id, part_shard_prefix),
            Self::Queue => QueueStateWriter::temp_file_name(block_id),
        }
    }

    pub fn from_extension(extension: &str) -> Option<Self> {
        match extension {
            ShardStateWriter::FILE_EXTENSION => Some(Self::Shard),
            QueueStateWriter::FILE_EXTENSION => Some(Self::Queue),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct PersistentStateStorage {
    inner: Arc<Inner>,
}

impl PersistentStateStorage {
    pub fn new(
        cells_db: CellsDb,
        files_dir: &Dir,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        shard_state_storage: Arc<ShardStateStorage>,
    ) -> Result<Self> {
        const MAX_PARALLEL_CHUNK_READS: usize = 20;

        let storage_dir = files_dir.create_subdir(BASE_DIR)?;

        // init persistent storage parts if required
        let storage_parts = Self::init_storage_parts(files_dir, &shard_state_storage)?;

        Ok(Self {
            inner: Arc::new(Inner {
                cells_db,
                block_handles: block_handle_storage,
                blocks: block_storage,
                shard_states: shard_state_storage,
                descriptor_cache: DescriptorCache::new(storage_dir),
                chunks_semaphore: Arc::new(Semaphore::new(MAX_PARALLEL_CHUNK_READS)),
                handles_queue: Default::default(),
                oldest_ps_changed: Default::default(),
                oldest_ps_handle: Default::default(),
                subscriptions: Default::default(),
                subscriptions_mutex: Default::default(),
                storage_parts,
            }),
        })
    }

    /// Initialize persistent storage parts from shard state storage parts
    fn init_storage_parts(
        files_dir: &Dir,
        shard_state_storage: &ShardStateStorage,
    ) -> Result<Option<Arc<PersistentStoragePartsMap>>> {
        let Some(state_storage_parts) = shard_state_storage.storage_parts() else {
            return Ok(None);
        };

        let mut res = PersistentStoragePartsMap::default();

        for (shard_prefix, state_storage_part) in state_storage_parts.iter() {
            let storage_part =
                PersistentStateStoragePartLocalImpl::new(files_dir, state_storage_part.clone())?;
            res.insert(*shard_prefix, Arc::new(storage_part));
        }

        Ok(Some(Arc::new(res)))
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

                    let Some(kind) = PersistentStateKind::from_extension(extension) else {
                        break 'file;
                    };

                    // TODO: should handle parts from files. State may already gone from shard_states
                    let parts_info = if kind == PersistentStateKind::Shard {
                        this.shard_states
                            .load_state_entry(&block_id)?
                            .map(|entry| entry.parts_info.unwrap_or_default())
                    } else {
                        None
                    };
                    this.descriptor_cache
                        .cache_state(mc_seqno, &block_id, kind, None, parts_info)?;
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
            'outer: for entry in this.descriptor_cache.storage_dir().entries()?.flatten() {
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

    pub fn subscribe(&self) -> (Vec<PersistentState>, PersistentStateReceiver) {
        let id = RECEIVER_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(1);

        // TODO: Hold `_guard` for the whole method body? So that we can know
        // that no states will be send to subscriptions while we are collecting
        // the current cache snapshot.
        {
            let _guard = self.inner.subscriptions_mutex.lock();
            let mut subscriptions = self.inner.subscriptions.load_full();
            {
                let subscriptions = Arc::make_mut(&mut subscriptions);
                let prev = subscriptions.insert(id, sender);
                assert!(
                    prev.is_none(),
                    "persistent state subscription must be unique"
                );
            }
            self.inner.subscriptions.store(subscriptions);
        }

        let receiver = PersistentStateReceiver {
            id,
            inner: Arc::downgrade(&self.inner),
            receiver,
        };

        let initial_states = self.inner.descriptor_cache.get_all_states();

        (initial_states, receiver)
    }

    pub fn state_exists(&self, block_id: &BlockId, kind: PersistentStateKind) -> bool {
        self.inner
            .descriptor_cache
            .contains_key(&CacheKey::from((block_id, kind)))
    }

    pub fn get_state_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Option<PersistentStateInfo> {
        self.inner
            .descriptor_cache
            .get(&CacheKey::from((block_id, kind)))
            .and_then(|cached| {
                let mut total_size = cached.file.length();
                if let Some(parts_info) = &cached.parts_info {
                    for part_info in parts_info {
                        // total_size = total_size.saturating_add(part_info.file_size);
                    }
                }
                let size = NonZeroU64::new(total_size as u64)?;
                Some(PersistentStateInfo {
                    // TODO: return parts info
                    size,
                    chunk_size: self.state_chunk_size(),
                })
            })
    }

    pub async fn read_state_chunk(
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

        let permit = {
            let semaphore = self.inner.chunks_semaphore.clone();
            semaphore.acquire_owned().await.ok()?
        };

        let key = CacheKey::from((block_id, state_kind));
        let cached = self.inner.descriptor_cache.get(&key)?;
        if offset > cached.file.length() {
            return None;
        }

        // NOTE: Cached file is a mapped file, therefore it can take a while to read from it.
        // NOTE: `spawn_blocking` is called here because it is mostly IO-bound operation.
        // TODO: Add semaphore to limit the number of concurrent operations.
        tokio::task::spawn_blocking(move || {
            // Ensure that permit is dropped only after cached state is used.
            let _permit = permit;

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
        // check if can reuse state
        let reused = self
            .try_reuse_persistent_state(mc_seqno, handle, PersistentStateKind::Shard)
            .await?;

        // return if state was fully reused
        if reused.reused() {
            return Ok(());
        }

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        let block_id = *handle.id();

        let entry = self
            .inner
            .shard_states
            .load_state_entry(&block_id)?
            .context("shard state entry not found")?;
        let root_hash = entry.root_hash;

        // build map of pruned parts branches to write in main file
        let pruned_parts = self.build_pruned_parts(entry.parts_info.as_deref())?;

        // run persistent store tasks for parts if were not reused
        let mut part_store_tasks = FuturesUnordered::new();
        if let Some(parts_info) = &entry.parts_info
            && !parts_info.is_empty()
            && !reused.reused_shard_parts()?
        {
            let storage_parts = self.inner.storage_parts.try_as_ref_ext()?;
            for part_info in parts_info {
                let storage_part = storage_parts.try_get_ext(&part_info.prefix)?.clone();
                part_store_tasks.push(tokio::spawn(storage_part.store_shard_state_part(
                    StoreStatePartContext {
                        mc_seqno,
                        block_id,
                        root_hash: part_info.hash,
                        tracker_handle: tracker_handle.clone(),
                        cancelled: Some(cancelled.clone()),
                    },
                )));
            }
        }

        // store main persistent state file
        let this = self.inner.clone();
        let cancelled = cancelled.clone();
        let handle_for_main = handle.clone();
        let parts_info = entry.parts_info.clone();

        let span = tracing::Span::current();
        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("main cancelled");
            });

            // NOTE: Ensure that the tracker handle will outlive the state writer.
            let _tracker_handle = tracker_handle;

            let states_dir = this
                .descriptor_cache
                .prepare_persistent_states_dir(mc_seqno)?;
            let writer =
                ShardStateWriter::new(this.cells_db.clone(), &states_dir, &block_id, pruned_parts);

            let stored = match writer.write(&root_hash, Some(&cancelled)) {
                Ok(()) => {
                    this.block_handles
                        .set_has_persistent_shard_state_main(&handle_for_main);
                    tracing::info!("persistent shard state saved");
                    true
                }
                Err(e) => {
                    // NOTE: We are ignoring an error here. It might be intentional
                    tracing::error!("failed to write persistent shard state: {e:?}");
                    false
                }
            };

            let state = if stored {
                let cached = this
                    .descriptor_cache
                    .cache_shard_state(mc_seqno, &block_id, None, parts_info)?;
                Some(cached)
            } else {
                None
            };

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        // wait for all store tasks in parts
        let mut all_parts_stored = true;
        while let Some(store_res) = part_store_tasks.next().await {
            match store_res {
                Ok(Ok(Some(_res))) => {
                    // do nothing
                }
                Ok(Ok(None)) => {
                    all_parts_stored = false;
                    tracing::error!("persistent part was not stored");
                }
                Ok(Err(store_error)) => {
                    all_parts_stored = false;
                    tracing::error!(?store_error, "error in store persistent part task");
                }
                Err(join_error) => {
                    all_parts_stored = false;
                    tracing::error!(?join_error, "error executing store persistent part task");
                }
            }
        }

        // update block handle flags that persistent state parts stored
        if all_parts_stored {
            self.inner
                .block_handles
                .set_has_persistent_shard_state_parts(handle);
        }

        if let Some(state) = state {
            // TODO: should handle parts as well
            self.notify_with_persistent_state(&state).await;
        }

        Ok(())
    }

    /// Loads part branch root cells and makes pruned cells
    fn build_pruned_parts(
        &self,
        parts_info: Option<&[ShardStatePartInfo]>,
    ) -> Result<Option<Arc<ShardStatePartsPrunedData>>> {
        let parts_info = match parts_info {
            Some(info) if !info.is_empty() => info,
            _ => return Ok(None),
        };

        let mut pruned_parts = ShardStatePartsPrunedData::default();

        for part_info in parts_info {
            let cell = self
                .inner
                .shard_states
                .cell_storage()
                .load_cell_ext(
                    &part_info.hash,
                    0,
                    Some(CellShardRouter::Shard {
                        shard_prefix: part_info.prefix,
                    }),
                )
                .context("failed to load part root cell")?;
            let cell = Cell::from(cell as Arc<_>);
            let pruned = make_pruned_branch(cell.as_ref(), 0, Cell::empty_context())
                .context("failed to build pruned branch for part")?;

            pruned_parts.insert(&pruned);
        }

        Ok(Some(Arc::new(pruned_parts)))
    }

    #[tracing::instrument(skip_all, fields(mc_seqno, block_id = %handle.id()))]
    pub async fn store_shard_state_file(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        file: File,
        parts_info: Option<Vec<ShardStatePartInfo>>,
    ) -> Result<()> {
        let reused = self
            .try_reuse_persistent_state(mc_seqno, handle, PersistentStateKind::Shard)
            .await?;
        if reused.reused_shard_main()? {
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

        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let states_dir = this
                .descriptor_cache
                .prepare_persistent_states_dir(mc_seqno)?;

            let cell_writer =
                ShardStateWriter::new(this.cells_db.clone(), &states_dir, handle.id(), None);
            cell_writer.write_file(file, Some(&cancelled))?;
            this.block_handles
                .set_has_persistent_shard_state_main(&handle);
            let state =
                this.descriptor_cache
                    .cache_shard_state(mc_seqno, handle.id(), None, parts_info)?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        // TODO: should handle parts as well
        self.notify_with_persistent_state(&state).await;
        Ok(())
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
            .reused()
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

        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let states_dir = this
                .descriptor_cache
                .prepare_persistent_states_dir(mc_seqno)?;
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

            let state = this
                .descriptor_cache
                .cache_queue_state(mc_seqno, handle.id())?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        self.notify_with_persistent_state(&state).await;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(mc_seqno, block_id = %handle.id()))]
    pub async fn store_queue_state_file(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        file: File,
    ) -> Result<()> {
        if self
            .try_reuse_persistent_state(mc_seqno, handle, PersistentStateKind::Queue)
            .await?
            .reused()
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

        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let states_dir = this
                .descriptor_cache
                .prepare_persistent_states_dir(mc_seqno)?;

            QueueStateWriter::write_file(&states_dir, handle.id(), file, Some(&cancelled))?;
            this.block_handles.set_has_persistent_queue_state(&handle);
            let state = this
                .descriptor_cache
                .cache_queue_state(mc_seqno, handle.id())?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        self.notify_with_persistent_state(&state).await;
        Ok(())
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
            this.descriptor_cache
                .remove_outdated_cached_states(top_handle.id())
        })
        .await?
    }

    async fn try_reuse_persistent_state(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        kind: PersistentStateKind,
    ) -> Result<ReuseStateResult> {
        // Check if there is anything to reuse (return false if nothing)
        match kind {
            // should try reuse main even if parts not saved
            PersistentStateKind::Shard if !handle.has_persistent_shard_state_main() => {
                return Ok(ReuseStateResult::Shard {
                    main: false,
                    parts: false,
                });
            }
            PersistentStateKind::Queue if !handle.has_persistent_queue_state() => {
                return Ok(ReuseStateResult::Queue(false));
            }
            _ => {}
        }

        let reused = self
            .inner
            .descriptor_cache
            .try_reuse_persistent_state(mc_seqno, *handle.id(), kind, None)
            .await?;

        // return result for queue
        if kind == PersistentStateKind::Queue {
            return Ok(ReuseStateResult::Queue(reused.is_some()));
        }

        // return result for shard
        let Some(reused) = reused else {
            // cannot check parts when main was not reused
            return Ok(ReuseStateResult::Shard {
                main: false,
                parts: false,
            });
        };

        // check parts if required
        let mut all_parts_reused = true;
        if let Some(parts_info) = &reused.state().cached().parts_info
            && !parts_info.is_empty()
        {
            let storage_parts = self.inner.storage_parts.try_as_ref_ext()?;

            for part_info in parts_info {
                let storage_part = storage_parts.try_get_ext(&part_info.prefix)?;
                let part_reused = storage_part
                    .try_reuse_persistent_state(mc_seqno, *handle.id())
                    .await?;
                if !part_reused {
                    // all parts should be reused
                    all_parts_reused = false;
                    break;
                }
            }
        }

        if let ReusePersistentStateResult::NewCached(state) = reused {
            // TODO: should handle parts as well
            self.notify_with_persistent_state(&state).await;
        }

        Ok(ReuseStateResult::Shard {
            main: true,
            parts: all_parts_reused,
        })
    }

    async fn notify_with_persistent_state(&self, state: &PersistentState) {
        let subscriptions = self.inner.subscriptions.load_full();
        for sender in subscriptions.values() {
            sender.send(state.clone()).await.ok();
        }
    }
}

enum ReuseStateResult {
    Shard { main: bool, parts: bool },
    Queue(bool),
}

impl ReuseStateResult {
    fn reused(&self) -> bool {
        match self {
            Self::Shard { main, parts } => *main && *parts,
            Self::Queue(reused) => *reused,
        }
    }
    fn reused_shard_main(&self) -> Result<bool> {
        match self {
            Self::Shard { main, .. } => Ok(*main),
            Self::Queue(_) => anyhow::bail!("not a shard state"),
        }
    }
    fn reused_shard_parts(&self) -> Result<bool> {
        match self {
            Self::Shard { parts, .. } => Ok(*parts),
            Self::Queue(_) => anyhow::bail!("not a shard state"),
        }
    }
}

struct Inner {
    cells_db: CellsDb,
    block_handles: Arc<BlockHandleStorage>,
    blocks: Arc<BlockStorage>,
    shard_states: Arc<ShardStateStorage>,
    descriptor_cache: DescriptorCache,
    chunks_semaphore: Arc<Semaphore>,
    handles_queue: Mutex<HandlesQueue>,
    oldest_ps_changed: Notify,
    oldest_ps_handle: ArcSwapAny<Option<BlockHandle>>,
    subscriptions: ArcSwap<FastHashMap<usize, mpsc::Sender<PersistentState>>>,
    subscriptions_mutex: Mutex<()>,
    storage_parts: Option<Arc<PersistentStoragePartsMap>>,
}

#[derive(Debug, Clone, Copy)]
pub struct PersistentStateInfo {
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
}

pub struct PersistentStateReceiver {
    id: usize,
    inner: Weak<Inner>,
    receiver: mpsc::Receiver<PersistentState>,
}

impl std::ops::Deref for PersistentStateReceiver {
    type Target = mpsc::Receiver<PersistentState>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl std::ops::DerefMut for PersistentStateReceiver {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

impl Drop for PersistentStateReceiver {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let _guard = inner.subscriptions_mutex.lock();
            let mut subscriptions = inner.subscriptions.load_full();
            {
                let subscriptions = Arc::make_mut(&mut subscriptions);
                subscriptions.remove(&self.id);
            }
            inner.subscriptions.store(subscriptions);
        }
    }
}

static RECEIVER_ID: AtomicUsize = AtomicUsize::new(0);

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
        if let Some(newest) = self.handles.front()
            && newest.id().seqno >= new_handle.id().seqno
        {
            return false;
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

#[derive(Default)]
pub struct ShardStatePartsPrunedData {
    inner: FastHashMap<HashBytes, PrunedCellData>,
}

impl ShardStatePartsPrunedData {
    pub fn insert(&mut self, pruned_cell: &Cell) {
        self.inner.insert(*pruned_cell.repr_hash(), PrunedCellData {
            descriptor: pruned_cell.descriptor(),
            data: pruned_cell.data().to_vec(),
        });
    }

    pub fn get(&self, hash: &HashBytes) -> Option<&PrunedCellData> {
        self.inner.get(hash)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

pub struct PrunedCellData {
    descriptor: CellDescriptor,
    data: Vec<u8>,
}

impl PrunedCellData {
    pub fn descriptor(&self) -> CellDescriptor {
        self.descriptor
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }
}
