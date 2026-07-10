use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{Seek, Write};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapAny};
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio::time::Instant;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::QueueStateHeader;
use tycho_storage::fs::Dir;
use tycho_types::models::{BlockId, PrevBlockRef};
use tycho_util::fs::MappedFile;
use tycho_util::sync::CancellationFlag;
use tycho_util::{FastHashMap, FastHashSet};

pub use self::queue_state::reader::{QueueDiffReader, QueueStateReader};
pub use self::queue_state::writer::QueueStateWriter;
pub use self::shard_state::reader::{BriefBocHeader, ShardStateReader};
pub use self::shard_state::writer::{
    PersistentStateMeta, ShardStateWriter, validate_persistent_state_split_metadata,
};
use super::{
    BlockHandle, BlockHandleStorage, BlockStorage, CellsDb, KeyBlocksDirection, NodeStateStorage,
    ShardStateStorage,
};

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
    pub fn make_file_name(&self, block_id: &BlockId) -> PathBuf {
        match self {
            Self::Shard => ShardStateWriter::file_name(block_id),
            Self::Queue => QueueStateWriter::file_name(block_id),
        }
    }

    pub fn make_temp_file_name(&self, block_id: &BlockId) -> PathBuf {
        match self {
            Self::Shard => ShardStateWriter::temp_file_name(block_id),
            Self::Queue => QueueStateWriter::temp_file_name(block_id),
        }
    }

    pub fn make_part_file_name(&self, block_id: &BlockId, part_prefix: u64) -> Option<PathBuf> {
        match self {
            Self::Shard => Some(ShardStateWriter::file_name_ext(block_id, Some(part_prefix))),
            Self::Queue => None,
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
        cells_db: CellsDb,
        files_dir: &Dir,
        node_state: Arc<NodeStateStorage>,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        shard_state_storage: Arc<ShardStateStorage>,
        persistent_state_split_depth: u8,
    ) -> Result<Self> {
        const MAX_PARALLEL_CHUNK_READS: usize = 20;

        let storage_dir = files_dir.create_subdir(BASE_DIR)?;

        Ok(Self {
            inner: Arc::new(Inner {
                cells_db,
                storage_dir,
                node_state,
                block_handles: block_handle_storage,
                blocks: block_storage,
                shard_states: shard_state_storage,
                persistent_state_split_depth,
                descriptor_cache: Default::default(),
                mc_seqno_to_block_ids: Default::default(),
                chunks_semaphore: Arc::new(Semaphore::new(MAX_PARALLEL_CHUNK_READS)),
                handles_queue: Default::default(),
                oldest_ps_changed: Default::default(),
                oldest_ps_handle: Default::default(),
                subscriptions: Default::default(),
                subscriptions_mutex: Default::default(),
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
        self.preload_states().await?;
        Ok(())
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

                // skip persistent metadata
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".meta.json"))
                {
                    continue;
                }

                // skip parts
                if parse_shard_state_part_file_name(&path).is_some() {
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

        let initial_states = self
            .inner
            .descriptor_cache
            .iter()
            .map(|item| PersistentState {
                block_id: item.key().block_id,
                kind: item.key().kind,
                cached: item.value().clone(),
            })
            .collect();

        (initial_states, receiver)
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
                    split_depth: cached.meta.as_ref().map_or(0, |meta| meta.split_depth),
                    parts: cached
                        .parts
                        .iter()
                        .filter_map(|part| {
                            Some(PersistentStatePartInfo {
                                prefix: part.prefix,
                                size: NonZeroU64::new(part.file.length() as u64)?,
                            })
                        })
                        .collect(),
                })
            })
    }

    pub async fn read_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        state_kind: PersistentStateKind,
        part_shard_prefix: Option<u64>,
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

        let key = CacheKey {
            block_id: *block_id,
            kind: state_kind,
        };
        let cached = self.inner.descriptor_cache.get(&key)?.clone();
        let part_index = match (state_kind, part_shard_prefix) {
            (PersistentStateKind::Shard, Some(prefix)) => {
                Some(cached.parts.iter().position(|part| part.prefix == prefix)?)
            }
            _ => None,
        };

        let file = match part_index {
            Some(index) => &cached.parts[index].file,
            None => &cached.file,
        };
        if offset > file.length() {
            return None;
        }

        // NOTE: Cached file is a mapped file, therefore it can take a while to read from it.
        // NOTE: `spawn_blocking` is called here because it is mostly IO-bound operation.
        // TODO: Add semaphore to limit the number of concurrent operations.
        tokio::task::spawn_blocking(move || {
            // Ensure that permit is dropped only after cached state is used.
            let _permit = permit;

            let file = match part_index {
                Some(index) => &cached.parts[index].file,
                None => &cached.file,
            };
            let end = std::cmp::min(offset.saturating_add(chunk_size), file.length());
            Some(file.as_slice()[offset..end].to_vec())
        })
        .await
        .ok()?
    }

    #[tracing::instrument(skip_all, fields(mc_seqno, block_id = %handle.id()))]
    pub async fn store_shard_state(&self, mc_seqno: u32, handle: &BlockHandle) -> Result<()> {
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

        let (root_hash, states_dir) = {
            let handle = handle.clone();
            let this = this.clone();
            let span = tracing::Span::current();

            tokio::task::spawn_blocking(move || {
                let _span = span.enter();

                let root_hash = this.shard_states.load_state_root_hash(handle.id())?;
                let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

                Ok::<_, anyhow::Error>((root_hash, states_dir))
            })
            .await??
        };

        match this
            .shard_states
            .write_persistent_shard_state(
                states_dir,
                *handle.id(),
                root_hash,
                this.persistent_state_split_depth,
                Some(cancelled.clone()),
            )
            .await
        {
            Ok(_) => {
                this.block_handles.set_has_persistent_shard_state(&handle);
                tracing::info!("persistent shard state saved");
            }
            Err(e) => {
                // NOTE: Keep trying `cache_state` after writer failure so a
                // concurrently completed file can still be reused. If the file
                // is absent or broken, `cache_state` returns the actual error.
                tracing::error!("failed to write persistent shard state: {e:?}");
            }
        }

        let span = tracing::Span::current();
        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let state = this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Shard)?;

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        self.notify_with_persistent_state(&state).await;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(mc_seqno, block_id = %handle.id()))]
    pub async fn store_shard_state_files(
        &self,
        mc_seqno: u32,
        handle: &BlockHandle,
        main: File,
        parts: Vec<(u64, File)>,
        split_depth: u8,
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
        let guard = scopeguard::guard((), |_| {
            tracing::warn!("cancelled");
        });

        let handle = handle.clone();
        let this = self.inner.clone();
        let span = tracing::Span::current();

        // prepare persistent state dir
        let states_dir = {
            let this = this.clone();
            tokio::task::spawn_blocking(move || {
                let _span = span.enter();

                this.prepare_persistent_states_dir(mc_seqno)
            })
            .await??
        };

        // will use semaphore to limit parrallel io operations
        const MAX_PARALLEL_STATE_FILE_WRITES: usize = 4;
        let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_STATE_FILE_WRITES));

        // write part files in parallel
        let mut prefixes = Vec::with_capacity(parts.len());
        let mut parts_writes = tokio::task::JoinSet::new();
        for (prefix, file) in parts {
            prefixes.push(prefix);

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .context("state file write semaphore closed")?;
            let cells_db = this.cells_db.clone();
            let states_dir = states_dir.clone();
            let block_id = *handle.id();
            let cancelled = cancelled.clone();
            let span = tracing::Span::current();

            parts_writes.spawn_blocking(move || {
                let _permit = permit;
                let _span = span.enter();

                ShardStateWriter::new_part(&cells_db, &states_dir, &block_id, prefix)
                    .write_file(file, Some(&cancelled))
                    .with_context(|| format!("failed to write persistent state part {prefix:016x}"))
            });
        }

        // collect results
        let mut write_result = Ok::<_, anyhow::Error>(());
        while let Some(result) = parts_writes.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    cancelled.cancel();
                    if write_result.is_ok() {
                        write_result = Err(e);
                    }
                }
                Err(e) => {
                    cancelled.cancel();
                    if write_result.is_ok() {
                        write_result = Err(e.into());
                    }
                }
            }
        }
        write_result?;

        // write metadata and main file
        let cancelled = cancelled.clone();
        let span = tracing::Span::current();

        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            // write persistent metadata even for single file
            PersistentStateMeta::new(if prefixes.is_empty() { 0 } else { split_depth }, prefixes)
                .write(&states_dir, handle.id())?;

            // write main
            ShardStateWriter::new(&this.cells_db, &states_dir, handle.id())
                .write_file(main, Some(&cancelled))?;

            this.block_handles.set_has_persistent_shard_state(&handle);
            let state = this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Shard)?;

            Ok::<_, anyhow::Error>(state)
        })
        .await??;

        self.notify_with_persistent_state(&state).await;

        scopeguard::ScopeGuard::into_inner(guard);
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

            let state = this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Queue)?;

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

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

            QueueStateWriter::write_file(&states_dir, handle.id(), file, Some(&cancelled))?;
            this.block_handles.set_has_persistent_queue_state(&handle);
            let state = this.cache_state(mc_seqno, handle.id(), PersistentStateKind::Queue)?;

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
        let zerostate_seqno = this
            .node_state
            .load_zerostate_mc_seqno()
            .unwrap_or_default();

        let mut top_handle = top_handle.clone();
        if top_handle.id().seqno <= zerostate_seqno {
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
                if mc_seqno >= top_handle.id().seqno || mc_seqno <= zerostate_seqno {
                    return true;
                }

                for block_id in block_ids.drain() {
                    // TODO: Clear flag in block handle
                    this.clear_cache(&block_id);
                }
                false
            });

            // Remove files
            this.clear_outdated_state_entries(top_handle.id(), zerostate_seqno)
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
        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let states_dir = this.prepare_persistent_states_dir(mc_seqno)?;

            if kind == PersistentStateKind::Shard {
                // write parts
                for part in &cached.parts {
                    let file_name = ShardStateWriter::file_name_ext(block_id, Some(part.prefix));
                    let temp_file = states_dir.file(ShardStateWriter::temp_file_name_ext(
                        block_id,
                        Some(part.prefix),
                    ));
                    std::fs::write(temp_file.path(), part.file.as_slice())?;
                    temp_file.rename(file_name)?;
                }

                // write persistent state metadata
                let meta = cached.meta.clone().unwrap_or_default();
                meta.write(&states_dir, &block_id)?;
            }

            // write main
            let temp_file = states_dir.file(kind.make_temp_file_name(&block_id));
            std::fs::write(temp_file.path(), cached.file.as_slice())?;
            temp_file.rename(kind.make_file_name(&block_id))?;

            drop(cached);

            this.cache_state(mc_seqno, &block_id, kind)
        })
        .await??;

        self.notify_with_persistent_state(&state).await;
        Ok(true)
    }

    async fn notify_with_persistent_state(&self, state: &PersistentState) {
        let subscriptions = self.inner.subscriptions.load_full();
        for sender in subscriptions.values() {
            sender.send(state.clone()).await.ok();
        }
    }
}

struct Inner {
    cells_db: CellsDb,
    storage_dir: Dir,
    node_state: Arc<NodeStateStorage>,
    block_handles: Arc<BlockHandleStorage>,
    blocks: Arc<BlockStorage>,
    shard_states: Arc<ShardStateStorage>,
    persistent_state_split_depth: u8,
    descriptor_cache: DashMap<CacheKey, Arc<CachedState>>,
    mc_seqno_to_block_ids: Mutex<BTreeMap<u32, FastHashSet<BlockId>>>,
    chunks_semaphore: Arc<Semaphore>,
    handles_queue: Mutex<HandlesQueue>,
    oldest_ps_changed: Notify,
    oldest_ps_handle: ArcSwapAny<Option<BlockHandle>>,
    subscriptions: ArcSwap<FastHashMap<usize, mpsc::Sender<PersistentState>>>,
    subscriptions_mutex: Mutex<()>,
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
    ) -> Result<PersistentState> {
        use std::collections::btree_map;

        use dashmap::mapref::entry::Entry;

        let key = CacheKey {
            block_id: *block_id,
            kind,
        };

        let states_dir = self.mc_states_dir(mc_seqno);

        let load_mapped = |file_name: PathBuf| {
            let mut file = states_dir.file(file_name).read(true).open()?;

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

        // cache main file
        let file = load_mapped(kind.make_file_name(block_id))
            .with_context(|| format!("failed to cache {kind:?} for {block_id}"))?;

        let (meta, parts) = if kind == PersistentStateKind::Shard {
            // cache metadata, and parts if exist
            let meta = match PersistentStateMeta::read(&states_dir, block_id)? {
                Some(meta) => meta,
                None if has_shard_state_part_files(&states_dir, block_id)? => {
                    anyhow::bail!(
                        "incomplete split persistent state bundle for {block_id}: missing metadata"
                    )
                }
                None => PersistentStateMeta::default(),
            };
            let mut parts = Vec::with_capacity(meta.parts.len());
            for &prefix in &meta.parts {
                let file_name = ShardStateWriter::file_name_ext(block_id, Some(prefix));
                anyhow::ensure!(
                    states_dir.file(&file_name).path().is_file(),
                    "incomplete split persistent state bundle for {block_id}: missing part {prefix:016x}"
                );
                parts.push(CachedStatePart {
                    prefix,
                    file: load_mapped(file_name).with_context(|| {
                        format!(
                            "failed to cache split persistent state part {prefix:016x} for {block_id}"
                        )
                    })?,
                });
            }
            (Some(meta), parts)
        } else {
            (None, Vec::new())
        };

        let new_state = Arc::new(CachedState {
            mc_seqno,
            file,
            meta,
            parts,
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

        let mut index = self.mc_seqno_to_block_ids.lock();

        // Remove previous entry if exists
        if let Some(prev_mc_seqno) = prev_mc_seqno
            && let btree_map::Entry::Occupied(mut entry) = index.entry(prev_mc_seqno)
        {
            entry.get_mut().remove(block_id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }

        index.entry(mc_seqno).or_default().insert(*block_id);

        Ok(PersistentState {
            block_id: *block_id,
            kind,
            cached: new_state,
        })
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

fn has_shard_state_part_files(states_dir: &Dir, block_id: &BlockId) -> Result<bool> {
    let block_id = block_id.to_string();
    for entry in std::fs::read_dir(states_dir.path())? {
        let path = entry?.path();
        if path.is_file()
            && parse_shard_state_part_file_name(&path)
                .is_some_and(|(part_block_id, _)| part_block_id == block_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_shard_state_part_file_name(path: &std::path::Path) -> Option<(&str, u64)> {
    if path.extension()?.to_str()? != ShardStateWriter::FILE_EXTENSION {
        return None;
    }
    let (block_id, prefix) = path.file_stem()?.to_str()?.rsplit_once("_part_")?;
    if prefix.len() != 16 {
        return None;
    }
    let prefix = u64::from_str_radix(prefix, 16).ok()?;
    block_id.parse::<BlockId>().ok()?;
    Some((block_id, prefix))
}

pub(super) fn check_can_reuse_shard_state_part_files(
    states_dir: &Dir,
    block_id: &BlockId,
    expected_meta: &PersistentStateMeta,
) -> Result<bool> {
    // read metadata from the disk
    let meta = match PersistentStateMeta::read(states_dir, block_id) {
        Ok(Some(meta)) => meta,
        Ok(None) => return Ok(false),
        Err(e) if e.downcast_ref::<std::io::Error>().is_none() => return Ok(false),
        Err(e) => return Err(e),
    };

    // can reuse existing parts only if metadata matches and all part files exist
    if meta != *expected_meta {
        return Ok(false);
    }
    for &prefix in &expected_meta.parts {
        let file_name = ShardStateWriter::file_name_ext(block_id, Some(prefix));
        if !states_dir.file(&file_name).path().is_file() {
            return Ok(false);
        }
    }

    Ok(true)
}

#[derive(Debug, Clone)]
pub struct PersistentStateInfo {
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
    pub split_depth: u8,
    pub parts: Vec<PersistentStatePartInfo>,
}

#[derive(Debug, Clone, Copy)]
pub struct PersistentStatePartInfo {
    pub prefix: u64,
    pub size: NonZeroU64,
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

    pub fn file(&self) -> &MappedFile {
        &self.cached.file
    }

    pub fn mc_seqno(&self) -> u32 {
        self.cached.mc_seqno
    }
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

struct CachedState {
    mc_seqno: u32,
    file: MappedFile,
    meta: Option<PersistentStateMeta>,
    parts: Vec<CachedStatePart>,
}

struct CachedStatePart {
    prefix: u64,
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
