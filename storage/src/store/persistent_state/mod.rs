use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use everscale_types::cell::HashBytes;
use everscale_types::models::BlockId;
use parking_lot::Mutex;
use tokio::time::Instant;
use tycho_block_util::block::KEY_BLOCK_UTIME_STEP;
use tycho_block_util::queue::QueueState;

use crate::db::{BaseDb, FileDb, MappedFile};
use crate::models::BlockHandle;
use crate::store::BlockHandleStorage;

mod state_writer;

const BASE_DIR: &str = "states";
const QUEUE_STATE_FILE_EXTENSION: &str = "queue";
const STATE_FILE_EXTENSION: &str = "boc";
const QUEUE_STATE_TMP_FILE_EXTENSION: &str = "queue_tmp";

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum CacheStateKind {
    BLOCK,
    QUEUE,
}

impl CacheStateKind {
    fn from_extension(extension: &str) -> Option<Self> {
        match extension {
            QUEUE_STATE_FILE_EXTENSION => Some(Self::QUEUE),
            STATE_FILE_EXTENSION => Some(Self::BLOCK),
            _ => None,
        }
    }

    fn extension(&self) -> &'static str {
        match self {
            Self::BLOCK => STATE_FILE_EXTENSION,
            Self::QUEUE => QUEUE_STATE_FILE_EXTENSION,
        }
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    block_id: BlockId,
    kind: CacheStateKind,
}

pub struct PersistentStateStorage {
    db: BaseDb,
    storage_dir: FileDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    descriptor_cache: Arc<DashMap<CacheKey, Arc<CachedState>>>,
    mc_seqno_to_block_ids: Mutex<BTreeMap<u32, Vec<BlockId>>>,
    is_cancelled: Arc<AtomicBool>,
}

impl PersistentStateStorage {
    pub fn new(
        db: BaseDb,
        files_dir: &FileDb,
        block_handle_storage: Arc<BlockHandleStorage>,
    ) -> Result<Self> {
        let storage_dir = files_dir.create_subdir(BASE_DIR)?;
        let is_cancelled = Arc::new(AtomicBool::new(false));

        let res = Self {
            db,
            storage_dir,
            block_handle_storage,
            descriptor_cache: Default::default(),
            mc_seqno_to_block_ids: Default::default(),
            is_cancelled,
        };
        res.preload_states()?;

        Ok(res)
    }

    pub fn state_exists(&self, block_id: &BlockId, kind: CacheStateKind) -> bool {
        self.descriptor_cache.contains_key(&CacheKey {
            block_id: *block_id,
            kind,
        })
    }

    pub fn get_state_info(&self, block_id: &BlockId) -> Option<PersistentStateInfo> {
        self.descriptor_cache
            .get(&CacheKey {
                block_id: *block_id,
                kind: CacheStateKind::BLOCK,
            })
            .map(|cached| PersistentStateInfo {
                size: cached.file.length(),
            })
    }

    pub async fn read_state_part(
        &self,
        block_id: &BlockId,
        limit: u32,
        offset: u64,
    ) -> Option<Vec<u8>> {
        // NOTE: Should be noop on x64
        let offset = usize::try_from(offset).ok()?;
        let limit = limit as usize;

        let key = CacheKey {
            block_id: *block_id,
            kind: CacheStateKind::BLOCK,
        };
        let cached = self.descriptor_cache.get(&key)?.clone();
        if offset > cached.file.length() {
            return None;
        }

        // NOTE: Cached file is a mapped file, therefore it can take a while to read from it.
        // NOTE: `spawn_blocking` is called here because it is mostly IO-bound operation.
        tokio::task::spawn_blocking(move || {
            let end = std::cmp::min(offset.saturating_add(limit), cached.file.length());
            cached.file.as_slice()[offset..end].to_vec()
        })
        .await
        .ok()
    }

    pub async fn store_state(&self, handle: &BlockHandle, root_hash: &HashBytes) -> Result<()> {
        if handle.has_persistent_state() {
            return Ok(());
        }

        tokio::task::spawn_blocking({
            let handle = handle.clone();
            let root_hash = *root_hash;
            let is_cancelled = self.is_cancelled.clone();

            let span = tracing::Span::current();
            let db = self.db.clone();
            let states_dir = self.prepare_persistent_states_dir(handle.mc_ref_seqno())?;
            let block_handles = self.block_handle_storage.clone();

            move || {
                let _span = span.enter();
                let cell_writer = state_writer::StateWriter::new(&db, &states_dir, handle.id());
                match cell_writer.write(&root_hash, Some(&is_cancelled)) {
                    Ok(()) => {
                        block_handles.set_has_persistent_state(&handle);
                        tracing::info!(block_id = %handle.id(), "persistent state saved");
                    }
                    Err(e) => {
                        tracing::error!(
                            block_id = %handle.id(),
                            "failed to write persistent state: {e:?}"
                        );

                        if let Err(e) = cell_writer.remove() {
                            tracing::error!(block_id = %handle.id(), "{e}");
                        }
                    }
                }
            }
        })
        .await?;

        self.cache_state(handle, CacheStateKind::BLOCK, handle.mc_ref_seqno())
    }

    pub async fn store_queue_state(
        &self,
        handle: &BlockHandle,
        states: Vec<(QueueState, BlockHandle)>,
    ) -> Result<()> {
        let cloned_states = states.clone();
        tokio::task::spawn_blocking({
            let mc_ref_seqno = handle.mc_ref_seqno();
            let dir = self.mc_states_dir(mc_ref_seqno);
            let states_dir = self.prepare_persistent_states_dir(mc_ref_seqno)?;

            move || {
                let result =
                    state_writer::QueueStateWriter::new(&states_dir, &cloned_states).write();
                if let Err(e) = result {
                    Self::remove_file_by_extension(&dir, QUEUE_STATE_TMP_FILE_EXTENSION)?;
                    Self::remove_file_by_extension(&dir, QUEUE_STATE_FILE_EXTENSION)?;
                    bail!("failed to write queue state: {e}");
                }
                Ok(())
            }
        })
        .await??;

        for (_, block_handle) in states {
            self.cache_state(&block_handle, CacheStateKind::QUEUE, handle.mc_ref_seqno())?;
        }
        Ok(())
    }

    pub fn prepare_persistent_states_dir(&self, mc_seqno: u32) -> Result<FileDb> {
        let states_dir = self.mc_states_dir(mc_seqno);
        if !states_dir.path().is_dir() {
            tracing::info!(mc_seqno, "creating persistent state directory");
            states_dir.create_if_not_exists()?;
        }
        Ok(states_dir)
    }

    pub async fn clear_old_persistent_states(&self) -> Result<()> {
        tracing::info!("started clearing old persistent state directories");
        let start = Instant::now();

        // Keep 2 days of states + 1 state before
        let block = {
            let now = tycho_util::time::now_sec();
            let mut key_block = self
                .block_handle_storage
                .find_last_key_block()
                .context("no key blocks found")?;

            loop {
                match self
                    .block_handle_storage
                    .find_prev_persistent_key_block(key_block.id().seqno)
                {
                    Some(prev_key_block) => {
                        if prev_key_block.meta().gen_utime() + 2 * KEY_BLOCK_UTIME_STEP < now {
                            break prev_key_block;
                        } else {
                            key_block = prev_key_block;
                        }
                    }
                    None => return Ok(()),
                }
            }
        };

        // Remove cached states
        {
            let recent_mc_seqno = block.id().seqno;

            let mut index = self.mc_seqno_to_block_ids.lock();
            index.retain(|&mc_seqno, block_ids| {
                if mc_seqno >= recent_mc_seqno || mc_seqno == 0 {
                    return true;
                }

                for block_id in block_ids.drain(..) {
                    // TODO: Clear flag in block handle
                    self.clear_cache(&block_id);
                }
                false
            });
        }

        // Remove files
        self.clear_outdated_state_entries(block.id())?;

        tracing::info!(
            elapsed = %humantime::format_duration(start.elapsed()),
            "clearing old persistent state directories completed"
        );

        Ok(())
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

            let is_recent = matches!(name.parse::<u32>(), Ok(seqno) if seqno >= recent_block_id.seqno || seqno == 0);

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

    #[tracing::instrument(skip_all)]
    fn preload_states(&self) -> Result<()> {
        // For each mc_seqno directory
        let process_states = |path: &PathBuf, mc_seqno: u32| -> Result<()> {
            'outer: for entry in std::fs::read_dir(path)?.flatten() {
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

                    let Some(cache_type) = CacheStateKind::from_extension(extension) else {
                        break 'file;
                    };

                    let Some(handle) = self.block_handle_storage.load_handle(&block_id) else {
                        tracing::warn!(%block_id, "block handle not found");
                        continue 'outer;
                    };

                    if handle.meta().mc_ref_seqno() != mc_seqno {
                        tracing::warn!(%block_id, mc_seqno, "block handle has wrong ref seqno");
                        continue 'outer;
                    }

                    self.cache_state(&handle, cache_type, mc_seqno)?;
                    continue 'outer;
                }
                tracing::warn!(path = %path.display(), "unexpected file");
            }
            Ok(())
        };

        // For each entry in the storage directory
        'outer: for entry in self.storage_dir.entries()?.flatten() {
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
                process_states(&path, mc_seqno)?;
                continue 'outer;
            }
            tracing::warn!(path = %path.display(), "unexpected directory");
        }
        Ok(())
    }

    fn cache_state(
        &self,
        block_handle: &BlockHandle,
        kind: CacheStateKind,
        mc_seqno: u32,
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let block_id = block_handle.id();

        let states = self.mc_states_dir(mc_seqno);
        let path = states
            .path()
            .join(block_id.to_string())
            .with_extension(kind.extension());
        let mut file = states.file(path);

        let mut is_new = false;

        let key = CacheKey {
            block_id: *block_id,
            kind,
        };

        if let Entry::Vacant(entry) = self.descriptor_cache.entry(key) {
            let file = file
                .read(true)
                .write(true)
                .create(false)
                .append(false)
                .open_as_mapped()?;

            entry.insert(Arc::new(CachedState { file }));
            is_new = true;
        }

        if is_new {
            let mut index = self.mc_seqno_to_block_ids.lock();
            index.entry(mc_seqno).or_default().push(*block_id);
        }

        Ok(())
    }

    fn mc_states_dir(&self, mc_seqno: u32) -> FileDb {
        FileDb::new_readonly(self.storage_dir.path().join(mc_seqno.to_string()))
    }

    pub fn remove_file_by_extension(dir: &FileDb, extension: &str) -> Result<()> {
        for entry in fs::read_dir(dir.path())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
                fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    fn clear_cache(&self, block_id: &BlockId) {
        self.descriptor_cache.remove(&CacheKey {
            block_id: *block_id,
            kind: CacheStateKind::BLOCK,
        });
        self.descriptor_cache.remove(&CacheKey {
            block_id: *block_id,
            kind: CacheStateKind::QUEUE,
        });
    }
}

impl Drop for PersistentStateStorage {
    fn drop(&mut self) {
        self.is_cancelled.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PersistentStateInfo {
    pub size: usize,
}

struct CachedState {
    file: MappedFile,
}
