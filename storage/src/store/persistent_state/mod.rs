use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use everscale_types::cell::HashBytes;
use everscale_types::models::BlockId;
use parking_lot::Mutex;
use tokio::time::Instant;
use tycho_util::sync::rayon_run;

use crate::db::{BaseDb, FileDb, MappedFile};
use crate::store::BlockHandleStorage;

mod state_writer;

const KEY_BLOCK_UTIME_STEP: u32 = 86400;
const BASE_DIR: &str = "states";

pub struct PersistentStateStorage {
    db: BaseDb,
    storage_dir: FileDb,
    block_handle_storage: Arc<BlockHandleStorage>,
    descriptor_cache: Arc<DashMap<BlockId, Arc<CachedState>>>,
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

    pub fn state_exists(&self, block_id: &BlockId) -> bool {
        self.descriptor_cache.contains_key(block_id)
    }

    pub fn get_state_info(&self, block_id: &BlockId) -> Option<PersistentStateInfo> {
        self.descriptor_cache
            .get(block_id)
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

        let cached = self.descriptor_cache.get(block_id)?.clone();
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

    pub async fn store_state(
        &self,
        mc_seqno: u32,
        block_id: &BlockId,
        root_hash: &HashBytes,
    ) -> Result<()> {
        let block_id = *block_id;
        let root_hash = *root_hash;
        let is_cancelled = Some(self.is_cancelled.clone());

        let span = tracing::Span::current();
        let db = self.db.clone();
        let states_dir = self.prepare_persistent_states_dir(mc_seqno)?;

        rayon_run(move || {
            let _span = span.enter();
            let cell_writer = state_writer::StateWriter::new(&db, &states_dir, &block_id);
            match cell_writer.write(&root_hash, is_cancelled.as_deref()) {
                Ok(()) => tracing::info!(block_id = %block_id, "persistent state saved"),
                Err(e) => {
                    tracing::error!(
                        block_id = %block_id,
                        "failed to write persistent state: {e:?}"
                    );

                    if let Err(e) = cell_writer.remove() {
                        tracing::error!(%block_id, "{e}");
                    }
                }
            }
        })
        .await;

        self.cache_state(mc_seqno, &block_id)
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
                    self.descriptor_cache.remove(&block_id);
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
                    let Ok(name) = entry.file_name().into_string() else {
                        break 'file;
                    };
                    let Ok(block_id) = name.parse::<BlockId>() else {
                        break 'file;
                    };

                    // Cache the state
                    self.cache_state(mc_seqno, &block_id)?;
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

    fn cache_state(&self, mc_seqno: u32, block_id: &BlockId) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let states = self.mc_states_dir(mc_seqno);
        let mut file = states.file(block_id.to_string());

        let mut is_new = false;
        if let Entry::Vacant(entry) = self.descriptor_cache.entry(*block_id) {
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
