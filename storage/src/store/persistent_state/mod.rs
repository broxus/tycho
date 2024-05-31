use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use everscale_types::cell::HashBytes;
use everscale_types::models::BlockId;
use tokio::time::Instant;

use crate::db::BaseDb;
use crate::store::BlockHandleStorage;
use crate::FileDb;

mod cell_writer;

const KEY_BLOCK_UTIME_STEP: u32 = 86400;
const BASE_DIR: &str = "states";

pub struct PersistentStateStorage {
    db: BaseDb,
    storage_dir: FileDb,
    block_handle_storage: Arc<BlockHandleStorage>,
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

        Ok(Self {
            db,
            storage_dir,
            block_handle_storage,
            is_cancelled,
        })
    }

    pub async fn save_state(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
        root_hash: &HashBytes,
    ) -> Result<()> {
        let block_id = *block_id;
        let root_hash = *root_hash;
        let is_cancelled = Some(self.is_cancelled.clone());

        let db = self.db.clone();
        let states_dir = self.prepare_persistent_states_dir(mc_block_id)?;

        tokio::task::spawn_blocking(move || {
            let cell_writer = cell_writer::CellWriter::new(&db, &states_dir, &block_id.root_hash);
            match cell_writer.write(&root_hash.0, is_cancelled) {
                Ok(()) => {
                    tracing::info!(
                        block_id = %block_id,
                        "successfully wrote persistent state to a file",
                    );
                }
                Err(e) => {
                    tracing::error!(
                        block_id = %block_id,
                        "writing persistent state failed: {e:?}"
                    );

                    if let Err(e) = cell_writer.remove() {
                        tracing::error!(%block_id, "{e}");
                    }
                }
            }
        })
        .await
        .map_err(From::from)
    }

    pub async fn read_state_part(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
        offset: u64,
        size: u64,
    ) -> Result<Bytes> {
        // todo: add validation for offset and size
        // so it won't eat all the memory
        let mut builder = self
            .mc_states_dir(mc_block_id)
            .file(block_id.root_hash.to_string());
        let file_path = builder.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            // TODO: cache file handles
            let mut file = builder.read(true).open()?;

            file.seek(SeekFrom::Start(offset)).with_context(|| {
                format!(
                    "failed to seek state file offset, path: {}",
                    file_path.display()
                )
            })?;

            let mut buf_reader = BufReader::new(file);

            let mut result = BytesMut::zeroed(size as usize);
            let mut result_cursor = 0;

            let now = Instant::now();
            loop {
                match buf_reader.read(&mut result[result_cursor..]) {
                    Ok(bytes_read) => {
                        tracing::debug!(bytes_read, "reading state file");
                        if bytes_read == 0 || bytes_read == size as usize {
                            break;
                        }
                        result_cursor += bytes_read;
                    }
                    Err(e) => {
                        return Err(anyhow::Error::new(e).context(format!(
                            "failed to read state file. Path: {}",
                            file_path.display()
                        )))
                    }
                }
            }
            tracing::debug!(
                "finished reading buffer after: {} ms",
                now.elapsed().as_millis()
            );

            Ok(result.freeze())
        })
        .await
        .unwrap()
    }

    pub fn state_exists(&self, mc_block_id: &BlockId, block_id: &BlockId) -> bool {
        // TODO: cache file handles
        self.mc_states_dir(mc_block_id)
            .file_exists(block_id.root_hash.to_string())
    }

    pub fn prepare_persistent_states_dir(&self, mc_block: &BlockId) -> Result<FileDb> {
        let states_dir = self.storage_dir.subdir_readonly(mc_block.seqno.to_string());
        if !states_dir.path().is_dir() {
            tracing::info!(mc_block = %mc_block, "creating persistent state directory");
            states_dir.create_if_not_exists()?;
        }
        Ok(states_dir)
    }

    fn mc_states_dir(&self, mc_block_id: &BlockId) -> FileDb {
        FileDb::new_readonly(self.storage_dir.path().join(mc_block_id.seqno.to_string()))
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::Release);
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

            let is_recent =
                matches!(name.parse::<u32>(), Ok(seqno) if seqno >= recent_block_id.seqno);

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
}
