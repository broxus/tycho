use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::models::*;
use everscale_types::prelude::{Cell, HashBytes};
use tycho_block_util::block::*;
use tycho_block_util::state::*;

use self::cell_storage::*;
use self::replace_transaction::ShardStateReplaceTransaction;

use crate::db::*;
use crate::util::*;
use crate::{models::BlockHandle, BlockHandleStorage, BlockStorage};

mod cell_storage;
mod entries_buffer;
mod replace_transaction;
mod shard_state_reader;

const DOWNLOADS_DIR: &str = "downloads";

pub struct ShardStateStorage {
    db: Arc<Db>,
    downloads_dir: FileDb,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    cell_storage: Arc<CellStorage>,

    gc_lock: tokio::sync::Mutex<()>,
    min_ref_mc_state: MinRefMcStateTracker,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,
}

impl ShardStateStorage {
    pub fn new(
        db: Arc<Db>,
        files_dir: &FileDb,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        cache_size_bytes: u64,
    ) -> Result<Self> {
        let downloads_dir = files_dir.subdir(DOWNLOADS_DIR);
        downloads_dir.ensure_exists()?;

        let cell_storage = CellStorage::new(db.clone(), cache_size_bytes);

        let res = Self {
            db,
            block_handle_storage,
            block_storage,
            cell_storage,
            downloads_dir,
            gc_lock: Default::default(),
            min_ref_mc_state: Default::default(),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
        };

        // Done
        Ok(res)
    }

    pub fn metrics(&self) -> ShardStateStorageMetrics {
        #[cfg(feature = "count-cells")]
        let storage_cell = countme::get::<StorageCell>();

        ShardStateStorageMetrics {
            #[cfg(feature = "count-cells")]
            storage_cell_live_count: storage_cell.live,
            #[cfg(feature = "count-cells")]
            storage_cell_max_live_count: storage_cell.max_live,
            max_new_mc_cell_count: self.max_new_mc_cell_count.swap(0, Ordering::AcqRel),
            max_new_sc_cell_count: self.max_new_sc_cell_count.swap(0, Ordering::AcqRel),
        }
    }

    // TODO: implement metrics
    /*pub fn cache_metrics(&self) -> CacheStats {
        self.cell_storage.cache_stats()
    }*/

    pub fn min_ref_mc_state(&self) -> &MinRefMcStateTracker {
        &self.min_ref_mc_state
    }

    pub async fn store_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(ShardStateStorageError::BlockHandleIdMismatch.into());
        }

        if handle.meta().has_state() {
            return Ok(false);
        }

        let block_id = handle.id();
        let cell_id = state.root_cell().repr_hash();

        let mut batch = weedb::rocksdb::WriteBatch::default();

        let _gc_lock = self.gc_lock.lock().await;

        // todo: spawn_blocking
        let len = self
            .cell_storage
            .store_cell(&mut batch, state.root_cell().clone())?;

        if block_id.shard.is_masterchain() {
            self.max_new_mc_cell_count.fetch_max(len, Ordering::Release);
        } else {
            self.max_new_sc_cell_count.fetch_max(len, Ordering::Release);
        }

        let mut value = [0; 32 * 3];
        value[..32].copy_from_slice(cell_id.as_slice());
        value[32..64].copy_from_slice(block_id.root_hash.as_slice());
        value[64..96].copy_from_slice(block_id.file_hash.as_slice());

        batch.put_cf(
            &self.db.shard_states.cf(),
            BlockIdShort {
                shard: block_id.shard,
                seqno: block_id.seqno,
            }
            .to_vec(),
            value,
        );

        self.db.raw().write(batch)?;

        Ok(if handle.meta().set_has_state() {
            self.block_handle_storage.store_handle(handle)?;
            true
        } else {
            false
        })
    }

    pub async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff> {
        let cell_id = self.load_state_root(block_id.as_short_id())?;
        let cell = self.cell_storage.load_cell(cell_id)?;

        ShardStateStuff::new(
            *block_id,
            Cell::from(cell as Arc<_>),
            &self.min_ref_mc_state,
        )
    }

    pub fn begin_replace(&'_ self, block_id: &BlockId) -> Result<ShardStateReplaceTransaction<'_>> {
        ShardStateReplaceTransaction::new(
            &self.db,
            &self.downloads_dir,
            &self.cell_storage,
            &self.min_ref_mc_state,
            block_id,
        )
    }

    pub async fn remove_outdated_states(&self, mc_seqno: u32) -> Result<TopBlocks> {
        let _compaction_guard = self.db.delay_compaction().await;

        // Compute recent block ids for the specified masterchain seqno
        let top_blocks = self
            .compute_recent_blocks(mc_seqno)
            .await?
            .context("Recent blocks edge not found")?;

        tracing::info!(
            block_id = %top_blocks.mc_block,
            "starting shard states GC",
        );
        let instant = Instant::now();

        let raw = self.db.raw();

        // Manually get required column factory and r/w options
        let snapshot = raw.snapshot();
        let shard_states_cf = self.db.shard_states.get_unbounded_cf();
        let mut states_read_options = self.db.shard_states.new_read_config();
        states_read_options.set_snapshot(&snapshot);

        let cells_write_options = self.db.cells.write_config();

        let mut alloc = bumpalo::Bump::new();

        // Create iterator
        let mut iter = raw.raw_iterator_cf_opt(&shard_states_cf.bound(), states_read_options);
        iter.seek_to_first();

        // Iterate all states and remove outdated
        let mut removed_states = 0usize;
        let mut removed_cells = 0usize;
        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => match iter.status() {
                    Ok(()) => break,
                    Err(e) => return Err(e.into()),
                },
            };

            let block_id = BlockIdShort::deserialize(&mut std::convert::identity(key))?;
            let root_hash = HashBytes::wrap(value.try_into().expect("invalid value"));

            // Skip blocks from zero state and top blocks
            if block_id.seqno == 0
                || top_blocks.contains_shard_seqno(&block_id.shard, block_id.seqno)
            {
                iter.next();
                continue;
            }

            alloc.reset();
            let mut batch = weedb::rocksdb::WriteBatch::default();
            {
                let _guard = self.gc_lock.lock().await;
                let total = self
                    .cell_storage
                    .remove_cell(&mut batch, &alloc, root_hash)?;
                batch.delete_cf(&shard_states_cf.bound(), key);
                raw.write_opt(batch, cells_write_options)?;

                removed_cells += total;
                tracing::debug!(
                    removed_cells = total,
                    %block_id,
                );
            }

            removed_states += 1;
            iter.next();
        }

        // Done
        tracing::info!(
            removed_states,
            removed_cells,
            block_id = %top_blocks.mc_block,
            elapsed_sec = instant.elapsed().as_secs_f64(),
            "finished shard states GC",
        );
        Ok(top_blocks)
    }

    /// Searches for an edge with the least referenced masterchain block
    ///
    /// Returns `None` if all states are recent enough
    pub async fn compute_recent_blocks(&self, mut mc_seqno: u32) -> Result<Option<TopBlocks>> {
        // 0. Adjust masterchain seqno with minimal referenced masterchain state
        if let Some(min_ref_mc_seqno) = self.min_ref_mc_state.seqno() {
            if min_ref_mc_seqno < mc_seqno {
                mc_seqno = min_ref_mc_seqno;
            }
        }

        // 1. Find target block

        // Find block id using states table
        let mc_block_id = match self
            .find_mc_block_id(mc_seqno)
            .context("Failed to find block id by seqno")?
        {
            Some(block_id) => block_id,
            None => return Ok(None),
        };

        // Find block handle
        let handle = match self.block_handle_storage.load_handle(&mc_block_id)? {
            Some(handle) if handle.meta().has_data() => handle,
            // Skip blocks without handle or data
            _ => return Ok(None),
        };

        // 2. Find minimal referenced masterchain block from the target block

        let block_data = self.block_storage.load_block_data(&handle).await?;
        let block_info = block_data
            .block()
            .load_info()
            .context("Failed to read target block info")?;

        // Find full min masterchain reference id
        let min_ref_mc_seqno = block_info.min_ref_mc_seqno;
        let min_ref_block_id = match self.find_mc_block_id(min_ref_mc_seqno)? {
            Some(block_id) => block_id,
            None => return Ok(None),
        };

        // Find block handle
        let min_ref_block_handle = match self
            .block_handle_storage
            .load_handle(&min_ref_block_id)
            .context("Failed to find min ref mc block handle")?
        {
            Some(handle) if handle.meta().has_data() => handle,
            // Skip blocks without handle or data
            _ => return Ok(None),
        };

        // Compute `TopBlocks` from block data
        self.block_storage
            .load_block_data(&min_ref_block_handle)
            .await
            .and_then(|block_data| TopBlocks::from_mc_block(&block_data))
            .map(Some)
    }

    fn load_state_root(&self, block_id_short: BlockIdShort) -> Result<HashBytes> {
        let shard_states = &self.db.shard_states;
        let shard_state = shard_states.get(block_id_short.to_vec())?;
        match shard_state {
            Some(root) => Ok(HashBytes::from_slice(&root[..32])),
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }

    fn find_mc_block_id(&self, mc_seqno: u32) -> Result<Option<BlockId>> {
        let shard_states = &self.db.shard_states;
        Ok(shard_states
            .get(
                BlockIdShort {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: mc_seqno,
                }
                .to_vec(),
            )?
            .and_then(|value| {
                let value = value.as_ref();
                if value.len() < 96 {
                    return None;
                }

                let root_hash: [u8; 32] = value[32..64].try_into().unwrap();
                let file_hash: [u8; 32] = value[64..96].try_into().unwrap();

                Some(BlockId {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: mc_seqno,
                    root_hash: HashBytes(root_hash),
                    file_hash: HashBytes(file_hash),
                })
            }))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    #[cfg(feature = "count-cells")]
    pub storage_cell_live_count: usize,
    #[cfg(feature = "count-cells")]
    pub storage_cell_max_live_count: usize,
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
}
