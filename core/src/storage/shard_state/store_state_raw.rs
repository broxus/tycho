use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};
use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_storage::fs::TempFileStorage;
use tycho_storage::kv::StoredValue;
use tycho_types::cell::*;
use tycho_types::models::BlockId;
use tycho_types::util::ArrayVec;
use tycho_util::fs::MappedFile;
use tycho_util::io::ByteOrderRead;
use tycho_util::progress_bar::*;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb;

use super::cell_storage::raw::{
    FinalizedTempCellSource, FinalizedTempCells, FinalizedTempCellsBuilder,
};
use super::cell_storage::*;
use super::db_state::{CELL_HASH_RANGE_END, CELL_HASH_RANGE_START};
use super::entries_buffer::*;
use super::util::{CellHashMap, HashBytesKey};
use crate::storage::{BriefBocHeader, CellsDb, ShardStateReader};

pub const MAX_DEPTH: u16 = u16::MAX - 1;

pub struct StoreStateContext {
    pub cells_db: CellsDb,
    pub cell_storage: Arc<CellStorage>,
    pub temp_file_storage: TempFileStorage,
    pub expected_root_hash: Option<HashBytes>,
}

impl StoreStateContext {
    pub fn store_split<R>(
        &self,
        block_id: &BlockId,
        main: R,
        parts: Vec<R>,
    ) -> Result<StoreStateResult>
    where
        R: std::io::Read + Send,
    {
        // TODO: remove after 1 release
        self.clear_temp_cells()?;

        // import main and parts to temp in parallel
        let (main, parts) = std::thread::scope(|scope| {
            let main_handle = scope.spawn(move || self.import_to_temp(main, true));
            let mut part_handles = Vec::with_capacity(parts.len());
            for part in parts {
                part_handles.push(scope.spawn(move || self.import_to_temp(part, false)));
            }
            let mut parts = Vec::with_capacity(part_handles.len());
            for part_handle in part_handles {
                match part_handle.join() {
                    Ok(part) => parts.push(part?),
                    Err(_) => anyhow::bail!("persistent state part import thread failed"),
                }
            }
            let main = match main_handle.join() {
                Ok(main) => main?,
                Err(_) => anyhow::bail!("persistent state main import thread failed"),
            };
            Ok::<_, anyhow::Error>((main, parts))
        })?;

        let mut imported_part_roots = FastHashSet::default();
        for part in &parts {
            anyhow::ensure!(
                part.absent_cells_hashes.is_empty(),
                "split shard state part must not contain absent cells"
            );
            anyhow::ensure!(
                imported_part_roots.insert(part.root_hash),
                "duplicate split shard state part root: {}",
                part.root_hash
            );
        }

        // validate
        anyhow::ensure!(
            main.absent_cells_hashes == imported_part_roots,
            "split shard state parts do not match main file absent cells: \
            absent_count={}, parts_count={}, missing_parts={:?}, unexpected_parts={:?}",
            main.absent_cells_hashes.len(),
            imported_part_roots.len(),
            main.absent_cells_hashes
                .difference(&imported_part_roots)
                .take(4),
            imported_part_roots
                .difference(&main.absent_cells_hashes)
                .take(4)
        );

        // NOTE: we make one atomic apply for all parts to avoid possible
        //      dangling part sub-trees (with a fake counter = 1),
        //      if a process crashed after applying parts but before applying main

        self.apply_temp(block_id, main, parts)
    }

    fn clear_temp_cells(&self) -> Result<()> {
        self.cells_db.rocksdb().delete_range_cf_opt(
            &self.cells_db.temp_cells.cf(),
            CELL_HASH_RANGE_START.as_slice(),
            CELL_HASH_RANGE_END.as_slice(),
            self.cells_db.temp_cells.write_config(),
        )?;
        Ok(())
    }

    fn import_to_temp<R>(&self, reader: R, is_main_part: bool) -> Result<TempState>
    where
        R: std::io::Read,
    {
        let preprocessed = self.preprocess(reader, is_main_part)?;
        self.finalize_to_temp(preprocessed, is_main_part)
    }

    fn preprocess<R>(&self, reader: R, allow_absent: bool) -> Result<PreprocessedState>
    where
        R: std::io::Read,
    {
        let mut pg = ProgressBar::builder()
            .exact_unit("cells")
            .build(|msg| tracing::info!("preprocessing state... {msg}"));

        let mut reader = ShardStateReader::begin(reader)?;
        let header = *reader.header();
        tracing::debug!(?header);

        if header.absent_count > 0 && !allow_absent {
            anyhow::bail!("absent cells are not supported in a single shard state file");
        }

        pg.set_progress(header.cell_count);

        let temp_file = self.temp_file_storage.unnamed_file().open()?;

        const CELLS_PER_CHUNK: usize = 10000;

        let mut buffer = [0; 256]; // At most 2 + 128 + 4 * 4
        let mut temp_file = BufWriter::with_capacity(buffer.len() * CELLS_PER_CHUNK, temp_file);

        let mut remaining_cells = header.cell_count;
        while remaining_cells > 0 {
            let to_read = std::cmp::min(remaining_cells, CELLS_PER_CHUNK as _);

            let mut chunk_bytes = 0u32;
            for _ in 0..to_read {
                let cell_size = reader.read_next_cell(&mut buffer)?;
                debug_assert!(cell_size < 256);
                buffer[cell_size] = cell_size as u8;

                // Write cell data and its size
                temp_file.write_all(&buffer[..=cell_size])?;

                chunk_bytes += cell_size as u32 + 1;
            }

            tracing::debug!(chunk_bytes, "creating chunk");
            temp_file.write_all(&chunk_bytes.to_le_bytes())?;

            remaining_cells -= to_read;

            pg.set_progress(header.cell_count - remaining_cells);
        }

        reader.finish()?;

        pg.complete();

        match temp_file.into_inner() {
            Ok(mut file) => {
                file.flush()?;
                file.seek(std::io::SeekFrom::Start(0))?;
                Ok(PreprocessedState { header, file })
            }
            Err(e) => Err(e.into_error().into()),
        }
    }

    fn finalize_to_temp(
        &self,
        preprocessed: PreprocessedState,
        is_main_part: bool,
    ) -> Result<TempState> {
        // absent cell may contain 4 * (hash + depth)
        const MAX_DATA_SIZE: usize = 4 * (32 + 2);
        const CELLS_PER_BATCH: u64 = 1_000_000;

        let PreprocessedState { header, file } = preprocessed;

        let mut pg = ProgressBar::builder()
            .with_mapper(|x| bytesize::to_string(x, false))
            .build(|msg| tracing::info!("writing state to temp... {msg}"));

        let file = MappedFile::from_existing_file(file)?;

        let mut hashes_file = self
            .temp_file_storage
            .unnamed_file()
            .prealloc(header.cell_count as usize * HashesEntry::LEN)
            .open_as_mapped_mut()?;

        let finalized_temp_cells = FinalizedTempCellsBuilder::new(
            self.temp_file_storage.unnamed_file().open()?,
            header.cell_count as usize,
        );
        let mut ctx = FinalizationContext::new(finalized_temp_cells);

        // Allocate on heap to prevent big future size
        let mut chunk_buffer = Vec::with_capacity(1 << 20);
        let mut data_buffer = vec![0u8; MAX_DATA_SIZE];

        let total_size = file.length();
        pg.set_total(total_size as u64);

        let mut file_pos = total_size;
        let mut cell_index = header.cell_count;
        let mut batch_len = 0;
        let mut absent_cells_hashes = FastHashSet::default();

        while file_pos >= 4 {
            file_pos -= 4;

            // Read chunk size from the current tail position
            let mut chunk_size = {
                let mut tail = [0; 4];
                unsafe { file.read_exact_at(file_pos, &mut tail) };
                u32::from_le_bytes(tail) as usize
            };

            // Rewind to the chunk start
            file_pos = file_pos
                .checked_sub(chunk_size)
                .ok_or_else(|| parser_error("invalid chunk size"))?;

            // Read chunk data
            chunk_buffer.resize(chunk_size, 0);
            unsafe { file.read_exact_at(file_pos, &mut chunk_buffer) };

            tracing::debug!(chunk_size, "processing chunk");

            while chunk_size > 0 {
                cell_index -= 1;
                batch_len += 1;
                let cell_size = chunk_buffer[chunk_size - 1] as usize;
                chunk_size = chunk_size
                    .checked_sub(cell_size + 1)
                    .ok_or_else(|| parser_error("chunk size underflow"))?;

                let cell = RawCell::from_stored_data(
                    &mut &chunk_buffer[chunk_size..chunk_size + cell_size],
                    header.ref_size,
                    header.cell_count as usize,
                    cell_index as usize,
                    &mut data_buffer,
                )?;

                for (&index, buffer) in cell
                    .reference_indices
                    .as_ref()
                    .iter()
                    .zip(ctx.entries_buffer.iter_child_buffers())
                {
                    // SAFETY: `buffer` is guaranteed to be in separate memory area
                    unsafe { hashes_file.read_exact_at(index as usize * HashesEntry::LEN, buffer) }
                }

                // collect absent cells hashes for futher validation
                if let FinalizeCellResult::Absent { hash } =
                    ctx.finalize_cell(cell_index as u32, cell)?
                {
                    absent_cells_hashes.insert(hash);
                }

                // SAFETY: `entries_buffer` is guaranteed to be in separate memory area
                unsafe {
                    hashes_file.write_all_at(
                        cell_index as usize * HashesEntry::LEN,
                        ctx.entries_buffer.current_entry_buffer(),
                    );
                };

                chunk_buffer.truncate(chunk_size);
            }

            if batch_len > CELLS_PER_BATCH {
                ctx.finalize_cell_usages();
                batch_len = 0;
            }

            pg.set_progress((total_size - file_pos) as u64);
        }

        if batch_len > 0 {
            ctx.finalize_cell_usages();
        }

        // Current entry contains root cell
        let root_hash = ctx.entries_buffer.repr_hash();
        if is_main_part && let Some(expected_root_hash) = &self.expected_root_hash {
            anyhow::ensure!(
                root_hash == expected_root_hash,
                "shard state root hash mismatch: expected={expected_root_hash}, got={}",
                HashBytes::wrap(root_hash)
            );
        }
        ctx.final_check(root_hash)?;

        let finalized_temp_cells = ctx.finalized_temp_cells.finish()?;

        pg.complete();

        Ok(TempState {
            root_hash: HashBytes(*root_hash),
            absent_cells_hashes,
            finalized_temp_cells,
        })
    }

    fn apply_temp(
        &self,
        block_id: &BlockId,
        state: TempState,
        parts: Vec<TempState>,
    ) -> Result<StoreStateResult> {
        tracing::info!("applying temp state {}: started", block_id.as_short_id());

        let temp_cell_source = FinalizedTempCellSource::new(
            &state.finalized_temp_cells,
            parts.iter().map(|part| &part.finalized_temp_cells),
        );
        self.cell_storage
            .apply_indexed_temp_cell(&state.root_hash, temp_cell_source)?;
        let shard_state_key = block_id.to_vec();
        let mut finalize_batch = rocksdb::WriteBatch::default();
        finalize_batch.delete_range_cf(
            &self.cells_db.temp_cells.cf(),
            CELL_HASH_RANGE_START.as_slice(),
            CELL_HASH_RANGE_END.as_slice(),
        );
        finalize_batch.put_cf(
            &self.cells_db.shard_states.cf(),
            shard_state_key.as_slice(),
            state.root_hash.as_slice(),
        );
        self.cells_db.rocksdb().write(finalize_batch)?;

        // Load stored shard state
        match self.cells_db.shard_states.get(shard_state_key)? {
            Some(root) => {
                tracing::info!("applying temp state {}: finished", block_id.as_short_id());

                Ok(StoreStateResult {
                    root_hash: HashBytes::from_slice(&root[..32]),
                })
            }
            None => {
                tracing::error!(
                    "applying temp state {}: failed - state not found",
                    block_id.as_short_id()
                );

                Err(StoreStateError::NotFound.into())
            }
        }
    }
}

pub struct StoreStateResult {
    pub root_hash: HashBytes,
}

struct TempState {
    root_hash: HashBytes,
    absent_cells_hashes: FastHashSet<HashBytes>,
    finalized_temp_cells: FinalizedTempCells,
}

struct FinalizationContext {
    pruned_branches: FastHashMap<u32, Vec<u8>>,
    cell_usages: CellHashMap<i32>,
    entries_buffer: EntriesBuffer,
    output_buffer: Vec<u8>,
    finalized_temp_cells: FinalizedTempCellsBuilder,
}

impl FinalizationContext {
    fn new(finalized_temp_cells: FinalizedTempCellsBuilder) -> Self {
        Self {
            pruned_branches: Default::default(),
            cell_usages: CellHashMap::with_capacity_and_hasher(128, Default::default()),
            entries_buffer: EntriesBuffer::new(),
            output_buffer: Vec::with_capacity(1 << 10),
            finalized_temp_cells,
        }
    }

    // TODO: Somehow reuse `tycho_types::cell::CellParts`.
    fn finalize_cell(&mut self, cell_index: u32, cell: RawCell<'_>) -> Result<FinalizeCellResult> {
        use sha2::{Digest, Sha256};

        let (mut current_entry, children) = self
            .entries_buffer
            .split_children(cell.reference_indices.as_ref());

        current_entry.clear();

        // Prepare mask and counters
        let mut children_mask = LevelMask::new(0);

        for (_, child) in children.iter() {
            children_mask |= child.level_mask();
        }

        let mut is_merkle_cell = false;
        let mut is_pruned_cell = false;
        let mut is_absent_cell = false;

        let level_mask = match cell.descriptor.cell_type() {
            CellType::Ordinary if cell.descriptor.is_absent() => {
                is_absent_cell = true;
                cell.descriptor.level_mask()
            }
            CellType::Ordinary => children_mask,
            CellType::PrunedBranch => {
                is_pruned_cell = true;
                cell.descriptor.level_mask()
            }
            CellType::LibraryReference => LevelMask::new(0),
            CellType::MerkleProof | CellType::MerkleUpdate => {
                is_merkle_cell = true;
                children_mask.virtualize(1)
            }
        };

        if cell.descriptor.level_mask() != level_mask.to_byte() {
            return Err(StoreStateError::InvalidCell).context("Level mask mismatch");
        }

        // Save mask and counters
        current_entry.set_level_mask(level_mask);
        current_entry.set_cell_type(cell.descriptor.cell_type());
        current_entry.set_is_absent(is_absent_cell);

        // Calculate hashes
        let hash_count = if is_pruned_cell {
            1
        } else {
            level_mask.level() + 1
        };

        let mut temp_descriptor = cell.descriptor;

        let mut hash_idx = 0;
        for level in 0..4 {
            if level != 0 && (is_pruned_cell || !level_mask.contains(level)) {
                continue;
            }

            // for absent cell read depth and hash from data
            if is_absent_cell {
                let depth = read_stored_depth_from_absent(level_mask, level, cell.data, 0)
                    .context("invalid absent cell")?;
                current_entry.set_depth(hash_idx, depth);

                let hash = read_stored_hash(level_mask, level, cell.data, 0)
                    .context("invalid absent cell")?;
                current_entry.set_hash(hash_idx, hash);

                hash_idx += 1;
                continue;
            }

            let mut hasher = Sha256::new();

            let level_mask = if is_pruned_cell {
                level_mask
            } else {
                LevelMask::from_level(level)
            };

            temp_descriptor.d1 &= !(CellDescriptor::LEVEL_MASK | CellDescriptor::STORE_HASHES_MASK);
            temp_descriptor.d1 |= u8::from(level_mask) << 5;
            hasher.update([temp_descriptor.d1, temp_descriptor.d2]);

            if level == 0 {
                hasher.update(cell.data);
            } else {
                hasher.update(current_entry.get_hash_slice(hash_idx - 1));
            }

            let mut depth = 0;
            for (index, child) in children.iter() {
                let child_depth = if child.cell_type().is_pruned_branch() {
                    let child_data = self
                        .pruned_branches
                        .get(index)
                        .ok_or(StoreStateError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child
                        .pruned_branch_depth(hash_idx + is_merkle_cell as u8, child_data)
                        .context("Invalid pruned branch")?
                } else {
                    child.depth(hash_idx + is_merkle_cell as u8)
                };
                hasher.update(child_depth.to_be_bytes());

                depth = child_depth
                    .checked_add(1)
                    .map(|next_depth| next_depth.max(depth))
                    .filter(|&depth| depth <= MAX_DEPTH)
                    .ok_or(StoreStateError::InvalidCell)
                    .context("Max tree depth exceeded")?;
            }

            current_entry.set_depth(hash_idx, depth);

            for (index, child) in children.iter() {
                let child_hash = if child.cell_type().is_pruned_branch() {
                    let child_data = self
                        .pruned_branches
                        .get(index)
                        .ok_or(StoreStateError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child
                        .pruned_branch_hash(hash_idx + is_merkle_cell as u8, child_data)
                        .context("Invalid pruned branch")?
                } else {
                    child.hash(hash_idx + is_merkle_cell as u8)
                };
                hasher.update(child_hash);
            }

            current_entry.set_hash(hash_idx, hasher.finalize().as_slice());
            hash_idx += 1;
        }

        anyhow::ensure!(hash_count == hash_idx, "invalid hash count");

        // Update pruned branches
        if is_pruned_cell {
            self.pruned_branches.insert(cell_index, cell.data.to_vec());
        }

        // get cell hash
        let repr_hash = if is_pruned_cell {
            *current_entry
                .as_reader()
                .pruned_branch_hash(LevelMask::MAX_LEVEL, cell.data)
                .context("Invalid pruned branch")?
        } else {
            *current_entry.as_reader().hash(LevelMask::MAX_LEVEL)
        };

        // do not store absent cell to storage
        if is_absent_cell {
            return Ok(FinalizeCellResult::Absent {
                hash: HashBytes(repr_hash),
            });
        }

        // Write cell data
        let output_buffer = &mut self.output_buffer;
        output_buffer.clear();

        let repr_hash_idx = hash_count - 1;

        output_buffer.extend_from_slice(&[cell.descriptor.d1, cell.descriptor.d2]);
        output_buffer.extend_from_slice(&cell.bit_len.to_le_bytes());
        output_buffer.extend_from_slice(current_entry.get_depth_slice(repr_hash_idx));
        output_buffer.extend_from_slice(cell.data);

        for i in 0..repr_hash_idx {
            output_buffer.extend_from_slice(current_entry.get_hash_slice(i));
            output_buffer.extend_from_slice(current_entry.get_depth_slice(i));
        }

        // Write cell references
        for (index, child) in children.iter() {
            let child_hash = if child.cell_type().is_pruned_branch() {
                let child_data = self
                    .pruned_branches
                    .get(index)
                    .ok_or(StoreStateError::InvalidCell)
                    .context("Pruned branch data not found")?;
                child
                    .pruned_branch_hash(LevelMask::MAX_LEVEL, child_data)
                    .context("Invalid pruned branch")?
            } else {
                child.hash(LevelMask::MAX_LEVEL)
            };

            // update cell usages only when child is not absent
            // because we won't store absent cells
            // this is for the local BOC check after import to temp
            if !child.is_absent() {
                *self
                    .cell_usages
                    .entry(HashBytesKey(*child_hash))
                    .or_default() += 1;
            }
            output_buffer.extend_from_slice(child_hash);
        }

        // Save serialized data
        self.finalized_temp_cells
            .append(&repr_hash, output_buffer)?;
        self.cell_usages.insert(HashBytesKey(repr_hash), -1);

        // Done
        Ok(FinalizeCellResult::Other)
    }

    fn finalize_cell_usages(&mut self) {
        self.cell_usages.retain(|_, &mut rc| rc < 0);
    }

    fn final_check(&self, root_hash: &[u8; 32]) -> Result<()> {
        tracing::info!(root_hash = %HashBytes::wrap(root_hash), "Final check");
        tracing::info!(len = ?self.cell_usages.len(), "Cell usages");

        anyhow::ensure!(
            self.cell_usages.len() == 1
                && self.cell_usages.contains_key(HashBytesKey::wrap(root_hash)),
            "Invalid shard state cell"
        );
        Ok(())
    }
}

enum FinalizeCellResult {
    Absent { hash: HashBytes },
    Other,
}

struct PreprocessedState {
    header: BriefBocHeader,
    file: File,
}

struct RawCell<'a> {
    descriptor: CellDescriptor,
    data: &'a [u8],
    bit_len: u16,
    reference_indices: ArrayVec<u32, 4>,
}

impl<'a> RawCell<'a> {
    fn from_stored_data<R>(
        src: &mut R,
        ref_size: usize,
        cell_count: usize,
        cell_index: usize,
        data_buffer: &'a mut [u8],
    ) -> std::io::Result<Self>
    where
        R: Read,
    {
        let descriptor = {
            let d1 = src.read_byte()?;
            let check = CellDescriptor::new([d1, 0]);
            if check.is_absent() {
                check
            } else {
                let d2 = src.read_byte()?;
                CellDescriptor::new([d1, d2])
            }
        };
        let byte_len = descriptor.byte_len() as usize;
        let ref_count = descriptor.reference_count() as usize;

        if ref_count > 4 && !descriptor.is_absent() {
            return Err(parser_error("invalid preprocessed cell descriptor"));
        }

        // for absent cells read stored hashes and depth into data
        let data_len = if descriptor.is_absent() {
            if byte_len != 0 {
                return Err(parser_error(
                    "absent cell should have no data except hashes/depths",
                ));
            }
            descriptor.hash_count() as usize * (32 + 2)
        } else {
            byte_len
        };

        let data = &mut data_buffer[0..data_len];
        src.read_exact(data)?;

        let mut reference_indices = ArrayVec::new();

        // skip refs for absent cells
        if !descriptor.is_absent() {
            for _ in 0..ref_count {
                let index = src.read_be_uint(ref_size)? as usize;
                if index >= cell_count || index <= cell_index {
                    return Err(parser_error("reference index out of range"));
                } else {
                    // SAFETY: `ref_count` is in range 0..=4
                    unsafe { reference_indices.push(index as u32) };
                }
            }
        }

        // TODO: Require normalized
        let bit_len = if descriptor.is_aligned() {
            (byte_len * 8) as u16
        } else if let Some(data) = data.last() {
            byte_len as u16 * 8 - data.trailing_zeros() as u16 - 1
        } else {
            0
        };

        Ok(RawCell {
            descriptor,
            data,
            bit_len,
            reference_indices,
        })
    }
}

fn parser_error<E>(error: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    std::io::Error::other(error)
}

#[derive(thiserror::Error, Debug)]
enum StoreStateError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid cell")]
    InvalidCell,
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    use bytes::Bytes;
    use bytesize::ByteSize;
    use rand::seq::IndexedRandom;
    use rand::{Rng, SeedableRng};
    use tycho_storage::{StorageConfig, StorageContext};
    use tycho_types::boc::Boc;
    use tycho_types::merkle::make_pruned_branch;
    use tycho_types::models::{BlockId, ShardIdent, ShardStateUnsplit};
    use tycho_types::prelude::Dict;
    use tycho_util::project_root;
    use weedb::rocksdb::{IteratorMode, WriteBatch};

    use super::*;
    use crate::ZEROSTATE_BOC;
    use crate::storage::db::is_table_empty;
    use crate::storage::shard_state::db_state::{CellsDbStateKey, CountersStore};
    use crate::storage::{CoreStorage, CoreStorageConfig};

    #[tokio::test]
    #[ignore]
    async fn insert_and_delete_of_several_shards() -> Result<()> {
        tycho_util::test::init_logger("insert_and_delete_of_several_shards", "debug");
        let project_root = project_root()?.join(".scratch");
        let integration_test_path = project_root.join("integration_tests");
        let current_test_path = integration_test_path.join("insert_and_delete_of_several_shards");
        std::fs::remove_dir_all(&current_test_path).ok();
        std::fs::create_dir_all(&current_test_path)?;
        // decompressing the archive
        let archive_path = integration_test_path.join("states.tar.zst");
        let res = std::process::Command::new("tar")
            .arg("-I")
            .arg("zstd")
            .arg("-xf")
            .arg(&archive_path)
            .arg("-C")
            .arg(&current_test_path)
            .status()?;
        if !res.success() {
            return Err(anyhow::anyhow!("Failed to decompress the archive"));
        }
        tracing::info!("Decompressed the archive");

        let ctx = StorageContext::new(StorageConfig {
            root_dir: current_test_path.join("db"),
            rocksdb_enable_metrics: false,
            rocksdb_lru_capacity: ByteSize::mib(256),
        })
        .await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig {
            cells_cache_size: ByteSize::mb(256),
            ..Default::default()
        })
        .await?;

        let cells_db = &storage.shard_state_storage().cells_db;
        let cell_storage = &storage.shard_state_storage().cell_storage;

        let store_ctx = StoreStateContext {
            cells_db: cells_db.clone(),
            cell_storage: cell_storage.clone(),
            temp_file_storage: storage.context().temp_files().clone(),
            expected_root_hash: None,
        };

        for file in std::fs::read_dir(current_test_path.join("states"))? {
            let file = file?;
            let filename = file.file_name().to_string_lossy().to_string();

            let block_id = parse_filename(filename.as_ref());

            #[allow(clippy::disallowed_methods)]
            let file = File::open(file.path())?;

            store_ctx.store_split(&block_id, file, Vec::new())?;
        }
        tracing::info!("Finished processing all states");
        tracing::info!("Starting gc");
        states_gc(cell_storage, cells_db).await?;

        Ok(())
    }

    async fn states_gc(cell_storage: &Arc<CellStorage>, db: &CellsDb) -> Result<()> {
        let states_iterator = db.shard_states.iterator(IteratorMode::Start);
        let bump = bumpalo_herd::Herd::new();

        let total_states = db.shard_states.iterator(IteratorMode::Start).count();

        for (deleted, state) in states_iterator.enumerate() {
            let (_, value) = state?;

            // check that state actually exists
            let cell = cell_storage.load_cell(&HashBytes::from_slice(value[..32].as_ref()), 0)?;

            let (_, batch) = cell_storage.remove_cell_mt(
                &bump,
                cell.hash(LevelMask::MAX_LEVEL),
                Default::default(),
            )?;

            // execute batch
            db.rocksdb().write_opt(batch, db.cells.write_config())?;

            tracing::info!("State deleted. Progress: {}/{total_states}", deleted + 1);
        }

        // two compactions in row. First one run merge operators, second one will remove all tombstones
        db.trigger_compaction().await;
        db.trigger_compaction().await;

        let cells_left = db.cells.iterator(IteratorMode::Start).count();
        tracing::info!("States GC finished. Cells left: {cells_left}");
        assert_eq!(cells_left, 0, "Gc is broken. Press F to pay respect");

        Ok(())
    }

    use rand::rngs::StdRng;

    #[tokio::test]
    async fn rand_cells_storage() -> Result<()> {
        tycho_util::test::init_logger("rand_cells_storage", "debug");

        let (ctx, _tempdir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;
        let cells_db = &storage.shard_state_storage().cells_db;
        let cell_storage = &storage.shard_state_storage().cell_storage;

        let mut rng = StdRng::seed_from_u64(1337);

        let mut cell_keys = Vec::new();

        const INITIAL_SIZE: usize = 100_000;

        let mut keys: BTreeSet<HashBytes> =
            (0..INITIAL_SIZE).map(|_| HashBytes(rng.random())).collect();

        let value = new_cell(4); // 4 is a random number, trust me

        let keys_inner = keys.iter().map(|k| (*k, value.clone())).collect::<Vec<_>>();
        let mut dict: Dict<HashBytes, Cell> = Dict::try_from_sorted_slice(&keys_inner)?;

        // 2. Modification Loop

        const MODIFY_COUNT: usize = INITIAL_SIZE / 50;

        for i in 0..20 {
            let keys_inner: Vec<_> = keys.iter().copied().collect();

            let keys_to_remove: Vec<_> =
                keys_inner.choose_multiple(&mut rng, MODIFY_COUNT).collect();

            // Remove
            for key in keys_to_remove {
                dict.remove(key)?;
                keys.remove(key);
            }

            let keys_inner: Vec<_> = keys.iter().copied().collect();
            let keys_to_update = keys_inner
                .choose_multiple(&mut rng, MODIFY_COUNT)
                .collect::<Vec<_>>();

            // Update
            for key in keys_to_update {
                let value = new_cell(rng.random());
                dict.set(key, value)?;
            }

            // Insert
            for val in 0..MODIFY_COUNT {
                let key = HashBytes(rng.random());
                let value = new_cell(val as u32);
                keys.insert(key);
                dict.set(key, value.clone())?;
            }

            // Store
            let new_dict_cell = CellBuilder::build_from(dict.clone())?;

            let cell_hash = new_dict_cell.repr_hash();
            let mut batch = WriteBatch::new();

            let traversed = cell_storage.store_cell_mt(
                new_dict_cell.as_ref(),
                &mut batch,
                Default::default(),
                MODIFY_COUNT * 3,
                i,
            )?;

            cell_keys.push(*cell_hash);

            cells_db
                .rocksdb()
                .write_opt(batch, cells_db.cells.write_config())?;

            tracing::info!("Iteration {i} Finished. traversed: {traversed}",);
        }

        let mut bump = bumpalo_herd::Herd::new();

        tracing::info!("Starting GC");
        let total = cell_keys.len();
        for (id, key) in cell_keys.into_iter().enumerate() {
            let cell = cell_storage.load_cell(&key, 0)?;

            traverse_cell((cell as Arc<DynCell>).as_ref());

            let (res, batch) = cell_storage.remove_cell_mt(&bump, &key, Default::default())?;
            cells_db
                .rocksdb()
                .write_opt(batch, cells_db.cells.write_config())?;
            tracing::info!("Gc {id} of {total} done. Traversed: {res}",);
            bump.reset();
        }

        // two compactions in row. First one run merge operators, second one will remove all tombstones
        cells_db.trigger_compaction().await;
        cells_db.trigger_compaction().await;

        let cells_left = cells_db.cells.iterator(IteratorMode::Start).count();
        tracing::info!("States GC finished. Cells left: {cells_left}");
        assert_eq!(cells_left, 0, "Gc is broken. Press F to pay respect");
        Ok(())
    }

    #[tokio::test]
    async fn raw_state_store_accepts_level1_pruned_child() -> Result<()> {
        let (ctx, _tempdir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        let original_child = CellBuilder::build_from((0xaa_u8, Cell::empty_cell()))?;
        let pruned_child = make_pruned_branch(original_child.as_ref(), 0, Cell::empty_context())?;
        let root = CellBuilder::build_from((0xbb_u8, pruned_child))?;
        let boc = Boc::encode(&root);
        let block_id = BlockId {
            shard: ShardIdent::BASECHAIN,
            seqno: 1,
            root_hash: *root.repr_hash(),
            file_hash: Boc::file_hash_blake(&boc),
        };

        let root_hash = storage
            .shard_state_storage()
            .store_state_bytes(&block_id, Bytes::from(boc), Some(&block_id.root_hash))
            .await?;
        assert_eq!(root_hash, block_id.root_hash);

        Ok(())
    }

    #[tokio::test]
    async fn raw_state_store_allows_existing_snapshot() -> Result<()> {
        let (ctx, _tempdir) = StorageContext::new_temp().await?;
        let mut config = CoreStorageConfig::new_potato();
        config.cell_storage_threads = NonZeroUsize::new(1).unwrap();
        let storage = CoreStorage::open(ctx.clone(), config).await?;

        let root = Boc::decode(ZEROSTATE_BOC)?;
        let state = root.parse::<ShardStateUnsplit>()?;
        let block_id = BlockId {
            shard: state.shard_ident,
            seqno: state.seqno,
            root_hash: *root.repr_hash(),
            file_hash: Boc::file_hash_blake(ZEROSTATE_BOC),
        };
        let next_block_id = BlockId {
            seqno: block_id.seqno + 1,
            ..block_id
        };

        let root_hash = storage
            .shard_state_storage()
            .store_state_bytes(
                &block_id,
                Bytes::from_static(ZEROSTATE_BOC),
                Some(&block_id.root_hash),
            )
            .await?;
        assert_eq!(root_hash, block_id.root_hash);

        let cells_db = storage.shard_state_storage().cells_db.clone();
        anyhow::ensure!(
            is_table_empty(&cells_db.temp_cells)?,
            "temp cells were not cleared"
        );

        // The first raw store creates CounterSnapshotLatest. Store the same
        // cell set again under another block id to ensure raw store can reuse
        // existing counters.
        let root_hash = storage
            .shard_state_storage()
            .store_state_bytes(
                &next_block_id,
                Bytes::from_static(ZEROSTATE_BOC),
                Some(&block_id.root_hash),
            )
            .await?;
        assert_eq!(root_hash, next_block_id.root_hash);

        drop(storage);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let counters = CountersStore::open(cells_db, pool)
            .load_snapshot(CellsDbStateKey::CounterSnapshotLatest)?;
        assert_eq!(counters.next_idx.get(), 332);

        Ok(())
    }

    fn traverse_cell(cell: &DynCell) {
        for cell in cell.references() {
            traverse_cell(cell);
        }
    }

    fn new_cell(value: u32) -> Cell {
        let mut cell = CellBuilder::new();
        cell.store_u32(value).unwrap();
        cell.store_u64(1).unwrap();
        cell.store_reference(cell.clone().build().unwrap()).unwrap();
        cell.build().unwrap()
    }

    fn parse_filename(name: &str) -> BlockId {
        // Split the remaining string by commas into components
        let parts: Vec<&str> = name.split(',').collect();

        // Parse each part
        let workchain: i32 = parts[0].parse().unwrap();
        let prefix = u64::from_str_radix(parts[1], 16).unwrap();
        let seqno: u32 = parts[2].parse().unwrap();

        BlockId {
            shard: ShardIdent::new(workchain, prefix).unwrap(),
            seqno,
            root_hash: Default::default(),
            file_hash: Default::default(),
        }
    }
}
