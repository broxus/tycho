use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::cell::*;
use everscale_types::models::BlockId;

use super::cell_storage::*;
use super::entries_buffer::*;
use super::files_context::*;
use super::shard_state_reader::*;
use crate::db::*;

use tycho_block_util::*;
use tycho_util::FastHashMap;

pub struct ShardStateReplaceTransaction<'a> {
    db: &'a Db,
    cell_storage: &'a Arc<CellStorage>,
    min_ref_mc_state: &'a Arc<MinRefMcState>,
    reader: ShardStatePacketReader,
    header: Option<BocHeader>,
    cells_read: u64,
}

impl<'a> ShardStateReplaceTransaction<'a> {
    pub fn new(
        db: &'a Db,
        cell_storage: &'a Arc<CellStorage>,
        min_ref_mc_state: &'a Arc<MinRefMcState>,
    ) -> Self {
        Self {
            db,
            cell_storage,
            min_ref_mc_state,
            reader: ShardStatePacketReader::new(),
            header: None,
            cells_read: 0,
        }
    }

    pub fn header(&self) -> &Option<BocHeader> {
        &self.header
    }

    pub async fn process_packet(
        &mut self,
        ctx: &mut FilesContext,
        packet: Vec<u8>,
        progress_bar: &mut ProgressBar,
    ) -> Result<bool> {
        use tokio::io::AsyncWriteExt;

        let cells_file = ctx.cells_file()?;

        self.reader.set_next_packet(packet);

        let header = loop {
            if let Some(header) = &self.header {
                break header;
            }

            let header = match self.reader.read_header()? {
                Some(header) => header,
                None => return Ok(false),
            };

            tracing::debug!(?header);
            progress_bar.set_total(header.cell_count);

            self.header = Some(header);
        };

        let mut chunk_size = 0u32;
        let mut buffer = [0; 256]; // At most 2 + 128 + 4 * 4

        while self.cells_read < header.cell_count {
            let cell_size = match self.reader.read_cell(header.ref_size, &mut buffer)? {
                Some(cell_size) => cell_size,
                None => break,
            };

            buffer[cell_size] = cell_size as u8;
            cells_file.write_all(&buffer[..cell_size + 1]).await?;

            chunk_size += cell_size as u32 + 1;
            self.cells_read += 1;
        }

        progress_bar.set_progress(self.cells_read);

        if chunk_size > 0 {
            tracing::debug!(chunk_size, "creating chunk");
            cells_file.write_u32_le(chunk_size).await?;
        }

        if self.cells_read < header.cell_count {
            return Ok(false);
        }

        if header.has_crc && self.reader.read_crc()?.is_none() {
            return Ok(false);
        }

        progress_bar.complete();
        Ok(true)
    }

    pub async fn finalize(
        self,
        ctx: &mut FilesContext,
        block_id: BlockId,
        progress_bar: &mut ProgressBar,
    ) -> Result<Arc<ShardStateStuff>> {
        // 2^7 bits + 1 bytes
        const MAX_DATA_SIZE: usize = 128;
        const CELLS_PER_BATCH: u64 = 1_000_000;

        let header = match &self.header {
            Some(header) => header,
            None => {
                return Err(ReplaceTransactionError::InvalidShardStatePacket)
                    .context("BOC header not found")
            }
        };

        let hashes_file =
            ctx.create_mapped_hashes_file(header.cell_count as usize * HashesEntry::LEN)?;
        let cells_file = ctx.create_mapped_cells_file().await?;

        let raw = self.db.raw().as_ref();
        let write_options = self.db.cells.new_write_config();

        let mut tail = [0; 4];
        let mut ctx = FinalizationContext::new(self.db);

        // Allocate on heap to prevent big future size
        let mut chunk_buffer = Vec::with_capacity(1 << 20);
        let mut data_buffer = vec![0u8; MAX_DATA_SIZE];

        let total_size = cells_file.length();
        progress_bar.set_total(total_size as u64);

        let mut file_pos = total_size;
        let mut cell_index = header.cell_count;
        let mut batch_len = 0;
        while file_pos >= 4 {
            file_pos -= 4;
            unsafe { cells_file.read_exact_at(file_pos, &mut tail) };

            let mut chunk_size = u32::from_le_bytes(tail) as usize;
            chunk_buffer.resize(chunk_size, 0);

            file_pos -= chunk_size;
            unsafe { cells_file.read_exact_at(file_pos, &mut chunk_buffer) };

            tracing::debug!(chunk_size, "processing chunk");

            while chunk_size > 0 {
                cell_index -= 1;
                batch_len += 1;
                let cell_size = chunk_buffer[chunk_size - 1] as usize;
                chunk_size -= cell_size + 1;

                let cell = RawCell::from_stored_data(
                    &mut &chunk_buffer[chunk_size..chunk_size + cell_size],
                    header.ref_size,
                    header.cell_count as usize,
                    cell_index as usize,
                    &mut data_buffer,
                )?;

                for (&index, buffer) in cell
                    .reference_indices
                    .iter()
                    .zip(ctx.entries_buffer.iter_child_buffers())
                {
                    // SAFETY: `buffer` is guaranteed to be in separate memory area
                    unsafe { hashes_file.read_exact_at(index as usize * HashesEntry::LEN, buffer) }
                }

                self.finalize_cell(&mut ctx, cell_index as u32, cell)?;

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
                raw.write_opt(std::mem::take(&mut ctx.write_batch), &write_options)?;
                batch_len = 0;
            }

            progress_bar.set_progress((total_size - file_pos) as u64);
            tokio::task::yield_now().await;
        }

        if batch_len > 0 {
            ctx.finalize_cell_usages();
            raw.write_opt(std::mem::take(&mut ctx.write_batch), &write_options)?;
        }

        // Current entry contains root cell
        let root_hash = ctx.entries_buffer.repr_hash();
        ctx.final_check(root_hash)?;

        let shard_state_key = block_id.as_short_id().to_vec();
        self.db.shard_states.insert(&shard_state_key, root_hash)?;

        progress_bar.complete();

        // Load stored shard state
        match self.db.shard_states.get(shard_state_key)? {
            Some(root) => {
                let cell_id = HashBytes::from_slice(&root[..32]);

                let cell = self.cell_storage.load_cell(cell_id)?;
                Ok(Arc::new(ShardStateStuff::new(
                    block_id,
                    Cell::from(cell as Arc<_>),
                    self.min_ref_mc_state,
                )?))
            }
            None => Err(ReplaceTransactionError::NotFound.into()),
        }
    }

    fn finalize_cell(
        &self,
        ctx: &mut FinalizationContext<'_>,
        cell_index: u32,
        mut cell: RawCell<'_>,
    ) -> Result<()> {
        use sha2::{Digest, Sha256};

        let (mut current_entry, children) =
            ctx.entries_buffer.split_children(&cell.reference_indices);

        current_entry.clear();

        // Prepare mask and counters
        let mut children_mask = LevelMask::new(0);
        let mut tree_bits_count = cell.bit_len as u64;
        let mut tree_cell_count = 1;

        for (_, child) in children.iter() {
            children_mask |= child.level_mask();
            tree_bits_count += child.tree_bits_count();
            tree_cell_count += child.tree_cell_count();
        }

        let mut is_merkle_cell = false;
        let mut is_pruned_cell = false;
        let level_mask = match cell.descriptor.cell_type() {
            CellType::Ordinary => children_mask,
            CellType::PrunedBranch => {
                is_pruned_cell = true;
                cell.descriptor.level_mask()
            }
            CellType::LibraryReference => LevelMask::new(0),
            CellType::MerkleProof => {
                is_merkle_cell = true;
                children_mask.virtualize(1)
            }
            CellType::MerkleUpdate => {
                is_merkle_cell = true;
                children_mask.virtualize(1)
            }
        };

        if cell.descriptor.level_mask() != level_mask.to_byte() {
            return Err(ReplaceTransactionError::InvalidCell).context("Level mask mismatch");
        }

        // Save mask and counters
        current_entry.set_level_mask(level_mask);
        current_entry.set_cell_type(cell.descriptor.cell_type());
        current_entry.set_tree_bits_count(tree_bits_count);
        current_entry.set_tree_cell_count(tree_cell_count);

        // Calculate hashes
        let hash_count = if is_pruned_cell {
            1
        } else {
            level_mask.level() + 1
        };

        let mut max_depths = [0u16; 4];
        for i in 0..hash_count {
            let mut hasher = Sha256::new();

            let level_mask = if is_pruned_cell {
                level_mask
            } else {
                LevelMask::from_level(i)
            };

            cell.descriptor.d1 &= !(CellDescriptor::LEVEL_MASK | CellDescriptor::STORE_HASHES_MASK);
            cell.descriptor.d1 |= u8::from(level_mask) << 5;
            hasher.update([cell.descriptor.d1, cell.descriptor.d2]);

            if i == 0 {
                hasher.update(cell.data);
            } else {
                hasher.update(current_entry.get_hash_slice(i - 1));
            }

            for (index, child) in children.iter() {
                let child_depth = if child.cell_type().is_pruned_branch() {
                    let child_data = ctx
                        .pruned_branches
                        .get(index)
                        .ok_or(ReplaceTransactionError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child.pruned_branch_depth(i, child_data)
                } else {
                    child.depth(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(child_depth.to_be_bytes());

                let depth = &mut max_depths[i as usize];
                *depth = std::cmp::max(*depth, child_depth + 1);

                current_entry.set_depth(i, *depth);
            }

            for (index, child) in children.iter() {
                let child_hash = if child.cell_type().is_pruned_branch() {
                    let child_data = ctx
                        .pruned_branches
                        .get(index)
                        .ok_or(ReplaceTransactionError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child
                        .pruned_branch_hash(i, child_data)
                        .context("Invalid pruned branch")?
                } else {
                    child.hash(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(child_hash);
            }

            current_entry.set_hash(i, hasher.finalize().as_slice());
        }

        // Update pruned branches
        if is_pruned_cell {
            ctx.pruned_branches.insert(cell_index, cell.data.to_vec());
        }

        // Write cell data
        let output_buffer = &mut ctx.output_buffer;
        output_buffer.clear();

        output_buffer.extend_from_slice(&[
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            cell.descriptor.d1,
            cell.descriptor.d2,
        ]);
        output_buffer.extend_from_slice(&cell.bit_len.to_le_bytes());
        output_buffer.extend_from_slice(cell.data);

        let hash_count = cell.descriptor.hash_count();
        for i in 0..hash_count {
            output_buffer.extend_from_slice(current_entry.get_hash_slice(i));
            output_buffer.extend_from_slice(current_entry.get_depth_slice(i));
        }

        // Write cell references
        for (index, child) in children.iter() {
            let child_hash = if child.cell_type().is_pruned_branch() {
                let child_data = ctx
                    .pruned_branches
                    .get(index)
                    .ok_or(ReplaceTransactionError::InvalidCell)
                    .context("Pruned branch data not found")?;
                child
                    .pruned_branch_hash(MAX_LEVEL, child_data)
                    .context("Invalid pruned branch")?
            } else {
                child.hash(MAX_LEVEL)
            };

            *ctx.cell_usages.entry(*child_hash).or_default() += 1;
            output_buffer.extend_from_slice(child_hash);
        }

        // // Write counters
        // output_buffer.extend_from_slice(current_entry.get_tree_counters());

        // Save serialized data
        let repr_hash = if is_pruned_cell {
            current_entry
                .as_reader()
                .pruned_branch_hash(3, cell.data)
                .context("Invalid pruned branch")?
        } else {
            current_entry.as_reader().hash(MAX_LEVEL)
        };

        ctx.write_batch
            .merge_cf(&ctx.cells_cf, repr_hash, output_buffer.as_slice());
        ctx.cell_usages.insert(*repr_hash, -1);

        // Done
        Ok(())
    }
}

struct FinalizationContext<'a> {
    pruned_branches: FastHashMap<u32, Vec<u8>>,
    cell_usages: FastHashMap<[u8; 32], i32>,
    entries_buffer: EntriesBuffer,
    output_buffer: Vec<u8>,
    cells_cf: BoundedCfHandle<'a>,
    write_batch: rocksdb::WriteBatch,
}

impl<'a> FinalizationContext<'a> {
    fn new(db: &'a Db) -> Self {
        Self {
            pruned_branches: Default::default(),
            cell_usages: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            entries_buffer: EntriesBuffer::new(),
            output_buffer: Vec::with_capacity(1 << 10),
            cells_cf: db.cells.cf(),
            write_batch: rocksdb::WriteBatch::default(),
        }
    }

    fn finalize_cell_usages(&mut self) {
        self.cell_usages.retain(|key, &mut rc| {
            if rc > 0 {
                self.write_batch.merge_cf(
                    &self.cells_cf,
                    key,
                    refcount::encode_positive_refcount(rc as u32),
                );
            }

            rc < 0
        });
    }

    fn final_check(&self, root_hash: &[u8; 32]) -> Result<()> {
        anyhow::ensure!(
            self.cell_usages.len() == 1 && self.cell_usages.contains_key(root_hash),
            "Invalid shard state cell"
        );
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum ReplaceTransactionError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid shard state packet")]
    InvalidShardStatePacket,
    #[error("Invalid cell")]
    InvalidCell,
}

const MAX_LEVEL: u8 = 3;
