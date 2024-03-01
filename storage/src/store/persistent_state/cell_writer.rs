use std::collections::hash_map;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use everscale_types::cell::HashBytes;
use everscale_types::models::BlockId;
use num_traits::ToPrimitive;
use smallvec::SmallVec;
use tycho_util::byte_reader::ByteOrderRead;
use tycho_util::FastHashMap;

use crate::db::Db;
use crate::{db, FileDb};

pub struct CellWriter<'a> {
    db: &'a Db,
    base_path: &'a Path,
}

impl<'a> CellWriter<'a> {
    pub fn clear_temp(base_path: &Path, master_block_id: &BlockId, block_id: &BlockId) {
        tracing::info!("Cleaning temporary persistent state files");

        let file_path = Self::make_pss_path(base_path, master_block_id, block_id);
        let int_file_path = Self::make_rev_pss_path(&file_path);
        let temp_file_path = Self::make_temp_pss_path(&file_path);

        let _ = fs::remove_file(int_file_path);
        let _ = fs::remove_file(temp_file_path);
    }

    pub fn make_pss_path(base_path: &Path, mc_block_id: &BlockId, block_id: &BlockId) -> PathBuf {
        let dir_path = mc_block_id.seqno.to_string();
        let file_name = block_id.root_hash.to_string();
        base_path.join(dir_path).join(file_name)
    }

    pub fn make_temp_pss_path(file_path: &Path) -> PathBuf {
        file_path.with_extension("temp")
    }

    pub fn make_rev_pss_path(file_path: &Path) -> PathBuf {
        file_path.with_extension("rev")
    }

    #[allow(unused)]
    pub fn new(db: &'a Db, base_path: &'a Path) -> Self {
        Self { db, base_path }
    }

    pub fn write(
        &self,
        master_block_id: &BlockId,
        block_id: &BlockId,
        state_root_hash: &HashBytes,
        is_cancelled: Arc<AtomicBool>,
    ) -> Result<PathBuf> {
        let file_path = Self::make_pss_path(self.base_path, master_block_id, block_id);

        // Load cells from db in reverse order into the temp file
        tracing::info!(block = %block_id, "Started loading cells");
        let now = Instant::now();
        let mut intermediate = write_rev_cells(
            self.db,
            Self::make_rev_pss_path(&file_path),
            state_root_hash.as_array(),
            is_cancelled.clone(),
        )
        .map_err(|e| {
            anyhow::Error::msg(format!("Failed to write reversed cells data. Inner: {e:?}"))
        })?;

        let temp_file_path = Self::make_temp_pss_path(&file_path);

        tracing::info!(block = %block_id, "Creating intermediate file {:?}", file_path);

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_file_path)
            .context("Failed to create target file")?;

        let cell_count = intermediate.cell_sizes.len() as u32;
        tracing::info!(
            elapsed = %humantime::format_duration(now.elapsed()),
            cell_count,
            block = %block_id,
            "Finished loading cells"
        );

        // Compute offset type size (usually 4 bytes)
        let offset_size =
            std::cmp::min(number_of_bytes_to_fit(intermediate.total_size), 8) as usize;

        // Reserve space for the file
        alloc_file(
            &file,
            22 + offset_size * (1 + cell_count as usize) + (intermediate.total_size as usize),
        )?;

        // Write cells data in BOC format
        let mut buffer = std::io::BufWriter::with_capacity(FILE_BUFFER_LEN / 2, file);

        // Header            | current len: 0
        let flags = 0b1000_0000u8 | (REF_SIZE as u8);
        buffer.write_all(&[0xb5, 0xee, 0x9c, 0x72, flags, offset_size as u8])?;

        // Unique cell count | current len: 6
        buffer.write_all(&cell_count.to_be_bytes())?;

        // Root count        | current len: 10
        buffer.write_all(&1u32.to_be_bytes())?;

        // Absent cell count | current len: 14
        buffer.write_all(&[0, 0, 0, 0])?;

        // Total cell size   | current len: 18
        buffer.write_all(&intermediate.total_size.to_be_bytes()[(8 - offset_size)..8])?;

        // Root index        | current len: 18 + offset_size
        buffer.write_all(&[0, 0, 0, 0])?;

        // Cells index       | current len: 22 + offset_size
        tracing::info!(block = %block_id, "Started building index");
        {
            let mut next_offset = 0;
            for &cell_size in intermediate.cell_sizes.iter().rev() {
                next_offset += cell_size as u64;
                buffer.write_all(&next_offset.to_be_bytes()[(8 - offset_size)..8])?;
            }
        }
        tracing::info!(block = %block_id, "Finished building index");

        // Cells             | current len: 22 + offset_size * (1 + cell_sizes.len())
        let mut cell_buffer = [0; 2 + 128 + 4 * REF_SIZE];
        for (i, &cell_size) in intermediate.cell_sizes.iter().rev().enumerate() {
            if i % 1000 == 0 && is_cancelled.load(Ordering::Relaxed) {
                anyhow::bail!("Persistent state writing cancelled.")
            }
            intermediate.total_size -= cell_size as u64;
            intermediate
                .file_db
                .file
                .seek(SeekFrom::Start(intermediate.total_size))?;
            intermediate
                .file_db
                .file
                .read_exact(&mut cell_buffer[..cell_size as usize])?;

            let d1 = cell_buffer[0];
            let d2 = cell_buffer[1];
            let ref_count = (d1 & 7) as usize;
            let data_size = ((d2 >> 1) + (d2 & 1 != 0) as u8) as usize;

            let ref_offset = 2 + data_size;
            for r in 0..ref_count {
                let ref_offset = ref_offset + r * REF_SIZE;
                let slice = &mut cell_buffer[ref_offset..ref_offset + REF_SIZE];

                let index = u32::from_be_bytes(slice.try_into().unwrap());
                slice.copy_from_slice(&(cell_count - index - 1).to_be_bytes());
            }

            buffer.write_all(&cell_buffer[..cell_size as usize])?;
        }

        buffer.flush()?;
        std::fs::rename(&temp_file_path, &file_path)?;

        Ok(file_path)
    }
}

struct IntermediateState {
    file_db: FileDb,
    cell_sizes: Vec<u8>,
    total_size: u64,
    _remove_on_drop: RemoveOnDrop,
}

fn write_rev_cells(
    db: &Db,
    file_path: PathBuf,
    state_root_hash: &[u8; 32],
    is_cancelled: Arc<AtomicBool>,
) -> Result<IntermediateState> {
    todo!()

    /*enum StackItem {
        New([u8; 32]),
        Loaded(LoadedCell),
    }

    struct LoadedCell {
        hash: [u8; 32],
        d1: u8,
        d2: u8,
        data: SmallVec<[u8; 128]>,
        indices: SmallVec<[u32; 4]>,
    }

    tracing::info!("Creating rev file {:?}", file_path);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&file_path)
        .context("Failed to write rev file")?;
    let remove_on_drop = RemoveOnDrop(file_path);

    let raw = db.raw().as_ref();
    let read_options = db.cells.read_config();
    let cf = db.cells.cf();

    let mut references_buffer = SmallVec::<[[u8; 32]; 4]>::with_capacity(4);

    let mut indices = FastHashMap::default();
    let mut remap = FastHashMap::default();
    let mut cell_sizes = Vec::<u8>::with_capacity(FILE_BUFFER_LEN);
    let mut stack = Vec::with_capacity(32);

    let mut total_size = 0u64;
    let mut iteration = 0u32;
    let mut remap_index = 0u32;

    stack.push((iteration, StackItem::New(*state_root_hash)));
    indices.insert(*state_root_hash, (iteration, false));

    let mut temp_file_buffer = std::io::BufWriter::with_capacity(FILE_BUFFER_LEN, file);

    while let Some((index, data)) = stack.pop() {
        if iteration % 1000 == 0 && is_cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("Persistent state writing cancelled.")
        }

        match data {
            StackItem::New(hash) => {
                let value = raw
                    .get_pinned_cf_opt(&cf, hash, read_options)?
                    .ok_or(CellWriterError::CellNotFound)?;

                let value = value.as_ref();

                let mut value = match db::refcount::strip_refcount(value) {
                    Some(bytes) => bytes,
                    None => return Err(CellWriterError::CellNotFound.into()),
                };
                if value.is_empty() {
                    return Err(CellWriterError::InvalidCell.into());
                }

                let cell_data = ton_types::CellData::deserialize(&mut value)?;
                let bit_length = cell_data.bit_length();
                let d2 = (((bit_length >> 2) as u8) & !0b1) | ((bit_length % 8 != 0) as u8);

                let references_count = cell_data.references_count();
                let cell_type = cell_data
                    .cell_type()
                    .to_u8()
                    .ok_or(CellWriterError::InvalidCell)?;

                let level_mask = cell_data.level_mask().mask();
                let d1 =
                    references_count as u8 | (((cell_type != 0x01) as u8) << 3) | (level_mask << 5);
                let data = cell_data.data();

                for _ in 0..references_count {
                    let hash = HashBytes::from(value.read_u256()?);
                    references_buffer.push(hash.inner());
                }

                let mut reference_indices = SmallVec::with_capacity(references_buffer.len());

                let mut indices_buffer = [0; 4];
                let mut keys = [std::ptr::null(); 4];
                let mut preload_count = 0;

                for hash in &references_buffer {
                    let index = match indices.entry(*hash) {
                        hash_map::Entry::Vacant(entry) => {
                            remap_index += 1;

                            entry.insert((remap_index, false));

                            indices_buffer[preload_count] = remap_index;
                            keys[preload_count] = hash.as_ptr();
                            preload_count += 1;

                            remap_index
                        }
                        hash_map::Entry::Occupied(entry) => {
                            let (remap_index, written) = *entry.get();
                            if !written {
                                indices_buffer[preload_count] = remap_index;
                                keys[preload_count] = hash.as_ptr();
                                preload_count += 1;
                            }
                            remap_index
                        }
                    };

                    reference_indices.push(index);
                }

                stack.push((
                    index,
                    StackItem::Loaded(LoadedCell {
                        hash,
                        d1,
                        d2,
                        data: SmallVec::from_slice(data),
                        indices: reference_indices,
                    }),
                ));

                if preload_count > 0 {
                    indices_buffer[..preload_count].reverse();
                    keys[..preload_count].reverse();

                    for i in 0..preload_count {
                        let index = indices_buffer[i];
                        let hash = unsafe { *(keys[i] as *const [u8; 32]) };
                        stack.push((index, StackItem::New(hash)));
                    }
                }

                references_buffer.clear();
            }
            StackItem::Loaded(loaded) => {
                match remap.entry(index) {
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(iteration.to_be_bytes());
                    }
                    hash_map::Entry::Occupied(_) => continue,
                };

                if let Some((_, written)) = indices.get_mut(&loaded.hash) {
                    *written = true;
                }

                iteration += 1;
                if iteration % 100000 == 0 {
                    tracing::info!(iteration);
                }

                let cell_size = 2 + loaded.data.len() + loaded.indices.len() * REF_SIZE;
                cell_sizes.push(cell_size as u8);
                total_size += cell_size as u64;

                temp_file_buffer.write_all(&[loaded.d1, loaded.d2])?;
                temp_file_buffer.write_all(&loaded.data)?;
                for index in loaded.indices {
                    let index = remap.get(&index).with_context(|| {
                        format!("Child not found. Iteration {iteration}. Child {index}")
                    })?;
                    temp_file_buffer.write_all(index)?;
                }
            }
        }
    }

    let mut file = temp_file_buffer.into_inner()?;
    file.flush()?;

    Ok(IntermediateState {
        file,
        cell_sizes,
        total_size,
        _remove_on_drop: remove_on_drop,
    })*/
}

#[cfg(not(target_os = "macos"))]
fn alloc_file(file: &File, len: usize) -> std::io::Result<()> {
    let res = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len as i64) };
    if res == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
pub fn alloc_file(file: &File, len: usize) -> std::io::Result<()> {
    let res = unsafe { libc::ftruncate(file.as_raw_fd(), len as i64) };
    if res < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn number_of_bytes_to_fit(l: u64) -> u32 {
    8 - l.leading_zeros() / 8
}

struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.0) {
            tracing::error!(path = %self.0.display(), "failed to remove file: {e:?}");
        }
    }
}

const REF_SIZE: usize = std::mem::size_of::<u32>();
const FILE_BUFFER_LEN: usize = 128 * 1024 * 1024; // 128 MB

#[derive(thiserror::Error, Debug)]
enum CellWriterError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
}
