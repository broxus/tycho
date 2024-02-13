use std::collections::hash_map;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use smallvec::SmallVec;

use crate::db::Db;
use tycho_block_util::FastHashMap;

pub struct CellWriter<'a> {
    db: &'a Db,
    base_path: &'a Path,
}

impl<'a> CellWriter<'a> {
    #[allow(unused)]
    pub fn new(db: &'a Db, base_path: &'a Path) -> Self {
        Self { db, base_path }
    }

    #[allow(unused)]
    pub fn write(&self, root_hash: &[u8; 32]) -> Result<()> {
        // Open target file in advance to get the error immediately (if any)
        let file_path = self.base_path.join(hex::encode(root_hash));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .context("Failed to create target file")?;

        // Load cells from db in reverse order into the temp file
        tracing::info!("started loading cells");
        let mut intermediate = write_rev_cells(self.db, self.base_path, root_hash)
            .context("Failed to write reversed cells data")?;
        tracing::info!("finished loading cells");
        let cell_count = intermediate.cell_sizes.len() as u32;

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
        tracing::info!("started building index");
        {
            let mut next_offset = 0;
            for &cell_size in intermediate.cell_sizes.iter().rev() {
                next_offset += cell_size as u64;
                buffer.write_all(&next_offset.to_be_bytes()[(8 - offset_size)..8])?;
            }
        }
        tracing::info!("finished building index");

        // Cells             | current len: 22 + offset_size * (1 + cell_sizes.len())
        let mut cell_buffer = [0; 2 + 128 + 4 * REF_SIZE];
        for &cell_size in intermediate.cell_sizes.iter().rev() {
            intermediate.total_size -= cell_size as u64;
            intermediate
                .file
                .seek(SeekFrom::Start(intermediate.total_size))?;
            intermediate
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

        Ok(())
    }
}

struct IntermediateState {
    file: File,
    cell_sizes: Vec<u8>,
    total_size: u64,
    _remove_on_drop: RemoveOnDrop,
}

fn write_rev_cells<P: AsRef<Path>>(
    db: &Db,
    base_path: P,
    root_hash: &[u8; 32],
) -> Result<IntermediateState> {
    enum StackItem {
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

    let file_path = base_path
        .as_ref()
        .join(hex::encode(root_hash))
        .with_extension("temp");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&file_path)
        .context("Failed to create temp file")?;
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

    stack.push((iteration, StackItem::New(*root_hash)));
    indices.insert(*root_hash, (iteration, false));

    let mut temp_file_buffer = std::io::BufWriter::with_capacity(FILE_BUFFER_LEN, file);

    while let Some((index, data)) = stack.pop() {
        match data {
            StackItem::New(hash) => {
                let value = raw
                    .get_pinned_cf_opt(&cf, hash, read_options)?
                    .ok_or(CellWriterError::CellNotFound)?;

                let value = value.as_ref();
                if value.is_empty() {
                    return Err(CellWriterError::InvalidCell.into());
                }

                let (d1, d2, data) = deserialize_cell(&value[1..], &mut references_buffer)
                    .ok_or(CellWriterError::InvalidCell)?;

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
    })
}

fn deserialize_cell<'a>(
    value: &'a [u8],
    references_buffer: &mut SmallVec<[[u8; 32]; 4]>,
) -> Option<(u8, u8, &'a [u8])> {
    let mut index = Index {
        value_len: value.len(),
        offset: 0,
    };

    index.require(3)?;
    let cell_type = value[*index];
    index.advance(1);
    let bit_length = u16::from_le_bytes((&value[*index..*index + 2]).try_into().unwrap());
    index.advance(2);

    let d2 = (((bit_length >> 2) as u8) & !0b1) | ((bit_length % 8 != 0) as u8);

    // TODO: Replace with `(big_length + 7) / 8`
    let data_len = ((d2 >> 1) + u8::from(d2 & 1 != 0)) as usize;
    index.require(data_len)?;
    let data = &value[*index..*index + data_len];

    // NOTE: additional byte is required here due to internal structure
    index.advance(((bit_length + 8) / 8) as usize);

    index.require(1)?;
    let level_mask = value[*index];
    // skip store_hashes
    index.advance(2);

    index.require(2)?;
    let has_hashes = value[*index];
    index.advance(1);
    if has_hashes != 0 {
        let count = value[*index];
        index.advance(1 + (count * 32) as usize);
    }

    index.require(2)?;
    let has_depths = value[*index];
    index.advance(1);
    if has_depths != 0 {
        let count = value[*index];
        index.advance(1 + (count * 2) as usize);
    }

    index.require(1)?;
    let reference_count = value[*index];
    index.advance(1);

    let d1 = reference_count | (((cell_type != 0x01) as u8) << 3) | (level_mask << 5);

    for _ in 0..reference_count {
        index.require(32)?;
        let mut hash = [0; 32];
        hash.copy_from_slice(&value[*index..*index + 32]);
        references_buffer.push(hash);
        index.advance(32);
    }

    Some((d1, d2, data))
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

struct Index {
    value_len: usize,
    offset: usize,
}

impl Index {
    #[inline(always)]
    fn require(&self, len: usize) -> Option<()> {
        if self.offset + len < self.value_len {
            Some(())
        } else {
            None
        }
    }

    #[inline(always)]
    fn advance(&mut self, bytes: usize) {
        self.offset += bytes;
    }
}

impl std::ops::Deref for Index {
    type Target = usize;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.offset
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
