use std::collections::hash_map;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use everscale_types::cell::{CellDescriptor, HashBytes};
use everscale_types::models::*;
use smallvec::SmallVec;
use tycho_storage::FileDb;
use tycho_util::compression::ZstdCompressedFile;
use tycho_util::sync::CancellationFlag;
use tycho_util::FastHashMap;

use crate::storage::CoreDb;

pub struct ShardStateWriter<'a> {
    db: &'a CoreDb,
    states_dir: &'a FileDb,
    block_id: &'a BlockId,
}

impl<'a> ShardStateWriter<'a> {
    pub const COMPRESSION_LEVEL: i32 = 9;

    pub const FILE_EXTENSION: &'static str = "boc";

    // Partially written BOC file.
    const FILE_EXTENSION_TEMP: &'static str = "boc.temp";

    pub fn file_name(block_id: &BlockId) -> PathBuf {
        PathBuf::from(block_id.to_string()).with_extension(Self::FILE_EXTENSION)
    }

    pub fn temp_file_name(block_id: &BlockId) -> PathBuf {
        PathBuf::from(block_id.to_string()).with_extension(Self::FILE_EXTENSION_TEMP)
    }

    pub fn new(db: &'a CoreDb, states_dir: &'a FileDb, block_id: &'a BlockId) -> Self {
        Self {
            db,
            states_dir,
            block_id,
        }
    }

    pub fn write_file(
        &self,
        mut boc_file: File,
        _cancelled: Option<&CancellationFlag>,
    ) -> Result<()> {
        let temp_file_name = Self::temp_file_name(self.block_id);
        scopeguard::defer! {
            self.states_dir.remove_file(&temp_file_name).ok();
        }

        boc_file.seek(SeekFrom::Start(0))?;

        // Create states file
        let compressed_file = self
            .states_dir
            .file(&temp_file_name)
            .create(true)
            .write(true)
            .truncate(true)
            .open()?;

        let mut compressed_file = ZstdCompressedFile::new(
            compressed_file,
            Self::COMPRESSION_LEVEL,
            FILE_BUFFER_LEN / 2,
        )?;

        // TODO: Find a way to cancel this operation.
        std::io::copy(&mut boc_file, &mut compressed_file)?;

        // Terminate the compressor and flush the file
        compressed_file.finish()?.flush()?;

        // Atomically rename the file
        self.states_dir
            .file(&temp_file_name)
            .rename(Self::file_name(self.block_id))
            .map_err(Into::into)
    }

    pub fn write(&self, root_hash: &HashBytes, cancelled: Option<&CancellationFlag>) -> Result<()> {
        let temp_file_name = Self::temp_file_name(self.block_id);
        scopeguard::defer! {
            self.states_dir.remove_file(&temp_file_name).ok();
        }

        // Load cells from db in reverse order into the temp file
        tracing::info!("started loading cells");
        let mut intermediate = self
            .write_rev(&root_hash.0, cancelled)
            .context("Failed to write reversed cells data")?;
        tracing::info!("finished loading cells");
        let cell_count = intermediate.cell_sizes.len() as u32;

        // Compute offset type size (usually 4 bytes)
        let offset_size =
            std::cmp::min(number_of_bytes_to_fit(intermediate.total_size), 8) as usize;

        // Compute file size
        let file_size =
            22 + offset_size * (1 + cell_count as usize) + (intermediate.total_size as usize);

        // Create states file
        let file = self
            .states_dir
            .file(&temp_file_name)
            .create(true)
            .write(true)
            .truncate(true)
            .prealloc(file_size)
            .open()?;
        let file = ZstdCompressedFile::new(file, Self::COMPRESSION_LEVEL, FILE_BUFFER_LEN / 2)?;

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

        let mut cancelled = cancelled.map(|c| c.debounce(1000));
        for &cell_size in intermediate.cell_sizes.iter().rev() {
            if let Some(cancelled) = &mut cancelled {
                if cancelled.check() {
                    anyhow::bail!("Cell writing cancelled")
                }
            }

            intermediate.total_size -= cell_size as u64;
            intermediate
                .file
                .seek(SeekFrom::Start(intermediate.total_size))?;
            intermediate
                .file
                .read_exact(&mut cell_buffer[..cell_size as usize])?;

            let descriptor = CellDescriptor {
                d1: cell_buffer[0],
                d2: cell_buffer[1],
            };

            let ref_offset = 2 + descriptor.byte_len() as usize;
            for r in 0..descriptor.reference_count() as usize {
                let ref_offset = ref_offset + r * REF_SIZE;
                let slice = &mut cell_buffer[ref_offset..ref_offset + REF_SIZE];

                let index = u32::from_be_bytes(slice.try_into().unwrap());
                slice.copy_from_slice(&(cell_count - index - 1).to_be_bytes());
            }

            buffer.write_all(&cell_buffer[..cell_size as usize])?;
        }

        match buffer.into_inner() {
            Ok(file) => {
                let mut file = file.finish()?;
                file.flush()?;

                // Truncate file to the resulting file size
                let file_size = file.stream_position()?;
                file.set_len(file_size)?;
            }
            Err(e) => return Err(e.into_error()).context("failed to flush the compressed buffer"),
        }

        self.states_dir
            .file(&temp_file_name)
            .rename(Self::file_name(self.block_id))
            .map_err(Into::into)
    }

    fn write_rev(
        &self,
        root_hash: &[u8; 32],
        cancelled: Option<&CancellationFlag>,
    ) -> Result<IntermediateState> {
        enum StackItem {
            New([u8; 32]),
            Loaded(LoadedCell),
        }

        struct LoadedCell {
            hash: [u8; 32],
            descriptor: CellDescriptor,
            data: SmallVec<[u8; 128]>,
            indices: SmallVec<[u32; 4]>,
        }

        let mut file = self.states_dir.unnamed_file().open()?;

        let raw = self.db.rocksdb().as_ref();
        let read_options = self.db.cells.read_config();
        let cf = self.db.cells.cf();

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

        let mut temp_file_buffer = std::io::BufWriter::with_capacity(FILE_BUFFER_LEN, &mut file);

        let mut cancelled = cancelled.map(|c| c.debounce(1000));
        while let Some((index, data)) = stack.pop() {
            if let Some(cancelled) = &mut cancelled {
                if cancelled.check() {
                    anyhow::bail!("Persistent state writing cancelled")
                }
            }

            match data {
                StackItem::New(hash) => {
                    let value = raw
                        .get_pinned_cf_opt(&cf, hash, read_options)?
                        .ok_or(CellWriterError::CellNotFound)?;

                    let value = match tycho_storage::refcount::strip_refcount(value.as_ref()) {
                        Some(bytes) => bytes,
                        None => {
                            return Err(CellWriterError::CellNotFound.into());
                        }
                    };
                    if value.is_empty() {
                        return Err(CellWriterError::InvalidCell.into());
                    }

                    let (descriptor, data) = deserialize_cell(value, &mut references_buffer)
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
                            descriptor,
                            data: SmallVec::from_slice(data),
                            indices: reference_indices,
                        }),
                    ));

                    if preload_count > 0 {
                        indices_buffer[..preload_count].reverse();
                        keys[..preload_count].reverse();

                        for i in 0..preload_count {
                            let index = indices_buffer[i];
                            let hash = unsafe { *keys[i].cast::<[u8; 32]>() };
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

                    temp_file_buffer.write_all(&[loaded.descriptor.d1, loaded.descriptor.d2])?;
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

        drop(temp_file_buffer);

        file.flush()?;

        Ok(IntermediateState {
            file,
            cell_sizes,
            total_size,
        })
    }
}

struct IntermediateState {
    file: File,
    cell_sizes: Vec<u8>,
    total_size: u64,
}

fn deserialize_cell<'a>(
    value: &'a [u8],
    references_buffer: &mut SmallVec<[[u8; 32]; 4]>,
) -> Option<(CellDescriptor, &'a [u8])> {
    let mut index = Index {
        value_len: value.len(),
        offset: 0,
    };

    index.require(4)?;
    let mut descriptor = CellDescriptor::new([value[*index], value[*index + 1]]);
    descriptor.d1 &= !CellDescriptor::STORE_HASHES_MASK;

    index.advance(2);
    let bit_length = u16::from_le_bytes([value[*index], value[*index + 1]]);
    index.advance(2);

    let data_len = descriptor.byte_len() as usize;
    index.require(data_len)?;
    let data = &value[*index..*index + data_len];
    index.advance(data_len);

    assert_eq!((bit_length as usize).div_ceil(8), data_len);

    index.advance((32 + 2) * descriptor.hash_count() as usize);

    for _ in 0..descriptor.reference_count() {
        index.require(32)?;
        let mut hash = [0; 32];
        hash.copy_from_slice(&value[*index..*index + 32]);
        references_buffer.push(hash);
        index.advance(32);
    }

    Some((descriptor, data))
}

fn number_of_bytes_to_fit(l: u64) -> u32 {
    8 - l.leading_zeros() / 8
}

struct Index {
    value_len: usize,
    offset: usize,
}

impl Index {
    #[inline(always)]
    fn require(&self, len: usize) -> Option<()> {
        if self.offset + len <= self.value_len {
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
