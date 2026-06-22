use std::collections::hash_map;
use std::fmt::Display;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tycho_storage::fs::Dir;
use tycho_types::cell::{Cell, CellDescriptor, HashBytes};
use tycho_types::models::*;
use tycho_util::FastHashMap;
use tycho_util::compression::ZstdCompressedFile;
use tycho_util::progress_bar::ProgressBar;
use tycho_util::sync::CancellationFlag;

use crate::storage::CellsDb;
use crate::storage::shard_state::decode_indexed_value;

pub struct ShardStateWriter<'a> {
    db: &'a CellsDb,
    states_dir: &'a Dir,
    block_id: &'a BlockId,
    part_prefix: Option<u64>,
}

impl<'a> ShardStateWriter<'a> {
    pub const COMPRESSION_LEVEL: i32 = 9;

    pub const FILE_EXTENSION: &'static str = "boc";

    // Partially written BOC file.
    const FILE_EXTENSION_TEMP: &'static str = "boc.temp";

    pub const META_FILE_EXTENSION: &'static str = "meta.json";

    fn build_file_name_base<S>(name: S, part_prefix: Option<u64>) -> String
    where
        S: Display,
    {
        match part_prefix {
            Some(prefix) => format!("{name}_part_{prefix:016x}"),
            None => name.to_string(),
        }
    }

    pub fn file_name<S>(name: S) -> PathBuf
    where
        S: Display,
    {
        Self::file_name_ext(name, None)
    }

    pub fn file_name_ext<S>(name: S, part_prefix: Option<u64>) -> PathBuf
    where
        S: Display,
    {
        PathBuf::from(Self::build_file_name_base(name, part_prefix))
            .with_extension(Self::FILE_EXTENSION)
    }

    pub fn temp_file_name<S>(name: S) -> PathBuf
    where
        S: Display,
    {
        Self::temp_file_name_ext(name, None)
    }

    pub fn temp_file_name_ext<S>(name: S, part_prefix: Option<u64>) -> PathBuf
    where
        S: Display,
    {
        PathBuf::from(Self::build_file_name_base(name, part_prefix))
            .with_extension(Self::FILE_EXTENSION_TEMP)
    }

    pub fn meta_file_name(block_id: &BlockId) -> PathBuf {
        PathBuf::from(block_id.to_string()).with_extension(Self::META_FILE_EXTENSION)
    }

    pub fn new(db: &'a CellsDb, states_dir: &'a Dir, block_id: &'a BlockId) -> Self {
        Self {
            db,
            states_dir,
            block_id,
            part_prefix: None,
        }
    }

    pub fn new_part(
        db: &'a CellsDb,
        states_dir: &'a Dir,
        block_id: &'a BlockId,
        part_prefix: u64,
    ) -> Self {
        Self {
            db,
            states_dir,
            block_id,
            part_prefix: Some(part_prefix),
        }
    }

    pub fn write_file(
        &self,
        mut boc_file: File,
        cancelled: Option<&CancellationFlag>,
    ) -> Result<()> {
        boc_file.seek(SeekFrom::Start(0))?;
        self.write_uncompressed_boc(boc_file, cancelled)
    }

    fn write_uncompressed_boc<R: Read>(
        &self,
        mut boc_file: R,
        _cancelled: Option<&CancellationFlag>,
    ) -> Result<()> {
        let temp_file_name = Self::temp_file_name_ext(self.block_id, self.part_prefix);
        scopeguard::defer! {
            self.states_dir.remove_file(&temp_file_name).ok();
        }

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
            .rename(Self::file_name_ext(self.block_id, self.part_prefix))
            .map_err(Into::into)
    }

    pub fn write(
        &self,
        root_hash: &HashBytes,
        cancelled: Option<&CancellationFlag>,
    ) -> Result<HashBytes> {
        self.write_inner(root_hash, None, None, None, cancelled)
    }

    pub fn write_with_absent(
        &self,
        root_hash: &HashBytes,
        to_make_absent_cells: FastHashMap<HashBytes, Cell>,
        cancelled: Option<&CancellationFlag>,
    ) -> Result<HashBytes> {
        self.write_inner(root_hash, None, Some(to_make_absent_cells), None, cancelled)
    }

    pub fn write_tracked(
        &self,
        root_hash: &HashBytes,
        file_name: &str,
        progress_bar: &mut ProgressBar,
        cancelled: Option<&CancellationFlag>,
    ) -> Result<HashBytes> {
        self.write_inner(
            root_hash,
            Some(file_name),
            None,
            Some(progress_bar),
            cancelled,
        )
    }

    fn write_inner(
        &self,
        root_hash: &HashBytes,
        file_name: Option<&str>,
        to_make_absent_cells: Option<FastHashMap<HashBytes, Cell>>,
        progress_bar: Option<&mut ProgressBar>,
        cancelled: Option<&CancellationFlag>,
    ) -> Result<HashBytes> {
        let temp_file_name = match file_name {
            Some(name) => Self::temp_file_name_ext(name, self.part_prefix),
            None => Self::temp_file_name_ext(self.block_id, self.part_prefix),
        };

        scopeguard::defer! {
            self.states_dir.remove_file(&temp_file_name).ok();
        }

        // Load cells from db in reverse order into the temp file
        tracing::info!("started loading cells");
        let intermediate = self
            .write_rev(&root_hash.0, to_make_absent_cells, cancelled)
            .context("Failed to write reversed cells data")?;
        tracing::info!("finished loading cells");

        self.write_intermediate(
            temp_file_name.clone(),
            file_name,
            intermediate,
            progress_bar,
            cancelled,
        )
    }

    fn write_intermediate(
        &self,
        temp_file_name: PathBuf,
        file_name: Option<&str>,
        mut intermediate: IntermediateState,
        mut progress_bar: Option<&mut ProgressBar>,
        cancelled: Option<&CancellationFlag>,
    ) -> Result<HashBytes> {
        let cell_count = intermediate.cell_sizes.len() as u32;

        // Compute offset type size (usually 4 bytes)
        let offset_size =
            std::cmp::min(number_of_bytes_to_fit(intermediate.total_size), 8) as usize;

        // Compute file size
        let file_size =
            22 + offset_size * (1 + cell_count as usize) + (intermediate.total_size as usize);

        if let Some(pg) = progress_bar.as_mut() {
            pg.set_total(file_size as u64);
        }

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

        let hasher = IntermediateHasher::new(file);

        // Write cells data in BOC format
        let mut buffer = std::io::BufWriter::with_capacity(FILE_BUFFER_LEN / 2, hasher);

        // Header            | current len: 0
        let flags = 0b1000_0000u8 | (REF_SIZE as u8);
        buffer.write_all(&[0xb5, 0xee, 0x9c, 0x72, flags, offset_size as u8])?;

        // Unique cell count | current len: 6
        buffer.write_all(&cell_count.to_be_bytes())?;

        // Root count        | current len: 10
        buffer.write_all(&1u32.to_be_bytes())?;

        // Absent cell count | current len: 14
        buffer.write_all(&intermediate.absent_cells_count.to_be_bytes())?;

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
            if let Some(cancelled) = &mut cancelled
                && cancelled.check()
            {
                anyhow::bail!("Cell writing cancelled")
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

            // skip refs for absent cells
            if !descriptor.is_absent() {
                let hash_depth_len = if descriptor.store_hashes() {
                    descriptor.hash_count() * (32 + 2)
                } else {
                    0
                };
                let ref_offset = 2 + hash_depth_len as usize + descriptor.byte_len() as usize;
                for r in 0..descriptor.reference_count() as usize {
                    let ref_offset = ref_offset + r * REF_SIZE;
                    let slice = &mut cell_buffer[ref_offset..ref_offset + REF_SIZE];

                    let index = u32::from_be_bytes(slice.try_into().unwrap());
                    slice.copy_from_slice(&(cell_count - index - 1).to_be_bytes());
                }
            }

            buffer.write_all(&cell_buffer[..cell_size as usize])?;
            if let Some(pg) = progress_bar.as_mut() {
                pg.add_progress(cell_size as u64);
            }
        }

        let file_hash = match buffer.into_inner() {
            Ok(intermediate_hasher) => {
                let (hash, compressed_file) = intermediate_hasher.finalize();

                let mut file = compressed_file.finish()?;
                file.flush()?;

                let file_size = file.stream_position()?;
                file.set_len(file_size)?;

                HashBytes(hash)
            }
            Err(e) => return Err(e.into_error()).context("failed to flush the compressed buffer"),
        };

        let name = match file_name {
            None => Self::file_name_ext(self.block_id, self.part_prefix),
            Some(name) => Self::file_name_ext(name, self.part_prefix),
        };

        self.states_dir.file(&temp_file_name).rename(name)?;

        Ok(file_hash)
    }

    fn write_rev(
        &self,
        root_hash: &[u8; 32],
        mut to_make_absent_cells: Option<FastHashMap<HashBytes, Cell>>,
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

        // data buffer for absent cell
        let mut absent_data_buffer = Vec::new();

        let mut absent_cells_count = 0u32;

        let mut indices = FastHashMap::default();
        let mut remap = FastHashMap::default();
        let mut cell_sizes = Vec::<u8>::with_capacity(FILE_BUFFER_LEN);
        let mut stack = Vec::with_capacity(32);

        let mut total_size = 0u64;
        let mut iteration = 0u32;
        let mut remap_index = 0u32;

        // we put cells in the stack and traverse down one branch until the leaf is reached,
        // then write this leaf cell, go back to the parent cell, and visit the sibling branch,
        // so when both child branches are stored, we continue moving backward

        // the leaf will have index 0 and the root will have the maximum index
        // parent cells refer to children by indexes
        // e.g. for the original branch
        // A -> B -> D
        //   -> C -> E
        // the intermediate file will contain
        // E[0; ] C[1; ref 0] D[2; ] B[3; ref 2] A[4; ref 1,3]

        stack.push((iteration, StackItem::New(*root_hash)));
        indices.insert(*root_hash, (iteration, false));

        let mut temp_file_buffer = std::io::BufWriter::with_capacity(FILE_BUFFER_LEN, &mut file);

        let mut cancelled = cancelled.map(|c| c.debounce(1000));
        while let Some((index, data)) = stack.pop() {
            if let Some(cancelled) = &mut cancelled
                && cancelled.check()
            {
                anyhow::bail!("Persistent state writing cancelled")
            }

            match data {
                StackItem::New(hash) => {
                    let hash_bytes = HashBytes::from(hash);

                    let to_make_absent_cell = to_make_absent_cells
                        .as_mut()
                        .and_then(|map| map.remove(&hash_bytes));

                    let (descriptor, data) = if let Some(to_make_absent_cell) = &to_make_absent_cell
                    {
                        // count absent cells
                        absent_cells_count += 1;

                        let descriptor =
                            make_absent_cell_data(to_make_absent_cell, &mut absent_data_buffer);

                        references_buffer.clear();
                        (
                            descriptor,
                            SmallVec::from_slice(absent_data_buffer.as_slice()),
                        )
                    } else {
                        // read cell from db
                        let value = raw
                            .get_pinned_cf_opt(&cf, hash, read_options)?
                            .ok_or(CellWriterError::CellNotFound(hash_bytes))?;

                        let (_, value) = decode_indexed_value(value.as_ref())
                            .ok_or(CellWriterError::InvalidCell(hash_bytes))?;
                        if value.is_empty() {
                            return Err(CellWriterError::InvalidCell(hash_bytes).into());
                        }

                        let (descriptor, data) = deserialize_cell(value, &mut references_buffer)
                            .ok_or(CellWriterError::InvalidCell(hash_bytes))?;

                        (descriptor, SmallVec::from_slice(data))
                    };

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
                            data,
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
                    if iteration.is_multiple_of(100000) {
                        tracing::info!(iteration);
                    }

                    // absent cell contain only 1 descriptor byte
                    let descriptor_len = 1 + !loaded.descriptor.is_absent() as usize;

                    let cell_size =
                        descriptor_len + loaded.data.len() + loaded.indices.len() * REF_SIZE;
                    cell_sizes.push(cell_size as u8);
                    total_size += cell_size as u64;

                    if descriptor_len > 1 {
                        temp_file_buffer
                            .write_all(&[loaded.descriptor.d1, loaded.descriptor.d2])?;
                    } else {
                        temp_file_buffer.write_all(&[loaded.descriptor.d1])?;
                    }

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

        ensure_absent_cells_consumed(&to_make_absent_cells)?;

        drop(temp_file_buffer);

        file.flush()?;

        Ok(IntermediateState {
            file,
            cell_sizes,
            total_size,
            absent_cells_count,
        })
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct PersistentStateMeta {
    pub split_depth: u8,
    pub parts: Vec<u64>,
}

impl PersistentStateMeta {
    pub const VERSION: u8 = 1;

    pub fn new(split_depth: u8, mut parts: Vec<u64>) -> Self {
        parts.sort_unstable();
        parts.dedup();
        Self { split_depth, parts }
    }

    pub fn write(&self, states_dir: &Dir, block_id: &BlockId) -> Result<()> {
        let file_path = states_dir
            .path()
            .join(ShardStateWriter::meta_file_name(block_id));
        self.write_to_file(file_path)
    }

    pub fn write_to_file(&self, file_path: impl AsRef<std::path::Path>) -> Result<()> {
        let raw = RawPersistentStateMeta {
            version: Self::VERSION,
            split_depth: self.split_depth,
            parts: self
                .parts
                .iter()
                .map(|prefix| format!("{prefix:016x}"))
                .collect(),
        };

        let file_path = file_path.as_ref();
        let temp_file_path = file_path.with_extension("temp");
        scopeguard::defer! {
            std::fs::remove_file(&temp_file_path).ok();
        }

        tycho_util::serde_helpers::save_json_to_file(&raw, &temp_file_path)?;
        std::fs::rename(&temp_file_path, file_path)?;

        Ok(())
    }

    pub fn read(states_dir: &Dir, block_id: &BlockId) -> Result<Option<Self>> {
        let file_path = states_dir
            .path()
            .join(ShardStateWriter::meta_file_name(block_id));
        Self::read_from_file(file_path)
    }

    pub fn read_from_file(file_path: impl AsRef<std::path::Path>) -> Result<Option<Self>> {
        if !file_path.as_ref().exists() {
            return Ok(None);
        }

        let raw: RawPersistentStateMeta =
            tycho_util::serde_helpers::load_json_from_file(file_path)?;
        anyhow::ensure!(
            raw.version == Self::VERSION,
            "unsupported persistent state meta version: {}",
            raw.version
        );
        let mut parts = Vec::with_capacity(raw.parts.len());
        for prefix in raw.parts {
            if prefix.len() != 16 || !prefix.chars().all(|c| c.is_ascii_hexdigit()) {
                anyhow::bail!("invalid persistent state part prefix: {prefix}");
            }
            parts.push(u64::from_str_radix(&prefix, 16)?);
        }

        Ok(Some(Self::new(raw.split_depth, parts)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RawPersistentStateMeta {
    version: u8,
    split_depth: u8,
    #[serde(default)]
    parts: Vec<String>,
}

struct IntermediateState {
    file: File,
    cell_sizes: Vec<u8>,
    total_size: u64,
    absent_cells_count: u32,
}

fn deserialize_cell<'a>(
    value: &'a [u8],
    references_buffer: &mut SmallVec<[[u8; 32]; 4]>,
) -> Option<(CellDescriptor, &'a [u8])> {
    let mut index = Index {
        value_len: value.len(),
        offset: 0,
    };

    index.require(6)?;
    let mut descriptor = CellDescriptor::new([value[*index], value[*index + 1]]);
    descriptor.d1 &= !CellDescriptor::STORE_HASHES_MASK;

    index.advance(2);
    let bit_length = u16::from_le_bytes([value[*index], value[*index + 1]]);
    index.advance(4); // also skip repr depth

    let data_len = descriptor.byte_len() as usize;
    index.require(data_len)?;
    let data = &value[*index..*index + data_len];
    index.advance(data_len);

    assert_eq!((bit_length as usize).div_ceil(8), data_len);

    index.advance((32 + 2) * (descriptor.hash_count() - 1) as usize);

    for _ in 0..descriptor.reference_count() {
        index.require(32)?;
        let mut hash = [0; 32];
        hash.copy_from_slice(&value[*index..*index + 32]);
        references_buffer.push(hash);
        index.advance(32);
    }

    Some((descriptor, data))
}

fn make_absent_cell_data(cell: &Cell, absent_data_buffer: &mut Vec<u8>) -> CellDescriptor {
    let level_mask = cell.level_mask();
    let d1 = CellDescriptor::ABSENT_MASK | level_mask.to_byte() << 5;
    let absent_descriptor = CellDescriptor::new([d1, 0]);

    absent_data_buffer.clear();
    absent_data_buffer.reserve(absent_descriptor.hash_count() as usize * (32 + 2));

    for level in level_mask {
        absent_data_buffer.extend_from_slice(cell.hash(level).as_slice());
    }

    for level in level_mask {
        absent_data_buffer.extend_from_slice(&cell.depth(level).to_be_bytes());
    }

    absent_descriptor
}

fn ensure_absent_cells_consumed(
    to_make_absent_cells: &Option<FastHashMap<HashBytes, Cell>>,
) -> Result<()> {
    if let Some(to_make_absent_cells) = to_make_absent_cells {
        let remaining = to_make_absent_cells.len();
        anyhow::ensure!(
            remaining == 0,
            "not all requested absent cells were written: remaining={}, sample={:?}",
            remaining,
            to_make_absent_cells.keys().take(4).collect::<Vec<_>>(),
        );
    }
    Ok(())
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
    #[error("Cell {0} not found in cell db")]
    CellNotFound(HashBytes),
    #[error("Invalid cell {0}")]
    InvalidCell(HashBytes),
}

struct IntermediateHasher<W: Write> {
    inner: W,
    hasher: blake3::Hasher,
}

impl<W: Write> IntermediateHasher<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
        }
    }

    pub fn finalize(self) -> ([u8; 32], W) {
        (self.hasher.finalize().into(), self.inner)
    }
}

impl<W: Write> Write for IntermediateHasher<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
