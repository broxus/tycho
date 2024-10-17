use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use bumpalo::Bump;
use everscale_types::boc;
use everscale_types::models::BlockId;
use tycho_block_util::queue::{QueueDiffMessagesIter, QueueState, QueueStateHeader};
use tycho_util::compression::ZstdCompressedFile;
use tycho_util::FastHasherState;

use crate::db::FileDb;

const FILE_BUFFER_LEN: usize = 128 * 1024 * 1024; // 128 MB

pub struct QueueStateWriter<'a> {
    states_dir: &'a FileDb,
    block_id: &'a BlockId,
    state: QueueStateHeader,
    messages: Vec<QueueDiffMessagesIter>,
}

impl<'a> QueueStateWriter<'a> {
    pub const COMPRESSION_LEVEL: i32 = 9;

    pub const FILE_EXTENSION: &'static str = "queue";

    // Partially written queue file.
    const FILE_EXTENSION_TEMP: &'static str = "queue.temp";

    pub fn file_name(block_id: &BlockId) -> PathBuf {
        PathBuf::from(block_id.to_string()).with_extension(Self::FILE_EXTENSION)
    }

    pub fn temp_file_name(block_id: &BlockId) -> PathBuf {
        PathBuf::from(block_id.to_string()).with_extension(Self::FILE_EXTENSION_TEMP)
    }

    pub fn new(
        states_dir: &'a FileDb,
        block_id: &'a BlockId,
        state: QueueStateHeader,
        messages: Vec<QueueDiffMessagesIter>,
    ) -> Self {
        Self {
            states_dir,
            block_id,
            state,
            messages,
        }
    }

    pub fn write(self, is_cancelled: Option<&AtomicBool>) -> Result<()> {
        const MAX_ROOTS_PER_CHUNK: usize = 10000;
        const MAX_CHUNK_SIZE: u64 = 10 << 20; // 10 MB

        if let Some(is_cancelled) = is_cancelled {
            if is_cancelled.load(Ordering::Relaxed) {
                anyhow::bail!(Cancelled);
            }
        }

        let states_dir = self.states_dir;

        let temp_file_name = Self::temp_file_name(self.block_id);
        scopeguard::defer! {
            states_dir.remove_file(&temp_file_name).ok();
        }

        // Create and write to the temporary file
        let file = states_dir
            .file(&temp_file_name)
            .create(true)
            .write(true)
            .truncate(true)
            .open()?;
        let file = ZstdCompressedFile::new(file, Self::COMPRESSION_LEVEL, FILE_BUFFER_LEN)?;

        let mut buffer = BufWriter::with_capacity(FILE_BUFFER_LEN, file);

        // Write file magic
        buffer.write_all(&QueueState::TL_ID.to_le_bytes())?;

        // Write queue state header
        {
            // State header data
            let mut writer = tl_proto::IoWriter::new(&mut buffer);
            tl_proto::TlWrite::write_to(&self.state, &mut writer);
            let (_, status) = writer.into_parts();
            status.context("failed to write queue state header")?;
        }

        // Count the number of message roots
        let mut cell_count = 0;
        for diff in &self.state.queue_diffs {
            cell_count += diff.messages.len();
        }

        let chunk_count = (cell_count + MAX_ROOTS_PER_CHUNK - 1) / MAX_ROOTS_PER_CHUNK;
        buffer.write_all(&(chunk_count as u32).to_le_bytes())?;

        let mut bump = Bump::new();

        // TODO: Balance the chunk size to contain at most N bytes instead
        let mut iter = self.messages.into_iter().flatten();
        loop {
            bump.reset();

            let mut boc = boc::ser::BocHeader::<FastHasherState>::with_capacity(cell_count);

            let mut message_count = 0usize;
            let mut total_size = 0u64;
            for entry in iter.by_ref().take(MAX_ROOTS_PER_CHUNK) {
                if let Some(is_cancelled) = is_cancelled {
                    if message_count % 1000 == 0 && is_cancelled.load(Ordering::Relaxed) {
                        anyhow::bail!(Cancelled);
                    }
                }

                let message_cell = entry?.into_inner();

                let cell = &*bump.alloc(message_cell);
                boc.add_root(cell.as_ref());

                message_count += 1;

                // NOTE: `compute_stats` is relatively cheap since it just multiplies some sizes.
                total_size = boc.compute_stats().total_size;
                if total_size >= MAX_CHUNK_SIZE {
                    break;
                }
            }

            if message_count == 0 {
                break;
            }

            // "BigBytes" goes here

            // Write the total BOC size in bytes (u32 LE)
            buffer.write_all(&(total_size as u32).to_le_bytes())?;

            // Write the BOC
            boc.encode_to_writer(&mut buffer)
                .context("failed to serialize multi-root BOC with messages")?;

            // Write padding
            const PADDING: [u8; 3] = [0; 3];
            if total_size % 4 != 0 {
                buffer.write_all(&PADDING[0..4 - (total_size as usize) % 4])?;
            }
        }

        drop(bump);

        match buffer.into_inner() {
            Ok(mut file) => {
                file.finish()?;
                file.flush()?;
            }
            Err(e) => return Err(e.into_error()).context("failed to flush the compressed buffer"),
        }

        states_dir
            .file(&temp_file_name)
            .rename(Self::file_name(self.block_id))
            .map_err(Into::into)
    }
}

#[derive(thiserror::Error, Debug)]
#[error("persistent state queue writing cancelled")]
struct Cancelled;
