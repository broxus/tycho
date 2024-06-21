#![allow(clippy::disallowed_types)]
use std::fs::File;
use std::io::{IoSlice, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::Mutex;

use crate::archive::Archive;

pub struct ArchiveWriter {
    pool_state: Arc<ArchiveWritersPoolState>,
    state: ArchiveWriterState,
}

impl ArchiveWriter {
    pub fn parse_archive(&self) -> Result<Archive> {
        match &self.state {
            ArchiveWriterState::InMemory(buffer) => Archive::new(buffer),
            ArchiveWriterState::File { file, .. } => {
                let mapped_file =
                    FileWriter::new(file).context("Failed to map temp archive file")?;

                Archive::new(mapped_file.as_slice())
            }
        }
    }

    fn acquire_memory(&mut self, additional: usize) -> std::io::Result<()> {
        if let ArchiveWriterState::InMemory(buffer) = &self.state {
            let move_to_file = {
                let mut acquired_memory = self.pool_state.acquired_memory.lock();
                if *acquired_memory + additional > self.pool_state.save_to_disk_threshold {
                    *acquired_memory -= buffer.len();
                    true
                } else {
                    *acquired_memory += additional;
                    false
                }
            };

            if move_to_file {
                let (path, mut file) = self.pool_state.acquire_file()?;
                file.write_all(buffer)?;
                self.state = ArchiveWriterState::File { path, file };
            }
        }

        Ok(())
    }
}

impl Drop for ArchiveWriter {
    fn drop(&mut self) {
        match &self.state {
            ArchiveWriterState::InMemory(buffer) => {
                *self.pool_state.acquired_memory.lock() -= buffer.len();
            }
            ArchiveWriterState::File { path, .. } => {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::error!(
                        target: "sync",
                        path = %path.display(),
                        "failed to remove temp archive file: {e:?}"
                    );
                }
            }
        }
    }
}

impl Write for ArchiveWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.acquire_memory(buf.len())?;

        match &mut self.state {
            ArchiveWriterState::InMemory(buffer) => buffer.write(buf),
            ArchiveWriterState::File { file, .. } => file.write(buf),
        }
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        let len = bufs.iter().map(|b| b.len()).sum();

        self.acquire_memory(len)?;

        match &mut self.state {
            ArchiveWriterState::InMemory(buffer) => {
                buffer.reserve(len);
                for buf in bufs {
                    buffer.extend_from_slice(buf);
                }
                Ok(len)
            }
            ArchiveWriterState::File { file, .. } => file.write_vectored(bufs),
        }
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.state {
            ArchiveWriterState::InMemory(_) => Ok(()),
            ArchiveWriterState::File { file, .. } => file.flush(),
        }
    }
}

struct ArchiveWritersPoolState {
    save_to_disk_threshold: usize,
    // NOTE: `AtomicUsize` is not used here because there is a complex
    // InMemory-to-File transition
    acquired_memory: Mutex<usize>,
    temp_file_index: AtomicUsize,
    base_path: PathBuf,
}

impl ArchiveWritersPoolState {
    fn acquire_file(&self) -> std::io::Result<(PathBuf, File)> {
        let temp_file_index = self.temp_file_index.fetch_add(1, Ordering::AcqRel);
        let path = self
            .base_path
            .join(format!("temp_archive{temp_file_index:04}"));

        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Ok((path, file))
    }
}

enum ArchiveWriterState {
    InMemory(Vec<u8>),
    File { path: PathBuf, file: File },
}

struct FileWriter<'a> {
    _file: &'a File,
    length: usize,
    ptr: *mut libc::c_void,
}

impl<'a> FileWriter<'a> {
    fn new(file: &'a File) -> std::io::Result<Self> {
        use std::os::unix::io::AsRawFd;

        let length = file.metadata()?.len() as usize;

        // SAFETY: File was opened successfully, file mode is R, offset is aligned
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        if unsafe { libc::madvise(ptr, length, libc::MADV_SEQUENTIAL) } != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            _file: file,
            length,
            ptr,
        })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.length) }
    }
}

impl Drop for FileWriter<'_> {
    fn drop(&mut self) {
        // SAFETY: File still exists, ptr and length were initialized once on creation
        if unsafe { libc::munmap(self.ptr, self.length) } != 0 {
            // TODO: how to handle this?
            let e = std::io::Error::last_os_error();
            panic!("failed to unmap temp archive file: {e:?}");
        }
    }
}
