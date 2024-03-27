use std::fs;
use std::path::Path;

use anyhow::Result;

use crate::FileDb;

/// Memory buffer that is mapped to a file
pub struct MappedFile {
    file_db: FileDb,
    length: usize,
    ptr: *mut libc::c_void,
}

impl MappedFile {
    /// Opens a file and maps it to memory. Resizes the file to `length` bytes.
    pub fn new<P>(path: &P, length: usize) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file_db = FileDb::new(
            path,
            fs::OpenOptions::new()
                .write(true)
                .read(true)
                .truncate(true)
                .create(true),
        )?;
        file_db.file.set_len(length as u64)?;

        Self::from_existing_file(file_db)
    }

    /// Opens an existing file and maps it to memory
    pub fn from_existing_file(file_db: FileDb) -> Result<Self> {
        use std::os::unix::io::AsRawFd;

        let length = file_db.file.metadata()?.len() as usize;

        // SAFETY: File was opened successfully, file mode is RW, offset is aligned
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file_db.file.as_raw_fd(),
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }

        if unsafe { libc::madvise(ptr, length, libc::MADV_RANDOM) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(Self {
            file_db,
            length,
            ptr,
        })
    }

    /// Mapped buffer length in bytes
    pub fn length(&self) -> usize {
        self.length
    }

    /// Copies chunk of bytes to the specified buffer
    ///
    /// # Safety
    /// The caller must take care that the buffer is not out of the mapped memory!
    pub unsafe fn read_exact_at(&self, offset: usize, buffer: &mut [u8]) {
        std::ptr::copy_nonoverlapping(
            (self.ptr as *const u8).add(offset),
            buffer.as_mut_ptr(),
            buffer.len(),
        );
    }

    /// Copies buffer to the mapped memory
    ///
    /// # Safety
    /// The caller must take care that the buffer is not out of the mapped memory!
    pub unsafe fn write_all_at(&self, offset: usize, buffer: &[u8]) {
        std::ptr::copy_nonoverlapping(
            buffer.as_ptr(),
            (self.ptr.cast::<u8>()).add(offset),
            buffer.len(),
        );
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        // SAFETY: File still exists, ptr and length were initialized once on creation
        if unsafe { libc::munmap(self.ptr, self.length) } != 0 {
            // TODO: how to handle this?
            panic!("failed to unmap file: {}", std::io::Error::last_os_error());
        }

        let _ = self.file_db.file.set_len(0);
        let _ = self.file_db.file.sync_all();
    }
}

unsafe impl Send for MappedFile {}
unsafe impl Sync for MappedFile {}
