#![allow(clippy::disallowed_types)]
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::Path;

/// Memory buffer that is mapped to a file
pub struct MappedFile {
    #[allow(unused)]
    file: File,
    length: usize,
    ptr: *mut libc::c_void,
}

impl MappedFile {
    /// Opens a file and maps it to memory. Resizes the file to `length` bytes.
    pub fn new<P: AsRef<Path>>(path: P, length: usize) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .truncate(true)
            .create(true)
            .open(path)?;

        file.set_len(length as u64)?;

        Self::from_existing_file(file)
    }

    /// Opens an existing file and maps it to memory
    pub fn from_existing_file(file: File) -> std::io::Result<Self> {
        let length = file.metadata()?.len() as usize;

        // SAFETY: File was opened successfully, file mode is RW, offset is aligned
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        if unsafe { libc::madvise(ptr, length, libc::MADV_RANDOM) } != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self { file, length, ptr })
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

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr and length were initialized once on creation
        unsafe { std::slice::from_raw_parts(self.ptr.cast::<u8>(), self.length) }
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        // SAFETY: File still exists, ptr and length were initialized once on creation
        if unsafe { libc::munmap(self.ptr, self.length) } != 0 {
            // TODO: how to handle this?
            panic!("failed to unmap file: {}", std::io::Error::last_os_error());
        }
    }
}

unsafe impl Send for MappedFile {}
unsafe impl Sync for MappedFile {}
