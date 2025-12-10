use std::fs::File;
use std::os::fd::AsRawFd;

use tycho_util::compression::ZstdDecompressStream;

/// Mutable memory buffer that is mapped to a file
pub struct MappedFileMut {
    inner: MappedFile,
}

impl MappedFileMut {
    /// Opens an existing file and maps it to memory
    pub fn from_existing_file(file: File) -> std::io::Result<Self> {
        // NOTE: We use `MAP_SHARED` to make sure that all writes will be backed
        //       by the underlying file.
        MappedFile::from_existing_file_ext(
            file,
            libc::MAP_SHARED,
            libc::PROT_READ | libc::PROT_WRITE,
        )
        .map(|inner| Self { inner })
    }

    /// Mapped buffer length in bytes
    pub fn length(&self) -> usize {
        self.inner.length
    }

    /// Copies chunk of bytes to the specified buffer
    ///
    /// # Safety
    /// The caller must take care that the buffer is not out of the mapped memory!
    pub unsafe fn read_exact_at(&self, offset: usize, buffer: &mut [u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.inner.ptr.cast::<u8>().add(offset),
                buffer.as_mut_ptr(),
                buffer.len(),
            );
        }
    }

    /// Copies buffer to the mapped memory
    ///
    /// # Safety
    /// The caller must take care that the buffer is not out of the mapped memory!
    pub unsafe fn write_all_at(&mut self, offset: usize, buffer: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr(),
                self.inner.ptr.cast::<u8>().add(offset),
                buffer.len(),
            );
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr and length were initialized once on creation
        unsafe { std::slice::from_raw_parts(self.inner.ptr.cast::<u8>(), self.inner.length) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr and length were initialized once on creation
        unsafe { std::slice::from_raw_parts_mut(self.inner.ptr.cast::<u8>(), self.inner.length) }
    }
}

impl AsRef<MappedFile> for MappedFileMut {
    fn as_ref(&self) -> &MappedFile {
        &self.inner
    }
}

impl AsRef<[u8]> for MappedFileMut {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

impl AsMut<[u8]> for MappedFileMut {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl std::ops::Deref for MappedFileMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl std::ops::DerefMut for MappedFileMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

pub const MIN_READ_CHUNK_SIZE: usize = 1024 * 128; // 128 Kb

/// Memory buffer that is mapped to a file
pub struct MappedFile {
    length: usize,
    ptr: *mut libc::c_void,
}

impl MappedFile {
    /// Opens an existing file and maps it to memory
    pub fn from_existing_file(file: File) -> std::io::Result<Self> {
        // NOTE: `MAP_PRIVATE` here is just in case. For `PROT_READ` is doen't really matter.
        Self::from_existing_file_ext(file, libc::MAP_PRIVATE, libc::PROT_READ)
    }

    fn from_existing_file_ext(
        file: File,
        flags: libc::c_int,
        prot: libc::c_int,
    ) -> std::io::Result<Self> {
        let length = file.metadata()?.len() as usize;

        // SAFETY: File was opened successfully, offset is aligned
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                prot,
                flags,
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

        Ok(Self { length, ptr })
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
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.ptr.cast::<u8>().add(offset),
                buffer.as_mut_ptr(),
                buffer.len(),
            );
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr and length were initialized once on creation
        unsafe { std::slice::from_raw_parts(self.ptr.cast::<u8>(), self.length) }
    }

    pub fn read_chunk(&self, offset: usize, chunk_size: usize) -> Option<Vec<u8>> {
        let chunk_size = std::cmp::max(MIN_READ_CHUNK_SIZE, chunk_size);

        if offset % chunk_size != 0 {
            return None;
        }
        if offset > self.length() {
            return None;
        }

        let end = std::cmp::min(offset.saturating_add(chunk_size), self.length());
        Some(self.as_slice()[offset..end].to_vec())
    }

    pub fn read_decompress_chunk(
        &self,
        offset: usize,
        chunk_size: usize,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        // read compressed bytes
        let Some(compressed_buffer) = self.read_chunk(offset, chunk_size) else {
            return Ok(None);
        };

        // create decompress stream
        let mut decompress_stream = ZstdDecompressStream::new(MIN_READ_CHUNK_SIZE)?;

        // decompress bytes
        let mut decompressed_buffer = vec![];
        decompress_stream.write(&compressed_buffer, &mut decompressed_buffer)?;

        Ok(Some(decompressed_buffer))
    }
}

impl AsRef<[u8]> for MappedFile {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::ops::Deref for MappedFile {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
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
