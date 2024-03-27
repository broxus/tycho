use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub use mapped_file::MappedFile;

mod mapped_file;

pub struct FileDb {
    file: File,
    _path: PathBuf,
}

impl FileDb {
    pub fn new<P>(path: P, options: &mut std::fs::OpenOptions) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = options.open(&path)?;

        Ok(Self {
            file,
            _path: PathBuf::from(path.as_ref()),
        })
    }

    pub fn file(&self) -> &File {
        &self.file
    }
}

impl Write for FileDb {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Read for FileDb {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.file.read(buf)?;
        Ok(bytes)
    }
}

impl Seek for FileDb {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl From<FileDb> for File {
    fn from(val: FileDb) -> Self {
        val.file
    }
}
