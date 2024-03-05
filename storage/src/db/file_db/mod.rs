use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use everscale_types::models::*;

pub use mapped_file::MappedFile;

mod mapped_file;

pub struct FileDb {
    file: File,
    path: PathBuf,
}

impl FileDb {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(&path)
            .context(format!("Failed to create file {:?}", path.as_ref()))?;

        Ok(Self {
            file,
            path: PathBuf::from(path.as_ref()),
        })
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write(buf)?;
        Ok(())
    }

    pub fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write_all(buf)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    pub fn seek(&mut self, pos: SeekFrom) -> Result<()> {
        self.file.seek(pos)?;
        Ok(())
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes = self.file.read(buf)?;
        Ok(bytes)
    }
}

impl Into<File> for FileDb {
    fn into(self) -> File {
        self.file
    }
}
