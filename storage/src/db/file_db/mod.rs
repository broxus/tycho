use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use everscale_types::models::*;

pub use mapped_file::MappedFile;

mod mapped_file;

pub struct FileDb {
    pub file: File,
    pub path: PathBuf,
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
            .context("Failed to create file")?;

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
}
