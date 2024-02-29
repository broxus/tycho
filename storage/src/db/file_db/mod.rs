use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use everscale_types::models::*;

pub use mapped_file::MappedFile;

mod mapped_file;

pub struct FileDb {
    root_path: PathBuf,
}

impl FileDb {
    pub fn new(root_path: PathBuf) -> Self {
        Self { root_path }
    }

    pub fn open<P>(&self, path: P, is_relative_path: bool) -> Result<File>
    where
        P: AsRef<Path>,
    {
        let path = if is_relative_path {
            self.root_path.join(path)
        } else {
            PathBuf::from(path.as_ref())
        };

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(&path)
            .context("Failed to create cells file")?;

        Ok(file)
    }

    pub fn clear<P>(&self, path: P, is_relative_path: bool) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let path = if is_relative_path {
            self.root_path.join(path)
        } else {
            PathBuf::from(path.as_ref())
        };

        std::fs::remove_file(path)?;

        Ok(())
    }

    pub fn write_all(file: &mut File, buf: &[u8]) -> Result<()> {
        file.write_all(buf)?;
        Ok(())
    }

    pub fn flush(file: &mut File) -> Result<()> {
        file.flush()?;
        Ok(())
    }

    pub fn root_path(&self) -> &PathBuf {
        &self.root_path
    }
}
