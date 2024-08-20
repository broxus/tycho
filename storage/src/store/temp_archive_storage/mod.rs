use std::path::PathBuf;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use tycho_util::io::BytesWriter;

use crate::FileDb;

const BASE_DIR: &str = "temp_archives";

pub struct TempArchiveStorage {
    storage_dir: FileDb,
}

impl TempArchiveStorage {
    pub fn new(files_dir: &FileDb) -> Result<Self> {
        let storage_dir = files_dir.create_subdir(BASE_DIR)?;

        // remove possible garbage
        tracing::debug!("removing temp archives on startup");
        let entries = storage_dir.entries()?;
        for e in entries {
            let e = e?;
            std::fs::remove_file(e.path())?;
        }

        Ok(Self { storage_dir })
    }

    pub fn create_archive_file(&self, id: u64) -> Result<tokio::fs::File> {
        let path = PathBuf::from(id.to_string());
        if let Err(e) = self.storage_dir.remove_file(&path) {
            tracing::warn!(id = id, "Failed to remove file {e:?}");
        }

        let file = self
            .storage_dir
            .file(&path)
            .create(true)
            .truncate(true)
            .write(true)
            .open()?;

        tracing::debug!(id = id, "temp archive created");
        Ok(tokio::fs::File::from(file))
    }

    pub async fn read_archive_to_bytes(&self, id: u64) -> Result<Bytes> {
        let file = self
            .storage_dir
            .file(PathBuf::from(id.to_string()))
            .read(true)
            .open()?;

        let file = tokio::fs::File::from(file);

        let mut writer = BytesWriter {
            writer: BytesMut::new().writer(),
        };

        tokio::io::copy(&mut tokio::io::BufReader::new(file), &mut writer).await?;
        Ok(writer.writer.into_inner().freeze())
    }

    pub fn remove_archive(&self, id: u64) -> Result<()> {
        self.storage_dir
            .remove_file(PathBuf::from(id.to_string()))?;
        tracing::debug!(id = id, "temp archive removed");
        Ok(())
    }
}
