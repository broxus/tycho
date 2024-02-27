use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use everscale_types::models::*;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

pub use mapped_file::MappedFile;

mod mapped_file;

pub struct FileDb {
    cells_path: PathBuf,
    cells_file: Option<BufWriter<File>>,
    hashes_path: PathBuf,
}

impl FileDb {
    pub async fn new<P>(downloads_dir: P, block_id: &BlockId) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let block_id = format!(
            "({},{:016x},{})",
            block_id.shard.workchain(),
            block_id.shard.prefix(),
            block_id.seqno
        );

        let cells_path = downloads_dir
            .as_ref()
            .join(format!("state_cells_{block_id}"));
        let hashes_path = downloads_dir
            .as_ref()
            .join(format!("state_hashes_{block_id}"));

        let cells_file = Some(BufWriter::new(
            tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .read(true)
                .open(&cells_path)
                .await
                .context("Failed to create cells file")?,
        ));

        Ok(Self {
            cells_path,
            cells_file,
            hashes_path,
        })
    }

    pub async fn clear(self) -> Result<()> {
        tokio::fs::remove_file(self.cells_path).await?;
        tokio::fs::remove_file(self.hashes_path).await?;
        Ok(())
    }

    pub fn cells_file(&mut self) -> Result<&mut BufWriter<File>> {
        match &mut self.cells_file {
            Some(file) => Ok(file),
            None => Err(FileDbError::AlreadyFinalized.into()),
        }
    }

    pub fn create_mapped_hashes_file(&self, length: usize) -> Result<MappedFile> {
        let mapped_file = MappedFile::new(&self.hashes_path, length)?;
        Ok(mapped_file)
    }

    pub async fn create_mapped_cells_file(&mut self) -> Result<MappedFile> {
        let file = match self.cells_file.take() {
            Some(mut file) => {
                file.flush().await?;
                file.into_inner().into_std().await
            }
            None => return Err(FileDbError::AlreadyFinalized.into()),
        };

        let mapped_file = MappedFile::from_existing_file(file)?;
        Ok(mapped_file)
    }
}

#[derive(thiserror::Error, Debug)]
enum FileDbError {
    #[error("Already finalized")]
    AlreadyFinalized,
}
