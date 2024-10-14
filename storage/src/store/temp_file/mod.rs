use std::fs::Metadata;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;

use crate::{FileBuilder, FileDb, UnnamedFileBuilder};

const BASE_DIR: &str = "temp";

#[derive(Clone)]
pub struct TempFileStorage {
    storage_dir: FileDb,
}

impl TempFileStorage {
    const MAX_FILE_TTL: Duration = Duration::from_secs(86400); // 1 day

    pub fn new(files_dir: &FileDb) -> Result<Self> {
        Ok(Self {
            storage_dir: files_dir.create_subdir(BASE_DIR)?,
        })
    }

    pub async fn remove_outdated_files(&self) -> Result<()> {
        let now = std::time::SystemTime::now();

        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            this.retain_files(|path, metadata| match metadata.modified() {
                Ok(modified) => {
                    let since_modified = now.duration_since(modified).unwrap_or(Duration::ZERO);
                    since_modified <= Self::MAX_FILE_TTL
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        "failed to check file metadata: {e:?}"
                    );
                    false
                }
            })
        })
        .await?
        .map_err(Into::into)
    }

    pub fn retain_files<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Path, &Metadata) -> bool,
    {
        let entries = self.storage_dir.entries()?;
        for e in entries {
            let e = e?;

            let path = e.path();
            let Ok(metadata) = std::fs::metadata(&path) else {
                tracing::warn!(
                    path = %path.display(),
                    "failed to check downloaded file metadata: {e:?}"
                );
                continue;
            };

            let is_file = metadata.is_file();
            let keep = is_file && f(&path, &metadata);
            tracing::debug!(keep, path = %path.display(), "found downloaded file");

            if keep {
                continue;
            }

            let e = if is_file {
                std::fs::remove_file(&path)
            } else {
                std::fs::remove_dir_all(&path)
            };
            if let Err(e) = e {
                tracing::warn!(path = %path.display(), "failed to remove downloads entry: {e:?}");
            }
        }

        Ok(())
    }

    pub fn file<P: AsRef<Path>>(&self, rel_path: P) -> FileBuilder {
        self.storage_dir.file(&rel_path)
    }

    pub fn unnamed_file(&self) -> UnnamedFileBuilder {
        self.storage_dir.unnamed_file()
    }
}
