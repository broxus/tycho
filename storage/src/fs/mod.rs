#![allow(clippy::disallowed_methods)]
#![allow(clippy::disallowed_types)] // it's wrapper around Files so

use std::borrow::Cow;
use std::ffi::OsStr;
use std::fs::{File, Metadata, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tycho_util::fs::MappedFileMut;

const BASE_DIR: &str = "temp";

#[derive(Clone)]
pub struct TempFileStorage {
    storage_dir: Dir,
}

impl TempFileStorage {
    const MAX_FILE_TTL: Duration = Duration::from_secs(86400); // 1 day

    pub fn new(files_dir: &Dir) -> Result<Self> {
        Ok(Self {
            storage_dir: files_dir.create_subdir(BASE_DIR)?,
        })
    }

    pub fn dir(&self) -> &Dir {
        &self.storage_dir
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

#[derive(Clone)]
pub struct Dir(Arc<DirInner>);

impl Dir {
    /// Creates a new `Dir` instance.
    /// If the `root` directory does not exist, it will be created.
    pub fn new<P>(root: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        std::fs::create_dir_all(root.as_ref())
            .with_context(|| format!("failed to create {}", root.as_ref().display()))?;
        Ok(Self(Arc::new(DirInner {
            base_dir: root.as_ref().to_path_buf(),
        })))
    }

    /// Creates a new `Dir` without creating the root directory tree
    pub fn new_readonly<P>(root: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self(Arc::new(DirInner {
            base_dir: root.as_ref().to_path_buf(),
        }))
    }

    pub fn path(&self) -> &Path {
        &self.0.base_dir
    }

    pub fn create_if_not_exists(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.0.base_dir)
    }

    pub fn create_dir_all<P: AsRef<Path>>(&self, rel_path: P) -> std::io::Result<()> {
        std::fs::create_dir_all(self.0.base_dir.join(rel_path))
    }

    pub fn remove_file<P: AsRef<Path>>(&self, rel_path: P) -> std::io::Result<()> {
        std::fs::remove_file(self.0.base_dir.join(rel_path))
    }

    pub fn file<P: AsRef<Path>>(&self, rel_path: P) -> FileBuilder {
        FileBuilder {
            path: self.0.base_dir.join(rel_path.as_ref()),
            options: std::fs::OpenOptions::new(),
            prealloc: None,
        }
    }

    pub fn unnamed_file(&self) -> UnnamedFileBuilder {
        UnnamedFileBuilder {
            base_dir: self.0.base_dir.clone(),
            prealloc: None,
        }
    }

    /// Creates `Dir` instance for a subdirectory of the current one.
    /// **Note**: The subdirectory will not be created if it does not exist.
    /// Use `create_subdir` to create it.
    pub fn subdir_readonly<P: AsRef<Path>>(&self, rel_path: P) -> Self {
        Self(Arc::new(DirInner {
            base_dir: self.0.base_dir.join(rel_path),
        }))
    }

    /// Creates `Dir` instance for a subdirectory of the current one.
    /// The subdirectory will be created if it does not exist.
    pub fn create_subdir<P: AsRef<Path>>(&self, rel_path: P) -> Result<Self> {
        Self::new(self.0.base_dir.join(rel_path))
    }

    pub fn file_exists<P: AsRef<Path>>(&self, rel_path: P) -> bool {
        self.path().join(rel_path).is_file()
    }

    pub fn entries(&self) -> std::io::Result<std::fs::ReadDir> {
        std::fs::read_dir(&self.0.base_dir)
    }
}

struct DirInner {
    base_dir: PathBuf,
}

#[derive(Clone)]
pub struct FileBuilder {
    path: PathBuf,
    options: OpenOptions,
    prealloc: Option<usize>,
}

impl FileBuilder {
    pub fn with_extension<S: AsRef<OsStr>>(&self, extension: S) -> Self {
        Self {
            path: self.path.with_extension(extension),
            options: self.options.clone(),
            prealloc: self.prealloc,
        }
    }

    pub fn open(&self) -> Result<File> {
        let file = self
            .options
            .open(&self.path)
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        if let Some(prealloc) = self.prealloc {
            alloc_file(&file, prealloc)?;
        }
        Ok(file)
    }

    pub fn rename<P: AsRef<Path>>(&self, new_path: P) -> std::io::Result<()> {
        let new_path = match self.path.parent() {
            Some(parent) => Cow::Owned(parent.join(new_path)),
            None => Cow::Borrowed(new_path.as_ref()),
        };
        std::fs::rename(&self.path, new_path)
    }

    pub fn exists(&self) -> bool {
        std::fs::metadata(&self.path)
            .ok()
            .map(|m| m.is_file())
            .unwrap_or_default()
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.options.append(append);
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.options.create(create);
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.options.create_new(create_new);
        self
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.options.read(read);
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.options.truncate(truncate);
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.options.write(write);
        self
    }

    pub fn prealloc(&mut self, prealloc: usize) -> &mut Self {
        self.prealloc = Some(prealloc);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct UnnamedFileBuilder {
    base_dir: PathBuf,
    prealloc: Option<usize>,
}

impl UnnamedFileBuilder {
    pub fn open(self) -> Result<File> {
        let file = tempfile::tempfile_in(&self.base_dir)?;
        if let Some(prealloc) = self.prealloc {
            file.set_len(prealloc as u64)?;
        }

        Ok(file)
    }

    pub fn open_as_mapped_mut(&self) -> Result<MappedFileMut> {
        let file = tempfile::tempfile_in(&self.base_dir).with_context(|| {
            format!("failed to create a tempfile in {}", self.base_dir.display())
        })?;

        if let Some(prealloc) = self.prealloc {
            #[cfg(target_os = "linux")]
            alloc_file(&file, prealloc)?;

            file.set_len(prealloc as u64)?;
        } else {
            anyhow::bail!("prealloc is required for mapping unnamed files");
        }

        MappedFileMut::from_existing_file(file).map_err(Into::into)
    }

    pub fn prealloc(&mut self, prealloc: usize) -> &mut Self {
        self.prealloc = Some(prealloc);
        self
    }
}

#[cfg(not(target_os = "macos"))]
fn alloc_file(file: &File, len: usize) -> std::io::Result<()> {
    let res = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len as i64) };
    if res == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
pub fn alloc_file(file: &File, len: usize) -> std::io::Result<()> {
    let res = unsafe { libc::ftruncate(file.as_raw_fd(), len as i64) };
    if res < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
