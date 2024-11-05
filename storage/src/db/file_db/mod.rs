#![allow(clippy::disallowed_methods)]
#![allow(clippy::disallowed_types)] // it's wrapper around Files so

use std::borrow::Cow;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

pub use self::mapped_file::{MappedFile, MappedFileMut};

mod mapped_file;

#[derive(Clone)]
pub struct FileDb(Arc<FileDbInner>);

impl FileDb {
    /// Creates a new `FileDb` instance.
    /// If the `root` directory does not exist, it will be created.
    pub fn new<P>(root: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        std::fs::create_dir_all(root.as_ref())
            .with_context(|| format!("failed to create {}", root.as_ref().display()))?;
        Ok(Self(Arc::new(FileDbInner {
            base_dir: root.as_ref().to_path_buf(),
        })))
    }

    /// Creates a new `FileDb` without creating the root directory tree
    pub fn new_readonly<P>(root: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self(Arc::new(FileDbInner {
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

    /// Creates `FileDb` instance for a subdirectory of the current one.
    /// **Note**: The subdirectory will not be created if it does not exist.
    /// Use `create_subdir` to create it.
    pub fn subdir_readonly<P: AsRef<Path>>(&self, rel_path: P) -> Self {
        Self(Arc::new(FileDbInner {
            base_dir: self.0.base_dir.join(rel_path),
        }))
    }

    /// Creates `FileDb` instance for a subdirectory of the current one.
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

struct FileDbInner {
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
