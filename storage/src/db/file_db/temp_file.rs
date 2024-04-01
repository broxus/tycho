use std::fs::File;
use std::mem::ManuallyDrop;
use std::path::PathBuf;

pub struct TempFile {
    file: ManuallyDrop<File>,
    file_path: Option<PathBuf>,
}

impl TempFile {
    pub fn new(path: PathBuf, file: File) -> Self {
        Self {
            file: ManuallyDrop::new(file),
            file_path: Some(path),
        }
    }

    pub fn disarm(mut self) -> File {
        self.file_path = None;

        // SAFETY: File will not be dropped as `file_path` is `None`.
        unsafe { ManuallyDrop::take(&mut self.file) }
    }
}

impl AsRef<File> for TempFile {
    #[inline]
    fn as_ref(&self) -> &File {
        &self.file
    }
}

impl AsMut<File> for TempFile {
    #[inline]
    fn as_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl std::ops::Deref for TempFile {
    type Target = File;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl std::ops::DerefMut for TempFile {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Some(file_path) = self.file_path.take() {
            // SAFETY: File will only be dropped once.
            unsafe { ManuallyDrop::drop(&mut self.file) };

            if let Err(e) = std::fs::remove_file(&file_path) {
                tracing::error!(path = %file_path.display(), "failed to remove file: {e:?}");
            }
        }
    }
}
