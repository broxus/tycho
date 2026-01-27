use std::num::NonZeroU64;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

use crate::proto::blockchain::ArchiveInfo;
use crate::storage::{ArchiveId, BlockStorage, CoreStorage};

/// Abstraction for archive providers (storage, S3, etc.)
#[async_trait]
pub trait ArchiveProvider: Send + Sync + 'static {
    /// Get archive info for the given masterchain seqno.
    ///
    /// Returns:
    /// - `ArchiveInfo::Found` if archive exists and is ready
    /// - `ArchiveInfo::TooNew` if the archive is not yet available
    /// - `ArchiveInfo::NotFound` if the archive doesn't exist
    async fn get_archive_info(&self, mc_seqno: u32) -> Result<ArchiveInfo>;

    /// Get a chunk of archive data at the given offset.
    async fn get_archive_chunk(&self, archive_id: u32, offset: u64) -> Result<Bytes>;
}

// === Storage Implementation ===

pub struct StorageArchiveProvider {
    storage: CoreStorage,
}

impl StorageArchiveProvider {
    pub fn new(storage: CoreStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ArchiveProvider for StorageArchiveProvider {
    async fn get_archive_info(&self, mc_seqno: u32) -> Result<ArchiveInfo> {
        let node_state = self.storage.node_state();

        match node_state.load_last_mc_block_id() {
            Some(last_applied_mc_block) => {
                if mc_seqno > last_applied_mc_block.seqno {
                    return Ok(ArchiveInfo::TooNew);
                }

                let block_storage = self.storage.block_storage();

                let id = block_storage.get_archive_id(mc_seqno);
                let size_res = match id {
                    ArchiveId::Found(id) => block_storage.get_archive_size(id),
                    ArchiveId::TooNew | ArchiveId::NotFound => Ok(None),
                };

                Ok(match (id, size_res) {
                    (ArchiveId::Found(id), Ok(Some(size))) if size > 0 => ArchiveInfo::Found {
                        id: id as u64,
                        size: NonZeroU64::new(size as _).unwrap(),
                        chunk_size: BlockStorage::DEFAULT_BLOB_CHUNK_SIZE,
                    },
                    (ArchiveId::Found(_) | ArchiveId::TooNew, Ok(None)) => ArchiveInfo::TooNew,
                    _ => ArchiveInfo::NotFound,
                })
            }
            None => {
                anyhow::bail!("get_archive_id failed: no blocks applied")
            }
        }
    }

    async fn get_archive_chunk(&self, archive_id: u32, offset: u64) -> Result<Bytes> {
        self.storage
            .block_storage()
            .get_archive_chunk(archive_id, offset)
            .await
    }
}

// === S3 Implementation ===

#[cfg(feature = "s3")]
pub use s3_impl::S3ArchiveProvider;

#[cfg(feature = "s3")]
mod s3_impl {
    use std::num::NonZeroU32;

    use super::*;
    use crate::s3::S3Client;

    pub struct S3ArchiveProvider {
        client: S3Client,
        storage: CoreStorage,
        chunk_size: NonZeroU32,
    }

    impl S3ArchiveProvider {
        pub fn new(client: S3Client, storage: CoreStorage) -> Self {
            let chunk_size = NonZeroU32::new(client.chunk_size() as u32).unwrap();
            Self {
                client,
                storage,
                chunk_size,
            }
        }
    }

    #[async_trait]
    impl ArchiveProvider for S3ArchiveProvider {
        async fn get_archive_info(&self, mc_seqno: u32) -> Result<ArchiveInfo> {
            let archive_id = self.storage.block_storage().estimate_archive_id(mc_seqno);

            match self.client.get_archive_info(archive_id).await {
                Ok(Some(info)) => Ok(ArchiveInfo::Found {
                    id: info.archive_id as u64,
                    size: info.size,
                    chunk_size: self.chunk_size,
                }),
                Ok(None) => Ok(ArchiveInfo::NotFound),
                Err(e) => {
                    tracing::warn!(archive_id, "failed to get archive info from S3: {e:?}");
                    Ok(ArchiveInfo::NotFound)
                }
            }
        }

        async fn get_archive_chunk(&self, archive_id: u32, offset: u64) -> Result<Bytes> {
            let path = self.client.make_archive_key(archive_id);
            let client = self.client.client();
            let chunk_size = self.chunk_size.get() as u64;

            let range = std::ops::Range {
                start: offset,
                end: offset + chunk_size,
            };

            client.get_range(&path, range).await.map_err(Into::into)
        }
    }
}

// === Hybrid Implementation ===

pub struct HybridArchiveProvider<T1, T2> {
    primary: T1,
    fallback: T2,
}

impl<T1, T2> HybridArchiveProvider<T1, T2> {
    pub fn new(primary: T1, fallback: T2) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl<T1, T2> ArchiveProvider for HybridArchiveProvider<T1, T2>
where
    T1: ArchiveProvider,
    T2: ArchiveProvider,
{
    async fn get_archive_info(&self, mc_seqno: u32) -> Result<ArchiveInfo> {
        // Try primary first
        match self.primary.get_archive_info(mc_seqno).await {
            Ok(info @ ArchiveInfo::Found { .. }) => return Ok(info),
            Ok(ArchiveInfo::TooNew) => {
                return Ok(ArchiveInfo::TooNew);
            }
            Ok(ArchiveInfo::NotFound) => {}
            Err(e) => {
                tracing::warn!(mc_seqno, "primary archive provider error: {e:?}");
            }
        }

        // Fallback
        self.fallback.get_archive_info(mc_seqno).await
    }

    async fn get_archive_chunk(&self, archive_id: u32, offset: u64) -> Result<Bytes> {
        // Try primary first
        match self.primary.get_archive_chunk(archive_id, offset).await {
            Ok(chunk) => return Ok(chunk),
            Err(e) => {
                tracing::warn!(archive_id, offset, "primary archive provider error: {e:?}");
            }
        }

        // Fallback
        self.fallback.get_archive_chunk(archive_id, offset).await
    }
}

// === IntoArchiveProvider trait ===

pub trait IntoArchiveProvider {
    fn into_archive_provider(self, proxy_to_s3: bool) -> Arc<dyn ArchiveProvider>;
}

impl<T: ArchiveProvider> IntoArchiveProvider for (T,) {
    #[inline]
    fn into_archive_provider(self, _proxy_to_s3: bool) -> Arc<dyn ArchiveProvider> {
        Arc::new(self.0)
    }
}

impl<T1: ArchiveProvider, T2: ArchiveProvider> IntoArchiveProvider for (T1, Option<T2>) {
    fn into_archive_provider(self, _proxy_to_s3: bool) -> Arc<dyn ArchiveProvider> {
        let (primary, fallback) = self;
        match fallback {
            None => Arc::new(primary),
            Some(fallback) => Arc::new(HybridArchiveProvider::new(primary, fallback)),
        }
    }
}

impl IntoArchiveProvider for CoreStorage {
    #[inline]
    fn into_archive_provider(self, _proxy_to_s3: bool) -> Arc<dyn ArchiveProvider> {
        Arc::new(StorageArchiveProvider::new(self))
    }
}
