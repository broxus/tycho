use std::num::NonZeroU64;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tycho_types::models::BlockId;

use crate::proto::blockchain::ArchiveInfo;
use crate::storage::{
    ArchiveId, BlockStorage, CoreStorage, PersistentStateInfo, PersistentStateKind,
};

/// Abstraction for rpc data providers (storage, S3, etc.)
#[async_trait]
pub trait RpcProvider: Send + Sync + 'static {
    /// Get archive info for the given masterchain seqno.
    ///
    /// Returns:
    /// - `ArchiveInfo::Found` if archive exists and is ready
    /// - `ArchiveInfo::TooNew` if the archive is not yet available
    /// - `ArchiveInfo::NotFound` if the archive doesn't exist
    async fn get_archive_info(&self, mc_seqno: u32) -> Result<ArchiveInfo>;

    /// Get a chunk of archive data at the given offset.
    async fn get_archive_chunk(&self, archive_id: u32, offset: u64) -> Result<Bytes>;

    /// Get persistent state info for the given block.
    async fn get_persistent_state_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Result<Option<PersistentStateInfo>>;

    /// Get a chunk of persistent state data at the given offset.
    async fn get_persistent_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        kind: PersistentStateKind,
    ) -> Result<Option<Bytes>>;
}

// === Storage Implementation ===

pub struct StorageRpcProvider {
    storage: CoreStorage,
}

impl StorageRpcProvider {
    pub fn new(storage: CoreStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcProvider for StorageRpcProvider {
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

    async fn get_persistent_state_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Result<Option<PersistentStateInfo>> {
        let persistent_state_storage = self.storage.persistent_state_storage();
        Ok(persistent_state_storage.get_state_info(block_id, kind))
    }

    async fn get_persistent_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        kind: PersistentStateKind,
    ) -> Result<Option<Bytes>> {
        let persistent_state_storage = self.storage.persistent_state_storage();
        Ok(persistent_state_storage
            .read_state_part(block_id, offset, kind)
            .await
            .map(Bytes::from))
    }
}

// === S3 Implementation ===

#[cfg(feature = "s3")]
pub use s3_impl::S3RpcProvider;

#[cfg(feature = "s3")]
mod s3_impl {
    use std::num::NonZeroU32;

    use super::*;
    use crate::s3::S3Client;

    pub struct S3RpcProvider {
        client: S3Client,
        storage: CoreStorage,
        chunk_size: NonZeroU32,
    }

    impl S3RpcProvider {
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
    impl RpcProvider for S3RpcProvider {
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

        async fn get_persistent_state_info(
            &self,
            block_id: &BlockId,
            kind: PersistentStateKind,
        ) -> Result<Option<PersistentStateInfo>> {
            Ok(self
                .client
                .get_persistent_state_info(block_id, kind)
                .await?
                .map(|info| PersistentStateInfo {
                    size: info.size,
                    chunk_size: self.chunk_size,
                }))
        }

        async fn get_persistent_state_chunk(
            &self,
            block_id: &BlockId,
            offset: u64,
            kind: PersistentStateKind,
        ) -> Result<Option<Bytes>> {
            let chunk_size = self.chunk_size.get() as u64;

            if !offset.is_multiple_of(chunk_size) {
                return Ok(None);
            }

            let path = self.client.make_state_key(block_id, kind);
            let client = self.client.client();

            let range = std::ops::Range {
                start: offset,
                end: offset + chunk_size,
            };

            match client.get_range(&path, range).await {
                Ok(data) => Ok(Some(data)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
    }
}

// === Hybrid Implementation ===

pub struct HybridRpcProvider<T1, T2> {
    primary: T1,
    fallback: T2,
}

impl<T1, T2> HybridRpcProvider<T1, T2> {
    pub fn new(primary: T1, fallback: T2) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl<T1, T2> RpcProvider for HybridRpcProvider<T1, T2>
where
    T1: RpcProvider,
    T2: RpcProvider,
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

    async fn get_persistent_state_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Result<Option<PersistentStateInfo>> {
        // Try primary first
        match self.primary.get_persistent_state_info(block_id, kind).await {
            Ok(Some(info)) => return Ok(Some(info)),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(?block_id, ?kind, "primary state provider error: {e:?}");
            }
        }

        // Fallback
        self.fallback
            .get_persistent_state_info(block_id, kind)
            .await
    }

    async fn get_persistent_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        kind: PersistentStateKind,
    ) -> Result<Option<Bytes>> {
        // Try primary first
        match self
            .primary
            .get_persistent_state_chunk(block_id, offset, kind)
            .await
        {
            Ok(Some(chunk)) => return Ok(Some(chunk)),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    ?block_id,
                    offset,
                    ?kind,
                    "primary state provider error: {e:?}"
                );
            }
        }

        // Fallback
        self.fallback
            .get_persistent_state_chunk(block_id, offset, kind)
            .await
    }
}

// === IntoRpcProvider trait ===

pub trait IntoRpcProvider {
    fn into_rpc_provider(self, proxy_to_s3: bool) -> Arc<dyn RpcProvider>;
}

impl<T: RpcProvider> IntoRpcProvider for (T,) {
    #[inline]
    fn into_rpc_provider(self, _proxy_to_s3: bool) -> Arc<dyn RpcProvider> {
        Arc::new(self.0)
    }
}

impl<T1: RpcProvider, T2: RpcProvider> IntoRpcProvider for (T1, Option<T2>) {
    fn into_rpc_provider(self, proxy_to_s3: bool) -> Arc<dyn RpcProvider> {
        let (primary, fallback) = self;
        match fallback.filter(|_| proxy_to_s3) {
            None => Arc::new(primary),
            Some(fallback) => Arc::new(HybridRpcProvider::new(primary, fallback)),
        }
    }
}

impl IntoRpcProvider for CoreStorage {
    #[inline]
    fn into_rpc_provider(self, _proxy_to_s3: bool) -> Arc<dyn RpcProvider> {
        Arc::new(StorageRpcProvider::new(self))
    }
}
