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
pub trait RpcDataProvider: Send + Sync + 'static {
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

pub struct StorageRpcDataProvider {
    storage: CoreStorage,
}

impl StorageRpcDataProvider {
    pub fn new(storage: CoreStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcDataProvider for StorageRpcDataProvider {
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
pub use s3_impl::S3RpcDataProvider;

#[cfg(feature = "s3")]
mod s3_impl {
    use std::num::NonZeroU32;

    use governor::clock::DefaultClock;
    use governor::state::{InMemoryState, NotKeyed};
    use governor::{Quota, RateLimiter};

    use super::*;
    use crate::blockchain_rpc::S3ProxyConfig;
    use crate::s3::S3Client;

    type S3RateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

    pub struct S3RpcDataProvider {
        client: S3Client,
        storage: CoreStorage,
        chunk_size: NonZeroU32,
        rate_limiter: S3RateLimiter,
        bandwidth_limiter: Option<S3RateLimiter>,
    }

    impl S3RpcDataProvider {
        pub fn new(client: S3Client, storage: CoreStorage, config: &S3ProxyConfig) -> Self {
            let chunk_size = client.chunk_size();
            let rate_limiter = RateLimiter::direct(Quota::per_second(config.rate_limit));

            let bandwidth_limiter =
                NonZeroU32::new(config.bandwidth_limit.as_u64() as u32).map(|bytes_per_sec| {
                    let burst = bytes_per_sec.get().max(chunk_size.get());
                    RateLimiter::direct(
                        Quota::per_second(bytes_per_sec)
                            .allow_burst(NonZeroU32::new(burst).unwrap()),
                    )
                });

            Self {
                client,
                storage,
                chunk_size,
                rate_limiter,
                bandwidth_limiter,
            }
        }
    }

    #[async_trait]
    impl RpcDataProvider for S3RpcDataProvider {
        async fn get_archive_info(&self, mc_seqno: u32) -> Result<ArchiveInfo> {
            self.check_rate_limit()?;

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
            let chunk_size = self.chunk_size.get() as u64;

            anyhow::ensure!(
                offset.is_multiple_of(chunk_size),
                "unaligned archive chunk offset"
            );

            self.check_rate_limit()?;
            self.check_bandwidth_limit()?;

            let path = self.client.make_archive_key(archive_id);
            let client = self.client.client();

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
            self.check_rate_limit()?;

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
            self.check_rate_limit()?;
            self.check_bandwidth_limit()?;

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

    impl S3RpcDataProvider {
        fn check_rate_limit(&self) -> Result<()> {
            self.rate_limiter
                .check()
                .map_err(|_err| anyhow::anyhow!("S3 rate limit exceeded"))
        }

        fn check_bandwidth_limit(&self) -> Result<()> {
            if let Some(limiter) = &self.bandwidth_limiter {
                limiter
                    .check_n(self.chunk_size)
                    .expect("shouldn't happen since burst = bytes_per_sec.max(chunk_size")
                    .map_err(|err| anyhow::anyhow!("S3 bandwidth limit exceeded {err:?}"))?;
            }
            Ok(())
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
impl<T1, T2> RpcDataProvider for HybridRpcProvider<T1, T2>
where
    T1: RpcDataProvider,
    T2: RpcDataProvider,
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

pub trait IntoRpcDataProvider {
    fn into_data_provider(self) -> Arc<dyn RpcDataProvider>;
}

impl<T: RpcDataProvider> IntoRpcDataProvider for (T,) {
    #[inline]
    fn into_data_provider(self) -> Arc<dyn RpcDataProvider> {
        Arc::new(self.0)
    }
}

impl<T1: RpcDataProvider, T2: RpcDataProvider> IntoRpcDataProvider for (T1, Option<T2>) {
    fn into_data_provider(self) -> Arc<dyn RpcDataProvider> {
        let (primary, fallback) = self;
        match fallback {
            None => Arc::new(primary),
            Some(fallback) => Arc::new(HybridRpcProvider::new(primary, fallback)),
        }
    }
}

impl IntoRpcDataProvider for CoreStorage {
    #[inline]
    fn into_data_provider(self) -> Arc<dyn RpcDataProvider> {
        Arc::new(StorageRpcDataProvider::new(self))
    }
}
