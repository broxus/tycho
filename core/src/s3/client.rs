use std::io::Write;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::Bytes;
use bytesize::ByteSize;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{DynObjectStore, Error, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::ArchiveVerifier;
use tycho_types::models::BlockId;

use crate::storage::{
    PersistentStateKind, PersistentStateMeta, PersistentStatePartInfo, ShardStateWriter,
    validate_persistent_state_split_metadata,
};
use crate::util::downloader::{DownloaderError, DownloaderResponseHandle, download_and_decompress};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ClientConfig {
    /// Endpoint region.
    pub region: String,

    /// Endpoint to be used. For instance, `"https://s3.my-provider.net"` or just
    /// `"s3.my-provider.net"` (default scheme is https).
    pub endpoint: String,

    /// The bucket name.
    pub bucket: String,

    /// Archive prefix before its id (Default: empty)
    #[serde(default)]
    pub archive_key_prefix: String,

    /// State prefix before its id (Default: empty)
    #[serde(default)]
    pub state_key_prefix: String,

    /// AWS API access credentials
    #[serde(default)]
    pub credentials: Option<S3Credentials>,

    /// Maximum downloaded chunk size.
    ///
    /// Default: 10 MB.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: ByteSize,

    /// Number of retries to download archives/blocks/states.
    ///
    /// Default: 10.
    #[serde(default = "default_download_retries")]
    pub download_retries: usize,
}

fn default_chunk_size() -> ByteSize {
    ByteSize::mib(10)
}

fn default_download_retries() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Credentials {
    /// Access key id
    pub access_key: String,
    /// Secret access key
    pub secret_key: String,
    /// Session token
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Clone)]
#[repr(transparent)]
pub struct S3Client {
    inner: Arc<Inner>,
}

impl S3Client {
    pub fn new(config: &S3ClientConfig) -> anyhow::Result<Self> {
        let chunk_size = config.chunk_size.as_u64();
        anyhow::ensure!(chunk_size >= 1024, "chunk size must be at least 1 KiB");
        anyhow::ensure!(
            u32::try_from(chunk_size).is_ok(),
            "chunk size must be at most 4 GiB"
        );

        let client: Arc<DynObjectStore> = {
            let mut b = object_store::aws::AmazonS3Builder::new()
                .with_region(&config.region)
                .with_endpoint(&config.endpoint)
                .with_bucket_name(&config.bucket)
                .with_client_options(object_store::ClientOptions::new().with_allow_http(true));

            if let Some(credentials) = &config.credentials {
                b = b
                    .with_access_key_id(&credentials.access_key)
                    .with_secret_access_key(&credentials.secret_key);

                if let Some(token) = &credentials.token {
                    b = b.with_token(token);
                }
            }

            b.build().map(Arc::new)?
        };

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                archive_key_prefix: config.archive_key_prefix.clone(),
                state_key_prefix: config.state_key_prefix.clone(),
                chunk_size: NonZeroU32::new(chunk_size as u32).unwrap(),
                download_retries: config.download_retries,
            }),
        })
    }

    pub fn client(&self) -> &Arc<DynObjectStore> {
        &self.inner.client
    }

    pub fn chunk_size(&self) -> NonZeroU32 {
        self.inner.chunk_size
    }

    pub fn make_archive_key(&self, archive_id: u32) -> Path {
        self.inner.make_archive_key(archive_id)
    }

    pub fn make_state_key(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
        part_prefix: Option<u64>,
    ) -> anyhow::Result<Path> {
        self.inner.make_state_key(block_id, kind, part_prefix)
    }

    pub fn make_state_meta_key(&self, block_id: &BlockId) -> Path {
        self.inner.make_state_meta_key(block_id)
    }

    pub async fn get_archive_info(
        &self,
        archive_id: u32,
    ) -> Result<Option<BriefArchiveInfo>, Error> {
        let path = self.inner.make_archive_key(archive_id);
        let meta = match self.inner.client.head(&path).await {
            Ok(meta) if meta.size > 0 => meta,
            Ok(_) | Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e),
        };

        Ok(Some(BriefArchiveInfo {
            archive_id,
            size: NonZeroU64::new(meta.size).unwrap(),
        }))
    }

    #[tracing::instrument(skip_all, fields(archive_id = archive_id))]
    pub async fn download_archive<W>(&self, archive_id: u32, output: W) -> anyhow::Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        use futures_util::FutureExt;

        tracing::debug!("started");
        scopeguard::defer! {
            tracing::debug!("finished");
        }

        let chunk_size = self.inner.chunk_size;
        let max_retries = self.inner.download_retries;

        let client = &self.inner.client.clone();

        let path = self.inner.make_archive_key(archive_id);
        let meta = client.head(&path).await?;
        let Some(target_size) = NonZeroU64::new(meta.size) else {
            return Err(empty_file_error(path));
        };

        download_and_decompress(
            target_size,
            chunk_size,
            PARALLEL_REQUESTS,
            (output, ArchiveVerifier::default()),
            |offset| {
                let started_at = Instant::now();

                tracing::debug!(archive_id, offset, "downloading archive chunk");
                download_with_retries(
                    path.clone(),
                    offset,
                    chunk_size,
                    client.clone(),
                    max_retries,
                    "archive chunk",
                )
                .map(move |res| {
                    tracing::info!(
                        archive_id,
                        offset,
                        elapsed = %humantime::format_duration(started_at.elapsed()),
                        "downloaded archive chunk",
                    );
                    res
                })
            },
            |(output, verifier), chunk| {
                verifier.write_verify(chunk)?;
                output.write_all(chunk)?;
                Ok(())
            },
            |(mut output, verifier)| {
                verifier.final_check()?;
                output.flush()?;
                Ok(output)
            },
        )
        .await
        .map_err(map_downloader_error)
    }

    pub async fn get_persistent_state_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Result<Option<BriefPersistentStateInfo>, Error> {
        // read and validate the optional shard manifest before looking up main object
        let meta_path = self.inner.make_state_meta_key(block_id);
        let meta = match kind {
            PersistentStateKind::Queue => None,
            PersistentStateKind::Shard => match self.inner.client.get(&meta_path).await {
                Ok(result) => {
                    let meta = PersistentStateMeta::from_bytes(&result.bytes().await?)
                        .map_err(persistent_state_error)?
                        .expect("S3 manifest bytes are always present");
                    validate_persistent_state_split_metadata(
                        block_id.shard,
                        meta.split_depth,
                        meta.parts.iter().copied(),
                    )
                    .map_err(persistent_state_error)?;
                    Some(meta)
                }
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => return Err(e),
            },
        };

        // read main object after manifest: is valid or not exists
        let main_path = self
            .inner
            .make_state_key(block_id, kind, None)
            .map_err(persistent_state_error)?;
        let main_size = match self.inner.client.head(&main_path).await {
            Ok(meta) => NonZeroU64::new(meta.size),
            Err(Error::NotFound { .. }) => None,
            Err(e) => return Err(e),
        };
        let Some(main_size) = main_size else {
            return Ok(None);
        };

        // return fast if no manifest or kind is queue
        let Some(meta) = meta else {
            return Ok(Some(BriefPersistentStateInfo {
                block_id: *block_id,
                kind,
                size: main_size,
                split_depth: 0,
                parts: Vec::new(),
            }));
        };

        // if a valid manifest exists then read parts info
        if let Some(parts) = self
            .get_persistent_state_parts_info(block_id, kind, &meta.parts)
            .await?
        {
            return Ok(Some(BriefPersistentStateInfo {
                block_id: *block_id,
                kind,
                size: main_size,
                split_depth: meta.split_depth,
                parts,
            }));
        };

        Ok(None)
    }

    async fn get_persistent_state_parts_info(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
        prefixes: &[u64],
    ) -> Result<Option<Vec<PersistentStatePartInfo>>, Error> {
        let client = self.inner.client.clone();
        futures_util::stream::iter(prefixes.iter().copied())
            .map(|prefix| {
                let path = self
                    .inner
                    .make_state_key(block_id, kind, Some(prefix))
                    .map_err(persistent_state_error);
                let client = client.clone();
                async move {
                    let path = path?;
                    match client.head(&path).await {
                        Ok(meta) => Ok(NonZeroU64::new(meta.size)
                            .map(|size| PersistentStatePartInfo { prefix, size })),
                        Err(Error::NotFound { .. }) => Ok(None),
                        Err(e) => Err(e),
                    }
                }
            })
            .buffered(PARALLEL_REQUESTS.get())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
    }

    #[tracing::instrument(skip_all, fields(
        block_id = %info.block_id,
        kind = ?info.kind,
    ))]
    pub async fn download_persistent_state<W>(
        &self,
        info: BriefPersistentStateInfo,
        part_prefix: Option<u64>,
        output: W,
    ) -> Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        use futures_util::FutureExt;

        tracing::debug!("started");
        scopeguard::defer! {
            tracing::debug!("finished");
        }

        let chunk_size = self.inner.chunk_size;
        let max_retries = self.inner.download_retries;

        let client = &self.inner.client.clone();

        let target_size = match part_prefix {
            Some(prefix) => info
                .parts
                .iter()
                .find(|part| part.prefix == prefix)
                .map(|part| part.size)
                .ok_or_else(|| {
                    persistent_state_error(anyhow::anyhow!(
                        "persistent state part not found: {prefix:016x}"
                    ))
                })?,
            None => info.size,
        };
        let path = self
            .inner
            .make_state_key(&info.block_id, info.kind, part_prefix)
            .map_err(persistent_state_error)?;

        download_and_decompress(
            target_size,
            chunk_size,
            PARALLEL_REQUESTS,
            output,
            |offset| {
                let started_at = Instant::now();

                tracing::debug!(path = %path, offset, "downloading state chunk");
                download_with_retries(
                    path.clone(),
                    offset,
                    chunk_size,
                    client.clone(),
                    max_retries,
                    "state chunk",
                )
                .map({
                    let path = path.clone();
                    move |res| {
                        tracing::info!(
                            path = %path,
                            offset,
                            elapsed = %humantime::format_duration(started_at.elapsed()),
                            "downloaded state chunk",
                        );
                        res
                    }
                })
            },
            |output, chunk| {
                output.write_all(chunk)?;
                Ok(())
            },
            |mut output| {
                output.flush()?;
                Ok(output)
            },
        )
        .await
        .map_err(map_downloader_error)
    }
}

#[derive(Clone)]
pub struct BriefArchiveInfo {
    pub archive_id: u32,
    pub size: NonZeroU64,
}

#[derive(Clone)]
pub struct BriefPersistentStateInfo {
    pub block_id: BlockId,
    pub kind: PersistentStateKind,
    pub size: NonZeroU64,
    /// Will be non-zero for persistent shard state with parts
    pub split_depth: u8,
    pub parts: Vec<PersistentStatePartInfo>,
}

struct Inner {
    client: Arc<DynObjectStore>,
    archive_key_prefix: String,
    state_key_prefix: String,
    chunk_size: NonZeroU32,
    download_retries: usize,
}

impl Inner {
    fn make_archive_key(&self, archive_id: u32) -> Path {
        Path::from(format!("{}{archive_id}", self.archive_key_prefix))
    }

    fn make_state_key(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
        part_prefix: Option<u64>,
    ) -> anyhow::Result<Path> {
        let file_name = match part_prefix {
            Some(prefix) => kind
                .make_part_file_name(block_id, prefix)
                .context("persistent state parts are not supported for queue state")?,
            None => kind.make_file_name(block_id),
        };
        Ok(Path::from(format!(
            "{}{}",
            self.state_key_prefix,
            file_name.display()
        )))
    }

    fn make_state_meta_key(&self, block_id: &BlockId) -> Path {
        Path::from(format!(
            "{}{}",
            self.state_key_prefix,
            ShardStateWriter::meta_file_name(block_id).display()
        ))
    }
}

async fn download_with_retries(
    path: Path,
    offset: u64,
    length: NonZeroU32,
    client: Arc<DynObjectStore>,
    max_retries: usize,
    name: &'static str,
) -> object_store::Result<(DownloaderHandle, Bytes)> {
    let mut retries = 0;
    loop {
        let range = std::ops::Range {
            start: offset,
            end: offset + length.get() as u64,
        };

        match client.get_range(&path, range).await {
            Ok(bytes) => {
                return Ok((DownloaderHandle, bytes));
            }
            Err(e) => {
                tracing::error!("failed to download {name}: {e:?}");
                retries += 1;
                if retries >= max_retries {
                    return Err(e);
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

fn map_downloader_error(error: DownloaderError<Error>) -> Error {
    match error {
        DownloaderError::DownloadFailed(e) => e,
        e => Error::Generic {
            store: "downloader",
            source: e.into(),
        },
    }
}

fn empty_file_error(path: impl Into<String>) -> Error {
    Error::Precondition {
        path: path.into(),
        source: Box::new(std::io::Error::other("empty file")),
    }
}

fn persistent_state_error(error: anyhow::Error) -> Error {
    Error::Generic {
        store: "s3 client",
        source: error.into(),
    }
}

struct DownloaderHandle;

impl DownloaderResponseHandle for DownloaderHandle {
    fn accept(self) {}
    fn reject(self) {}
}

// TODO: Move into config
const PARALLEL_REQUESTS: NonZeroUsize = NonZeroUsize::new(10).unwrap();

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use tycho_types::cell::HashBytes;
    use tycho_types::models::ShardIdent;

    use super::*;

    impl S3Client {
        pub(crate) fn new_for_tests(client: Arc<DynObjectStore>) -> Self {
            Self {
                inner: Arc::new(Inner {
                    client,
                    archive_key_prefix: String::new(),
                    state_key_prefix: String::new(),
                    chunk_size: NonZeroU32::new(1024).unwrap(),
                    download_retries: 1,
                }),
            }
        }
    }

    async fn make_client() -> (S3Client, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        (S3Client::new_for_tests(store.clone()), store)
    }

    fn block_id(shard: ShardIdent) -> BlockId {
        BlockId {
            shard,
            seqno: 42,
            root_hash: HashBytes::from([1; 32]),
            file_hash: HashBytes::from([2; 32]),
        }
    }

    async fn put(store: &InMemory, path: Path, bytes: &[u8]) {
        store
            .put(&path, Bytes::copy_from_slice(bytes).into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn persistent_state_discovery_handles_legacy_and_split_bundles() {
        let (client, store) = make_client().await;

        // discover a legacy shard state before introducing split metadata
        let legacy_id = block_id(ShardIdent::BASECHAIN);
        put(
            &store,
            client
                .make_state_key(&legacy_id, PersistentStateKind::Shard, None)
                .unwrap(),
            b"legacy",
        )
        .await;

        let legacy = client
            .get_persistent_state_info(&legacy_id, PersistentStateKind::Shard)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.size.get(), 6);
        assert_eq!(legacy.split_depth, 0);
        assert!(legacy.parts.is_empty());

        // keep archive discovery independent from persistent-state representation
        put(&store, client.make_archive_key(42), b"archive").await;
        assert_eq!(
            client
                .get_archive_info(42)
                .await
                .unwrap()
                .unwrap()
                .size
                .get(),
            7
        );

        // add a split bundle whose object insertion order differs from manifest order
        let split_id = BlockId {
            seqno: 43,
            ..legacy_id
        };
        let prefixes = vec![0xa000000000000000, 0x2000000000000000];
        let meta = PersistentStateMeta::new(2, prefixes.clone());
        let meta_bytes = meta.to_bytes().unwrap();
        put(&store, client.make_state_meta_key(&split_id), &meta_bytes).await;
        put(
            &store,
            client
                .make_state_key(&split_id, PersistentStateKind::Shard, None)
                .unwrap(),
            b"main",
        )
        .await;
        put(
            &store,
            client
                .make_state_key(
                    &split_id,
                    PersistentStateKind::Shard,
                    Some(0x2000000000000000),
                )
                .unwrap(),
            b"first",
        )
        .await;
        put(
            &store,
            client
                .make_state_key(
                    &split_id,
                    PersistentStateKind::Shard,
                    Some(0xa000000000000000),
                )
                .unwrap(),
            b"second",
        )
        .await;

        // discover the complete split bundle in canonical manifest order
        let split = client
            .get_persistent_state_info(&split_id, PersistentStateKind::Shard)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(split.size.get(), 4);
        assert_eq!(split.split_depth, 2);
        assert_eq!(
            split
                .parts
                .iter()
                .map(|part| (part.prefix, part.size.get()))
                .collect::<Vec<_>>(),
            vec![(0x2000000000000000, 5), (0xa000000000000000, 6)]
        );
    }

    #[tokio::test]
    async fn persistent_state_manifest_rejects_incomplete_and_invalid_bundles() {
        let (client, store) = make_client().await;
        let test_block_id = block_id(ShardIdent::BASECHAIN);

        // a manifest without main and declared parts is not a usable bundle
        let meta = PersistentStateMeta::new(2, vec![0x2000000000000000]);
        let meta_bytes = meta.to_bytes().unwrap();
        put(
            &store,
            client.make_state_meta_key(&test_block_id),
            &meta_bytes,
        )
        .await;
        assert!(
            client
                .get_persistent_state_info(&test_block_id, PersistentStateKind::Shard)
                .await
                .unwrap()
                .is_none()
        );

        // adding main alone leaves the split bundle incomplete
        put(
            &store,
            client
                .make_state_key(&test_block_id, PersistentStateKind::Shard, None)
                .unwrap(),
            b"main",
        )
        .await;
        assert!(
            client
                .get_persistent_state_info(&test_block_id, PersistentStateKind::Shard)
                .await
                .unwrap()
                .is_none()
        );

        // reject duplicate prefixes even when the main object is absent
        let invalid_id = BlockId {
            seqno: 44,
            ..test_block_id
        };
        put(
            &store,
            client.make_state_meta_key(&invalid_id),
            br#"{"version":1,"split_depth":2,"parts":["2000000000000000","2000000000000000"]}"#,
        )
        .await;
        assert!(
            client
                .get_persistent_state_info(&invalid_id, PersistentStateKind::Shard)
                .await
                .is_err()
        );

        // reject malformed and semantically invalid manifest variants
        for (seqno, bytes) in [
            (45, &b"not json"[..]),
            (
                46,
                br#"{"version":2,"split_depth":2,"parts":["2000000000000000"]}"#.as_slice(),
            ),
            (
                47,
                br#"{"version":1,"split_depth":7,"parts":["2000000000000000"]}"#.as_slice(),
            ),
            (
                48,
                br#"{"version":1,"split_depth":2,"parts":["0000000000000000"]}"#.as_slice(),
            ),
        ] {
            let invalid_id = BlockId {
                seqno,
                ..test_block_id
            };
            put(&store, client.make_state_meta_key(&invalid_id), bytes).await;
            assert!(
                client
                    .get_persistent_state_info(&invalid_id, PersistentStateKind::Shard)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn persistent_state_download_selects_main_or_declared_part() {
        let (client, store) = make_client().await;

        // store the compressed main object and its declared split part
        let block_id = block_id(ShardIdent::BASECHAIN);
        let main = tycho_util::compression::zstd_compress_simple(b"main");
        let part = tycho_util::compression::zstd_compress_simple(b"part");
        let info = BriefPersistentStateInfo {
            block_id,
            kind: PersistentStateKind::Shard,
            size: NonZeroU64::new(main.len() as u64).unwrap(),
            split_depth: 2,
            parts: vec![PersistentStatePartInfo {
                prefix: 0x2000000000000000,
                size: NonZeroU64::new(part.len() as u64).unwrap(),
            }],
        };
        put(
            &store,
            client
                .make_state_key(&block_id, PersistentStateKind::Shard, None)
                .unwrap(),
            &main,
        )
        .await;
        put(
            &store,
            client
                .make_state_key(
                    &block_id,
                    PersistentStateKind::Shard,
                    Some(0x2000000000000000),
                )
                .unwrap(),
            &part,
        )
        .await;

        // select the main object when no part prefix is requested
        assert_eq!(
            client
                .download_persistent_state(info.clone(), None, Vec::new())
                .await
                .unwrap(),
            b"main"
        );

        // select a declared part and reject prefixes outside the manifest
        assert_eq!(
            client
                .download_persistent_state(info.clone(), Some(0x2000000000000000), Vec::new())
                .await
                .unwrap(),
            b"part"
        );
        assert!(
            client
                .download_persistent_state(info, Some(0xa000000000000000), Vec::new())
                .await
                .is_err()
        );
    }
}
