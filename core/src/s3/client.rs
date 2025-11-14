use std::io::Write;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{DynObjectStore, Error, ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::{Archive, ArchiveVerifier};
use tycho_types::models::BlockId;

use crate::storage::PersistentStateKind;
use crate::util::downloader::{DownloaderError, DownloaderResponseHandle, download_and_decompress};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ClientConfig {
    /// Endpoint region.
    pub region: String,

    /// Endpoint to be used. For instance, `"https://s3.my-provider.net"` or just
    /// `"s3.my-provider.net"` (default scheme is https).
    pub endpoint: String,

    /// The bucket name.
    ///
    /// Default: "bucket".
    pub bucket: String,

    /// Archive prefix before its id (Default: empty)
    #[serde(default)]
    pub archive_key_prefix: String,

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
            chunk_size <= u32::MAX as u64,
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
                chunk_size: NonZeroU32::new(chunk_size as u32).unwrap(),
                download_retries: config.download_retries,
            }),
        })
    }

    pub fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.client.list(prefix)
    }

    pub async fn find_archive(
        &self,
        mc_seqno: u32,
        last_mc_seqno: u32,
        prev_key_block_seqno: u32,
    ) -> Result<PendingArchiveResponse, Error> {
        if mc_seqno > last_mc_seqno {
            return Ok(PendingArchiveResponse::TooNew);
        }

        let blocks_after_key = mc_seqno - prev_key_block_seqno;
        let archive_number = (blocks_after_key - 1) / 100;
        let archive_id = (prev_key_block_seqno + 1) + (archive_number * 100);

        let meta = self
            .inner
            .client
            .head(&self.inner.make_archive_key(archive_id))
            .await?;

        Ok(PendingArchiveResponse::Found(PendingArchive {
            id: archive_id,
            size: NonZeroU64::new(meta.size as _).unwrap(),
        }))
    }

    #[tracing::instrument(skip_all, fields(
        block_id = %block_id.as_short_id(),
        kind = ?kind,
    ))]
    pub async fn download_persistent_state<W>(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
        output: W,
    ) -> anyhow::Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        tracing::debug!("started");
        scopeguard::defer! {
            tracing::debug!("finished");
        }

        let location = PathBuf::from("states").join(kind.make_file_name(block_id));
        let path = Path::from(location.display().to_string());

        let chunk_size = self.inner.chunk_size;
        let max_retries = self.inner.download_retries;

        let client = &self.inner.client.clone();

        let meta = client.head(&path).await?;
        let Some(target_size) = NonZeroU64::new(meta.size) else {
            return Err(empty_file_error(path));
        };

        download_and_decompress(
            target_size,
            chunk_size,
            PARALLEL_REQUESTS,
            output,
            |offset| {
                tracing::debug!("downloading persistent state chunk");

                download_with_retries(
                    path.clone(),
                    offset,
                    chunk_size,
                    client.clone(),
                    max_retries,
                    "persistent state chunk",
                )
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

    #[tracing::instrument(skip_all, fields(archive_id))]
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

    #[tracing::instrument(skip_all, fields(
        block_id = %block_id.as_short_id(),
    ))]
    pub async fn get_block_full(
        &self,
        block_id: &BlockId,
        mc_seqno: u32,
        last_mc_seqno: u32,
        prev_key_block_seqno: u32,
    ) -> anyhow::Result<Option<BlockDataFull>, Error> {
        let archive_id = match self
            .find_archive(mc_seqno, last_mc_seqno, prev_key_block_seqno)
            .await?
        {
            PendingArchiveResponse::Found(id) => id.id,
            PendingArchiveResponse::TooNew => return Ok(None),
        };

        // TODO: write to file for huge archives
        let output = BytesMut::new().writer();
        let writer = self.download_archive(archive_id, output).await?;

        let span = tracing::Span::current();

        let mut archive = tokio::task::spawn_blocking(move || -> anyhow::Result<Archive> {
            let _span = span.enter();

            let bytes = writer.into_inner().freeze();

            let archive = Archive::new(bytes)?;
            archive.check_mc_blocks_range()?;

            Ok(archive)
        })
        .await
        .map_err(|e| Error::JoinError { source: e })?
        .map_err(|e| Error::Generic {
            store: "processing_archive",
            source: e.into(),
        })?;

        let missing_field_error = |field: &str| -> Error {
            Error::Generic {
                store: "unpacking_archive",
                source: anyhow::anyhow!("{} not found in archive", field).into(),
            }
        };

        let block_full = archive
            .blocks
            .remove(block_id)
            .map(|x| -> Result<BlockDataFull, Error> {
                Ok(BlockDataFull {
                    block_id: *block_id,
                    block_data: x.block.ok_or_else(|| missing_field_error("block data"))?,
                    proof_data: x.proof.ok_or_else(|| missing_field_error("proof data"))?,
                    queue_diff_data: x
                        .queue_diff
                        .ok_or_else(|| missing_field_error("queue_diff data"))?,
                })
            })
            .transpose()?;

        Ok(block_full)
    }
}

pub enum PendingArchiveResponse {
    Found(PendingArchive),
    TooNew,
}

#[derive(Clone)]
pub struct PendingArchive {
    pub id: u32,
    pub size: NonZeroU64,
}

pub struct BlockDataFull {
    pub block_id: BlockId,
    pub block_data: Bytes,
    pub proof_data: Bytes,
    pub queue_diff_data: Bytes,
}

struct Inner {
    client: Arc<DynObjectStore>,
    archive_key_prefix: String,
    chunk_size: NonZeroU32,
    download_retries: usize,
}

impl Inner {
    fn make_archive_key(&self, archive_id: u32) -> Path {
        Path::from(format!("{}{archive_id}", self.archive_key_prefix))
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

struct DownloaderHandle;

impl DownloaderResponseHandle for DownloaderHandle {
    fn accept(self) {}
    fn reject(self) {}
}

// TODO: Move into config
const PARALLEL_REQUESTS: NonZeroUsize = NonZeroUsize::new(10).unwrap();
