use std::io::Write;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{DynObjectStore, Error, ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tycho_block_util::archive::{Archive, ArchiveVerifier};
use tycho_types::models::BlockId;
use tycho_util::compression::ZstdDecompressStream;
use tycho_util::futures::JoinTask;

use crate::storage::PersistentStateKind;

#[derive(Clone)]
#[repr(transparent)]
pub struct S3Client {
    inner: Arc<Inner>,
}

impl S3Client {
    pub fn new(config: &S3ClientConfig) -> anyhow::Result<Self> {
        let client: Arc<DynObjectStore> = match &config.provider {
            S3ProviderConfig::Aws {
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
            } => Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(&config.bucket_name)
                    .with_endpoint(endpoint)
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key)
                    .with_client_options(
                        object_store::ClientOptions::new().with_allow_http(*allow_http),
                    )
                    .build()?,
            ),
            S3ProviderConfig::Gcs { credentials_path } => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::new()
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_connect_timeout_disabled()
                            .with_timeout_disabled(),
                    )
                    .with_bucket_name(&config.bucket_name)
                    .with_application_credentials(credentials_path)
                    .build()?,
            ),
        };

        Ok(Self {
            inner: Arc::new(Inner {
                settings: Settings {
                    chunk_size: config.chunk_size,
                    download_retries: config.download_retries,
                },
                client,
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
            .head(&Path::from(archive_id.to_string()))
            .await?;

        Ok(PendingArchiveResponse::Found(PendingArchive {
            id: archive_id as u64,
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

        let chunk_size = self.inner.settings.chunk_size.as_u64();
        let max_retries = self.inner.settings.download_retries;

        let client = &self.inner.client.clone();

        let meta = client.head(&path).await?;
        let target_size = meta.size;

        download_compressed(
            target_size,
            chunk_size,
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
    }

    #[tracing::instrument(skip_all, fields(archive_id))]
    pub async fn download_archive<W>(&self, archive_id: u64, output: W) -> anyhow::Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        use futures_util::FutureExt;

        tracing::debug!("started");
        scopeguard::defer! {
            tracing::debug!("finished");
        }

        let chunk_size = self.inner.settings.chunk_size.as_u64();
        let max_retries = self.inner.settings.download_retries;

        let client = &self.inner.client.clone();

        let path = Path::from(archive_id.to_string());
        let meta = client.head(&path).await?;

        let target_size = meta.size;

        download_compressed(
            target_size,
            chunk_size,
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
    pub id: u64,
    pub size: NonZeroU64,
}

pub struct BlockDataFull {
    pub block_id: BlockId,
    pub block_data: Bytes,
    pub proof_data: Bytes,
    pub queue_diff_data: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct S3ClientConfig {
    /// TODO
    ///
    /// Default: "bucket".
    pub bucket_name: String,

    /// TODO
    ///
    /// Default: GCS.
    pub provider: S3ProviderConfig,

    /// TODO
    ///
    /// Default: 10 MB.
    pub chunk_size: ByteSize,

    /// Number of retries to download archives/blocks/states
    ///
    /// Default: 10.
    pub download_retries: usize,
}

impl Default for S3ClientConfig {
    fn default() -> Self {
        Self {
            bucket_name: "bucket".to_string(),
            provider: S3ProviderConfig::Gcs {
                credentials_path: "credentials.json".to_owned(),
            },
            chunk_size: ByteSize::mb(10),
            download_retries: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum S3ProviderConfig {
    #[serde(rename = "aws")]
    Aws {
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
        allow_http: bool,
    },

    #[serde(rename = "gcs")]
    Gcs { credentials_path: String },
}

struct Inner {
    settings: Settings,
    client: Arc<DynObjectStore>,
}

async fn download_compressed<S, T, DF, DFut, PF, FF>(
    target_size: u64,
    chunk_size: u64,
    mut state: S,
    mut download_fn: DF,
    mut process_fn: PF,
    finalize_fn: FF,
) -> Result<T, Error>
where
    S: Send + 'static,
    T: Send + 'static,
    DF: FnMut(u64) -> DFut,
    DFut: Future<Output = DownloadedChunkResult> + Send + 'static,
    PF: FnMut(&mut S, &[u8]) -> anyhow::Result<()> + Send + 'static,
    FF: FnOnce(S) -> anyhow::Result<T> + Send + 'static,
{
    const PARALLEL_REQUESTS: usize = 10;

    let (chunks_tx, mut chunks_rx) = mpsc::channel::<Bytes>(PARALLEL_REQUESTS);

    let span = tracing::Span::current();
    let processing_task = tokio::task::spawn_blocking(move || {
        let _span = span.enter();

        let mut zstd_decoder = ZstdDecompressStream::new(chunk_size as usize)?;

        // Reuse buffer for decompressed data
        let mut decompressed_chunk = Vec::new();

        // Receive and process chunks
        let mut downloaded = 0;
        while let Some(chunk) = chunks_rx.blocking_recv() {
            anyhow::ensure!(chunk.len() <= chunk_size as usize, "received invalid chunk");

            downloaded += chunk.len() as u64;
            tracing::debug!(
                downloaded = %bytesize::ByteSize::b(downloaded),
                "got chunk"
            );

            anyhow::ensure!(downloaded <= target_size, "received too many chunks");

            decompressed_chunk.clear();
            zstd_decoder.write(chunk.as_ref(), &mut decompressed_chunk)?;

            process_fn(&mut state, &decompressed_chunk)?;
        }

        anyhow::ensure!(
            target_size == downloaded,
            "size mismatch (target size: {target_size}; downloaded: {downloaded})",
        );

        finalize_fn(state)
    });

    let stream = futures_util::stream::iter((0..target_size).step_by(chunk_size as usize))
        .map(|offset| JoinTask::new(download_fn(offset)))
        .buffered(PARALLEL_REQUESTS);

    let mut stream = std::pin::pin!(stream);
    while let Some(chunk) = stream.next().await.transpose()? {
        if chunks_tx.send(chunk).await.is_err() {
            break;
        }
    }

    drop(chunks_tx);

    let output = processing_task
        .await
        .map_err(|e| Error::JoinError { source: e })?
        .map_err(|e| Error::Generic {
            store: "processing_task",
            source: e.into(),
        })?;

    Ok(output)
}

async fn download_with_retries(
    path: Path,
    offset: u64,
    length: u64,
    client: Arc<DynObjectStore>,
    max_retries: usize,
    name: &'static str,
) -> DownloadedChunkResult {
    let mut retries = 0;
    loop {
        let range = std::ops::Range {
            start: offset,
            end: offset + length,
        };

        match client.get_range(&path, range).await {
            Ok(bytes) => {
                return Ok(bytes);
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

struct Settings {
    pub chunk_size: ByteSize,
    pub download_retries: usize,
}

type DownloadedChunkResult = anyhow::Result<Bytes, Error>;
