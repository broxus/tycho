use std::io::Write;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use bytesize::ByteSize;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{DynObjectStore, Error, ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::ArchiveVerifier;

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

    pub async fn get_archive_info(
        &self,
        archive_id: u32,
    ) -> Result<Option<BriefArchiveInfo>, Error> {
        let meta = match self
            .inner
            .client
            .head(&self.inner.make_archive_key(archive_id))
            .await
        {
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
}

#[derive(Clone)]
pub struct BriefArchiveInfo {
    pub archive_id: u32,
    pub size: NonZeroU64,
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
