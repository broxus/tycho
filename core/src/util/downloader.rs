use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use anyhow::Result;
use bytes::Bytes;
use futures_util::StreamExt;
use scopeguard::ScopeGuard;
use tokio::sync::mpsc;
use tracing::Instrument;
use tycho_util::compression::ZstdDecompressStream;
use tycho_util::futures::JoinTask;

pub trait DownloaderResponseHandle: Send + 'static {
    fn accept(self);
    fn reject(self);
}

pub async fn download_and_decompress<S, T, H, E, DF, DFut, PF, FF>(
    target_size: NonZeroU64,
    chunk_size: NonZeroU32,
    parallel_requests: NonZeroUsize,
    mut state: S,
    mut download_fn: DF,
    mut process_fn: PF,
    finalize_fn: FF,
) -> Result<T, DownloaderError<E>>
where
    S: Send + 'static,
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
    H: DownloaderResponseHandle,
    DF: FnMut(u64) -> DFut,
    DFut: Future<Output = Result<(H, Bytes), E>> + Send + 'static,
    PF: FnMut(&mut S, &[u8]) -> Result<()> + Send + 'static,
    FF: FnOnce(S) -> Result<T> + Send + 'static,
{
    let target_size = target_size.get();
    let chunk_size = chunk_size.get() as usize;

    let (chunks_tx, mut chunks_rx) = mpsc::channel::<(H, Bytes)>(parallel_requests.get());

    let span = tracing::Span::current();
    let processing_task = tokio::task::spawn_blocking(move || {
        let _span = span.enter();

        let mut zstd_decoder = ZstdDecompressStream::new(chunk_size)
            .map_err(|e| DownloaderError::ProcessingFailed(e.into()))?;

        // Reuse buffer for decompressed data
        let mut decompressed_chunk = Vec::new();

        // Receive and process chunks
        let mut downloaded = 0;
        while let Some((h, chunk)) = chunks_rx.blocking_recv() {
            let guard = scopeguard::guard(h, |handle| {
                handle.reject();
            });

            if chunk.len() > chunk_size {
                return Err(DownloaderError::TooBigChunk);
            }

            downloaded += chunk.len() as u64;
            tracing::debug!(
                downloaded = %bytesize::ByteSize::b(downloaded),
                "got chunk"
            );

            if downloaded > target_size {
                return Err(DownloaderError::TooManyChunks);
            }

            decompressed_chunk.clear();
            zstd_decoder
                .write(chunk.as_ref(), &mut decompressed_chunk)
                .map_err(|e| DownloaderError::ProcessingFailed(e.into()))?;

            process_fn(&mut state, &decompressed_chunk)
                .map_err(DownloaderError::ProcessingFailed)?;

            ScopeGuard::into_inner(guard).accept(); // defuse the guard
        }

        if target_size != downloaded {
            return Err(DownloaderError::SizeMismatch {
                target_size,
                downloaded,
            });
        }

        finalize_fn(state).map_err(DownloaderError::ProcessingFailed)
    });

    let span = tracing::Span::current();
    let stream = futures_util::stream::iter((0..target_size).step_by(chunk_size))
        .map(|offset| JoinTask::new(download_fn(offset).instrument(span.clone())))
        .buffered(parallel_requests.get());

    let mut stream = std::pin::pin!(stream);
    while let Some(chunk) = stream
        .next()
        .await
        .transpose()
        .map_err(DownloaderError::DownloadFailed)?
    {
        if chunks_tx.send(chunk).await.is_err() {
            break;
        }
    }

    drop(chunks_tx);

    processing_task
        .await
        .map_err(|_e| DownloaderError::Cancelled)?
}

#[derive(Debug, thiserror::Error)]
pub enum DownloaderError<E> {
    #[error(transparent)]
    DownloadFailed(E),
    #[error(transparent)]
    ProcessingFailed(anyhow::Error),
    #[error("received too big chunk")]
    TooBigChunk,
    #[error("received too many chunks")]
    TooManyChunks,
    #[error("size mismatch (target size: {target_size}; downloaded: {downloaded})")]
    SizeMismatch { target_size: u64, downloaded: u64 },
    #[error("operation cancelled")]
    Cancelled,
}
