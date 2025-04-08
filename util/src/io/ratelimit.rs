use std::future::Future;
use std::io::{self};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use governor::clock::{self, QuantaClock, ReasonablyRealtime};
use governor::middleware::RateLimitingMiddleware;
use governor::state::{DirectStateStore, InMemoryState, NotKeyed};
use governor::{NotUntil, RateLimiter};
use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;
use tokio::time::Sleep;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    CheckPermit,
    Waiting,
    Writing,
}

pin_project! {
    #[derive(Debug)]
    pub struct RatelimitedWriter<
        'a,
        W,
        D: DirectStateStore,
        C: ReasonablyRealtime,
        MW: RateLimitingMiddleware<C::Instant, NegativeOutcome = NotUntil<C::Instant>>
    > {
        #[pin]
        inner: W,
        state: State,
        limiter: &'a RateLimiter<NotKeyed, D, C, MW>,
        #[pin]
        delay: Sleep,
    }
}

impl<'a, W, D, C, MW> RatelimitedWriter<'a, W, D, C, MW>
where
    W: AsyncWrite,
    D: DirectStateStore,
    C: clock::Clock + ReasonablyRealtime,
    MW: RateLimitingMiddleware<C::Instant, NegativeOutcome = NotUntil<C::Instant>>,
{
    pub fn new(inner: W, limiter: &'a RateLimiter<NotKeyed, D, C, MW>) -> Self {
        let delay = tokio::time::sleep(Duration::ZERO);
        RatelimitedWriter {
            inner,
            limiter,
            state: State::CheckPermit,
            delay,
        }
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(self: Pin<&mut Self>) -> Pin<&mut W> {
        self.project().inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W, D, C, MW> AsyncWrite for RatelimitedWriter<'_, W, D, C, MW>
where
    W: AsyncWrite,
    D: DirectStateStore,
    C: clock::Clock + ReasonablyRealtime,
    MW: RateLimitingMiddleware<C::Instant, NegativeOutcome = NotUntil<C::Instant>>,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut this = self.project();

        loop {
            match this.state {
                State::CheckPermit => match this.limiter.check() {
                    Ok(_) => {
                        *this.state = State::Writing;
                    }
                    Err(negative) => {
                        *this.state = State::Waiting;

                        let delay = negative.wait_time_from(this.limiter.clock().reference_point());

                        if delay > Duration::ZERO {
                            let deadline = tokio::time::Instant::now() + delay;
                            this.delay.as_mut().reset(deadline);
                        } else {
                            *this.state = State::CheckPermit;
                        }
                    }
                },

                State::Waiting => match this.delay.as_mut().poll(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(_) => {
                        *this.state = State::CheckPermit;
                    }
                },

                State::Writing => {
                    let offer_len = std::cmp::min(buf.len(), 1024);
                    let offer_buf = &buf[..offer_len];

                    let result = this.inner.as_mut().poll_write(cx, offer_buf);

                    return match result {
                        Poll::Ready(Ok(bytes_written)) => {
                            *this.state = State::CheckPermit;
                            Poll::Ready(Ok(bytes_written))
                        }
                        Poll::Ready(Err(e)) => {
                            *this.state = State::CheckPermit;
                            Poll::Ready(Err(e))
                        }
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        false
    }
}

pub fn rate_limiter(
    speed: bytesize::ByteSize,
) -> Result<RateLimiter<NotKeyed, InMemoryState, QuantaClock>, RlConstructError> {
    let num_cells = speed.as_u64() / 1024;

    let speed: u32 = match num_cells.try_into() {
        Ok(num_cells) => num_cells,
        Err(_) => return Err(RlConstructError::InvalidRateLimit(speed)),
    };
    let speed = NonZeroU32::new(speed).ok_or(RlConstructError::ZeroRateLimit)?;
    let quota = governor::Quota::per_second(speed);

    Ok(governor::RateLimiter::direct(quota))
}

#[derive(thiserror::Error, Debug)]
pub enum RlConstructError {
    #[error("Invalid rate limit: {0} / sec. Must be less than 35.18 terabits / second")]
    InvalidRateLimit(bytesize::ByteSize),
    #[error("Rate limit is zero")]
    ZeroRateLimit,
}

pub trait RatelimitAsyncWriteExt: AsyncWrite + Sized {
    /// Creates a new `RatelimitedWriter` that wraps the current writer and applies the given rate limiter.
    /// ONE RATE LIMITER CELL IS EQUIVALENT TO 1024 bytes written.
    /// So if you want to ratelimit to 1MB/s, you need to set the rate limiter to 1024 cells per second.
    fn ratelimit_write<D, C, MW>(
        self,
        limiter: &RateLimiter<NotKeyed, D, C, MW>,
    ) -> RatelimitedWriter<'_, Self, D, C, MW>
    where
        D: DirectStateStore,
        C: clock::Clock + ReasonablyRealtime,
        MW: RateLimitingMiddleware<C::Instant, NegativeOutcome = NotUntil<C::Instant>>,
    {
        RatelimitedWriter::new(self, limiter)
    }
}

impl<W> RatelimitAsyncWriteExt for W where W: AsyncWrite + Sized {}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::{Cursor, Read};
    use std::num::NonZeroU32;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use governor::{Quota, RateLimiter};
    use tokio::io::{AsyncWrite, AsyncWriteExt};
    use tokio::time::{Duration, Instant};

    use super::*;

    // --- Mock Writers ---

    #[derive(Debug)]
    struct PendingOnceWriter {
        first_poll: bool,
        total_written: usize,
        inner: Vec<u8>,
    }

    impl PendingOnceWriter {
        fn new() -> Self {
            PendingOnceWriter {
                first_poll: true,
                total_written: 0,
                inner: Vec::new(),
            }
        }
    }

    impl AsyncWrite for PendingOnceWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            if self.first_poll {
                self.first_poll = false;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                let n = buf.len();
                self.inner.extend_from_slice(buf);
                self.total_written += n;
                Poll::Ready(Ok(n))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct ErrorWriter {
        error_kind: io::ErrorKind,
    }

    impl ErrorWriter {
        fn new(kind: io::ErrorKind) -> Self {
            ErrorWriter { error_kind: kind }
        }
    }

    impl AsyncWrite for ErrorWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            Poll::Ready(Err(io::Error::new(self.error_kind, "mock write error")))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn permissive_limiter() -> RateLimiter<NotKeyed, InMemoryState, QuantaClock> {
        let quota = Quota::per_second(NonZeroU32::new(u32::MAX).unwrap());
        RateLimiter::direct(quota)
    }

    fn test_limiter(
        cells_per_sec: u32,
        burst: u32,
    ) -> RateLimiter<NotKeyed, InMemoryState, QuantaClock> {
        let quota = Quota::per_second(NonZeroU32::new(cells_per_sec).unwrap())
            .allow_burst(NonZeroU32::new(burst).unwrap());
        RateLimiter::direct(quota)
    }

    // --- Tests ---

    #[tokio::test]
    async fn test_ratelimited_writer_basic() {
        let limiter = test_limiter(1, 1);
        let data: Vec<u8> = vec![b'a'; 1024 * 3];
        let buffer = Vec::with_capacity(data.len());
        let cursor = Cursor::new(buffer);

        let writer = cursor.ratelimit_write(&limiter);
        let mut writer = std::pin::pin!(writer);

        let start = Instant::now();
        writer.write_all(&data).await.expect("write_all failed");
        let elapsed = start.elapsed();

        println!("Basic test elapsed: {:?}", elapsed);
        assert!(
            elapsed >= Duration::from_millis(1900) && elapsed < Duration::from_millis(2500),
            "Expected ~2s elapsed time, got {:?}",
            elapsed
        );

        let mut final_cursor = RatelimitedWriter::get_mut(writer);
        final_cursor.set_position(0);
        let mut written_data = Vec::new();
        final_cursor
            .read_to_end(&mut written_data)
            .expect("read failed");
        assert_eq!(written_data, data);
    }

    #[tokio::test]
    async fn test_ratelimited_writer_small_writes() {
        let limiter = test_limiter(2, 1);
        let data_chunk: Vec<u8> = vec![b'b'; 100];
        let num_chunks = 5;
        let total_size = data_chunk.len() * num_chunks;

        let buffer = Vec::with_capacity(total_size);
        let cursor = Cursor::new(buffer);

        let writer = cursor.ratelimit_write(&limiter);
        let mut writer = std::pin::pin!(writer);

        let start = Instant::now();
        for _ in 0..num_chunks {
            writer
                .write_all(&data_chunk)
                .await
                .expect("write_all chunk failed");
        }
        let elapsed = start.elapsed();

        println!("Small writes test elapsed: {:?}", elapsed);
        assert!(
            elapsed >= Duration::from_millis(1500) && elapsed < Duration::from_millis(2100),
            "Expected ~1.5s elapsed time, got {:?}",
            elapsed
        );

        let mut final_cursor = RatelimitedWriter::get_mut(writer);
        final_cursor.set_position(0);
        let mut written_data = Vec::new();
        final_cursor
            .read_to_end(&mut written_data)
            .expect("read failed");
        assert_eq!(written_data.len(), total_size);
        assert!(written_data.iter().all(|&x| x == b'b'));
    }

    #[tokio::test]
    async fn test_ratelimited_writer_burst() {
        let limiter = test_limiter(1, 3);
        let data: Vec<u8> = vec![b'c'; 1024 * 5];
        let buffer = Vec::with_capacity(data.len());
        let cursor = Cursor::new(buffer);

        let writer = cursor.ratelimit_write(&limiter);
        let mut writer = std::pin::pin!(writer);

        let start = Instant::now();
        writer.write_all(&data).await.expect("write_all failed");
        let elapsed = start.elapsed();

        println!("Burst test elapsed: {:?}", elapsed);
        assert!(
            elapsed >= Duration::from_millis(1900) && elapsed < Duration::from_millis(2500),
            "Expected ~2s elapsed time, got {:?}",
            elapsed
        );

        let mut final_cursor = RatelimitedWriter::get_mut(writer);
        final_cursor.set_position(0);
        let mut written_data = Vec::new();
        final_cursor
            .read_to_end(&mut written_data)
            .expect("read failed");
        assert_eq!(written_data, data);
    }

    #[tokio::test]
    async fn test_ratelimited_writer_inner_error() {
        let limiter = permissive_limiter();
        let inner_writer = ErrorWriter::new(io::ErrorKind::BrokenPipe);
        let data = vec![0u8; 100];

        let writer = inner_writer.ratelimit_write(&limiter);
        let mut writer = std::pin::pin!(writer);

        let result = writer.write_all(&data).await;

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(e.to_string(), "mock write error");
        }
    }

    #[tokio::test]
    async fn test_ratelimited_writer_inner_pending() {
        let limiter = permissive_limiter();
        let inner_writer = PendingOnceWriter::new();
        let data = vec![1u8; 512];

        let writer = inner_writer.ratelimit_write(&limiter);
        let mut writer = std::pin::pin!(writer);

        let result = writer.write_all(&data).await;
        assert!(result.is_ok());

        let final_inner_writer = &writer.inner;
        assert!(
            !final_inner_writer.first_poll,
            "Inner writer should have been polled more than once"
        );
        assert_eq!(final_inner_writer.total_written, data.len());
        assert_eq!(final_inner_writer.inner, data);
    }
}
