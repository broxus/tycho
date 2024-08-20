use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::buf::Writer;
use bytes::BytesMut;

pub struct BytesWriter {
    pub writer: Writer<BytesMut>,
}

impl tokio::io::AsyncWrite for BytesWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().writer.get_mut().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(io::Write::write_vectored(&mut self.writer, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}
