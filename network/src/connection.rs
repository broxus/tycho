use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use bytes::Bytes;
use quinn::{ConnectionError, RecvStream};

use crate::types::{Direction, InboundRequestMeta, PeerId};

#[derive(Clone)]
pub struct Connection {
    inner: quinn::Connection,
    request_meta: Arc<InboundRequestMeta>,
}

impl Connection {
    pub fn new(inner: quinn::Connection, origin: Direction) -> Result<Self> {
        let peer_id = extract_peer_id(&inner)?;
        Ok(Self {
            request_meta: Arc::new(InboundRequestMeta {
                peer_id,
                origin,
                remote_address: inner.remote_address(),
            }),
            inner,
        })
    }

    pub fn request_meta(&self) -> &Arc<InboundRequestMeta> {
        &self.request_meta
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.request_meta.peer_id
    }

    pub fn stable_id(&self) -> usize {
        self.inner.stable_id()
    }

    pub fn origin(&self) -> Direction {
        self.request_meta.origin
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.request_meta.remote_address
    }

    pub fn close(&self) {
        self.inner.close(0u8.into(), b"connection closed")
    }

    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        self.inner.open_uni().await.map(SendStream)
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        self.inner
            .open_bi()
            .await
            .map(|(send, recv)| (SendStream(send), recv))
    }

    pub async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        self.inner.accept_uni().await
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        self.inner
            .accept_bi()
            .await
            .map(|(send, recv)| (SendStream(send), recv))
    }

    pub async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        self.inner.read_datagram().await
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("origin", &self.request_meta.origin)
            .field("id", &self.stable_id())
            .field("remote_address", &self.remote_address())
            .field("peer_id", &self.request_meta.peer_id)
            .finish_non_exhaustive()
    }
}

pub struct SendStream(quinn::SendStream);

impl Drop for SendStream {
    fn drop(&mut self) {
        _ = self.0.reset(0u8.into());
    }
}

impl std::ops::Deref for SendStream {
    type Target = quinn::SendStream;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for SendStream {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl tokio::io::AsyncWrite for SendStream {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
}

fn extract_peer_id(connection: &quinn::Connection) -> Result<PeerId> {
    let certificate = connection
        .peer_identity()
        .and_then(|identity| identity.downcast::<Vec<rustls::Certificate>>().ok())
        .and_then(|certificates| certificates.into_iter().next())
        .context("No certificate found in the connection")?;

    crate::crypto::peer_id_from_certificate(&certificate).map_err(Into::into)
}
