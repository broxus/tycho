use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use bytes::Bytes;
use quinn::{ConnectionError, SendDatagramError};
use webpki::types::CertificateDer;

use crate::network::crypto::peer_id_from_certificate;
use crate::types::{Direction, InboundRequestMeta, PeerId};

#[derive(Clone)]
pub struct Connection {
    inner: quinn::Connection,
    request_meta: Arc<InboundRequestMeta>,
}

impl Connection {
    pub fn new(inner: quinn::Connection, origin: Direction) -> Result<Self> {
        let peer_id = extract_peer_id(&inner)?;
        Ok(Self::with_peer_id(inner, origin, peer_id))
    }

    pub fn with_peer_id(inner: quinn::Connection, origin: Direction, peer_id: PeerId) -> Self {
        Self {
            request_meta: Arc::new(InboundRequestMeta {
                peer_id,
                origin,
                remote_address: inner.remote_address(),
            }),
            inner,
        }
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
        self.inner.close(0u8.into(), b"connection closed");
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        self.inner
            .open_bi()
            .await
            .map(|(send, recv)| (SendStream(send), RecvStream(recv)))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        self.inner
            .accept_bi()
            .await
            .map(|(send, recv)| (SendStream(send), RecvStream(recv)))
    }

    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        self.inner.open_uni().await.map(SendStream)
    }

    pub async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        self.inner.accept_uni().await.map(RecvStream)
    }

    pub fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        self.inner.send_datagram(data)
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

#[repr(transparent)]
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
        Pin::new(&mut self.0)
            .poll_write(cx, buf)
            .map_err(std::io::Error::from)
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

#[repr(transparent)]
pub struct RecvStream(quinn::RecvStream);

impl std::ops::Deref for RecvStream {
    type Target = quinn::RecvStream;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for RecvStream {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl tokio::io::AsyncRead for RecvStream {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

pub(crate) fn extract_peer_id(connection: &quinn::Connection) -> Result<PeerId> {
    parse_peer_identity(
        connection
            .peer_identity()
            .context("No identity found in the connection")?,
    )
}

pub(crate) fn parse_peer_identity(identity: Box<dyn std::any::Any>) -> Result<PeerId> {
    let certificate = identity
        .downcast::<Vec<CertificateDer<'static>>>()
        .ok()
        .and_then(|certificates| certificates.into_iter().next())
        .context("No certificate found in the connection")?;

    peer_id_from_certificate(&certificate).map_err(Into::into)
}
