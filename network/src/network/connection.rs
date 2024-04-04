use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use bytes::Bytes;
use quinn::{ConnectionError, SendDatagramError};

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
        .downcast::<Vec<rustls::Certificate>>()
        .ok()
        .and_then(|certificates| certificates.into_iter().next())
        .context("No certificate found in the connection")?;

    peer_id_from_certificate(&certificate).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_cert() {
        let peer_id =
            PeerId::from_str("7a6e86f44d5bd83093ba658fadccffbeb2878bb2e30db7b92e237e21eef77e07")
                .unwrap();

        #[allow(clippy::octal_escapes)]
        let certificate = rustls::Certificate(b"0\x81\xd80\x81\x8b\xa0\x03\x02\x01\x02\x02\x15\0\xa0\xd7\x8e\xa8\xf2\xfe\xd8\x10\xeb3\x90\x19br\x91S`\x01\xe0)0\x05\x06\x03+ep0\00 \x17\r750101000000Z\x18\x0f40960101000000Z0\00*0\x05\x06\x03+ep\x03!\0zn\x86\xf4M[\xd80\x93\xbae\x8f\xad\xcc\xff\xbe\xb2\x87\x8b\xb2\xe3\r\xb7\xb9.#~!\xee\xf7~\x07\xa3\x140\x120\x10\x06\x03U\x1d\x11\x04\t0\x07\x82\x05tycho0\x05\x06\x03+ep\x03A\0\xe3s-\xaf\xbd\xac\x81\xbc\x82\x8a\x83\xf8\xa3\xe3\xcb\x118\xa8g\xef_M\x99*\x7f\xed\x1bQ=\x9f\xf1\xc4%q\xa9g\xfa\x0f\x12R\x84LH\xff\x99\xa7bH\xfc\xbdb\xbcY\xc5C\x11\xc5\x91\x8dn#\xe2\x9b\x05".to_vec());
        let parsed_peer_id = peer_id_from_certificate(&certificate).unwrap();

        assert_eq!(peer_id, parsed_peer_id);
    }
}
