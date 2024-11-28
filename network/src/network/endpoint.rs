use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;

use crate::network::config::EndpointConfig;
use crate::network::connection::{extract_peer_id, parse_peer_identity, Connection};
use crate::types::{Direction, PeerId};

pub(crate) struct Endpoint {
    inner: quinn::Endpoint,
    local_addr: RwLock<SocketAddr>,
    config: EndpointConfig,
}

impl Endpoint {
    pub fn new(config: EndpointConfig, socket: std::net::UdpSocket) -> Result<Self> {
        let local_addr = RwLock::new(socket.local_addr()?);
        let server_config = config.quinn_server_config.clone();
        let endpoint = quinn::Endpoint::new(
            config.quinn_endpoint_config.clone(),
            Some(server_config),
            socket,
            Arc::new(quinn::TokioRuntime),
        )?;

        Ok(Self {
            inner: endpoint,
            local_addr,
            config,
        })
    }

    /// Returns the socket address that this Endpoint is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        *self.local_addr.read().unwrap()
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.config.peer_id
    }

    /// Close all of this endpoint's connections immediately and cease accepting new connections.
    pub fn close(&self) {
        tracing::trace!("closing endpoint");
        self.inner.close(0u32.into(), b"endpoint closed");
    }

    /// Wait for all connections on the endpoint to be cleanly shut down
    ///
    /// Waiting for this condition before exiting ensures that a good-faith effort is made to notify
    /// peers of recent connection closes, whereas exiting immediately could force them to wait out
    /// the idle timeout period.
    ///
    /// Does not proactively close existing connections or cause incoming connections to be
    /// rejected. Consider calling [`close()`] if that is desired.
    ///
    /// [`close()`]: Endpoint::close
    pub async fn wait_idle(&self, timeout: Duration) {
        if tokio::time::timeout(timeout, self.inner.wait_idle())
            .await
            .is_err()
        {
            tracing::warn!(
                timeout_sec = timeout.as_secs_f64(),
                "timeout reached while waiting for connections clean shutdown"
            );
        }
    }

    /// Connect to a remote endpoint expecting it to have the provided peer id.
    pub fn connect_with_expected_id(
        &self,
        address: &SocketAddr,
        peer_id: &PeerId,
    ) -> Result<Connecting, quinn::ConnectError> {
        let config = self.config.make_client_config_for_peer_id(peer_id);
        self.connect_with_client_config(config, address)
    }

    /// Connect to a remote endpoint using a custom configuration.
    pub fn connect_with_client_config(
        &self,
        config: quinn::ClientConfig,
        address: &SocketAddr,
    ) -> Result<Connecting, quinn::ConnectError> {
        self.inner
            .connect_with(config, *address, "tycho")
            .map(Connecting::new_outbound)
    }

    /// Get the next incoming connection attempt from a client
    ///
    /// Yields [`Connecting`] futures that must be `await`ed to obtain the final `Connection`, or
    /// `None` if the endpoint is [`close`](Self::close)d.
    pub fn accept(&self) -> Accept<'_> {
        Accept {
            inner: self.inner.accept(),
        }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub(crate) struct Accept<'a> {
        #[pin]
        inner: quinn::Accept<'a>,
    }
}

impl Future for Accept<'_> {
    type Output = Option<Connecting>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx).map(|c| {
            c.and_then(|c| {
                let remote_addr = c.remote_address();
                match c.accept() {
                    Ok(c) => Some(Connecting::new_inbound(c)),
                    Err(e) => {
                        tracing::warn!(%remote_addr, "failed to accept an incoming connection: {e:?}");
                        None
                    }
                }
            })
        })
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct Connecting {
    inner: quinn::Connecting,
    origin: Direction,
}

impl Connecting {
    fn new_inbound(inner: quinn::Connecting) -> Self {
        Self {
            inner,
            origin: Direction::Inbound,
        }
    }

    fn new_outbound(inner: quinn::Connecting) -> Self {
        Self {
            inner,
            origin: Direction::Outbound,
        }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.inner.remote_address()
    }

    pub fn into_0rtt(self) -> Into0RttResult {
        match self.inner.into_0rtt() {
            Ok((c, accepted)) => match c.peer_identity() {
                Some(identity) => match parse_peer_identity(identity) {
                    Some(peer_id) => Into0RttResult::Established(
                        Connection::with_peer_id(c, self.origin, peer_id),
                        accepted,
                    ),
                    None => Into0RttResult::InvalidCertificate,
                },
                None => Into0RttResult::WithoutIdentity(ConnectingFallback {
                    inner: Some(c),
                    accepted,
                    origin: self.origin,
                }),
            },
            Err(inner) => Into0RttResult::Unavailable(Self {
                inner,
                origin: self.origin,
            }),
        }
    }
}

impl Future for Connecting {
    type Output = Result<Connection, ConnectionInitError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx).map(|res| match res {
            Ok(c) => match extract_peer_id(&c) {
                Some(peer_id) => Ok(Connection::with_peer_id(c, self.origin, peer_id)),
                None => Err(ConnectionInitError::InvalidCertificate),
            },
            Err(e) => Err(ConnectionInitError::ConnectionFailed(e)),
        })
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct ConnectingFallback {
    inner: Option<quinn::Connection>,
    accepted: quinn::ZeroRttAccepted,
    origin: Direction,
}

impl Drop for ConnectingFallback {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.close(0u8.into(), b"cancelled");
        }
    }
}

impl Future for ConnectingFallback {
    type Output = Result<Connection, ConnectionInitError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.accepted).poll(cx).map(|_| {
            let c = self
                .inner
                .take()
                .expect("future must not be polled after completion");

            match c.close_reason() {
                Some(e) => Err(ConnectionInitError::ConnectionFailed(e)),
                None => match extract_peer_id(&c) {
                    Some(peer_id) => Ok(Connection::with_peer_id(c, self.origin, peer_id)),
                    None => Err(ConnectionInitError::InvalidCertificate),
                },
            }
        })
    }
}

pub(crate) enum Into0RttResult {
    Established(Connection, quinn::ZeroRttAccepted),
    WithoutIdentity(ConnectingFallback),
    InvalidCertificate,
    Unavailable(#[allow(unused)] Connecting),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ConnectionInitError {
    #[error(transparent)]
    ConnectionFailed(quinn::ConnectionError),
    #[error("invalid certificate")]
    InvalidCertificate,
}
