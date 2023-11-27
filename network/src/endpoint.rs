use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;

use crate::config::EndpointConfig;
use crate::connection::Connection;
use crate::types::{Direction, PeerId};

pub struct Endpoint {
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

    /// Connect to a remote endpoint using the endpoint configuration.
    pub fn connect(&self, address: SocketAddr) -> Result<Connecting> {
        self.connect_with_client_config(self.config.quinn_client_config.clone(), address)
    }

    /// Connect to a remote endpoint expecting it to have the provided peer id.
    pub fn connect_with_expected_id(
        &self,
        address: SocketAddr,
        peer_id: PeerId,
    ) -> Result<Connecting> {
        let config = self.config.make_client_config_for_peer_id(peer_id)?;
        self.connect_with_client_config(config, address)
    }

    /// Connect to a remote endpoint using a custom configuration.
    fn connect_with_client_config(
        &self,
        config: quinn::ClientConfig,
        address: SocketAddr,
    ) -> Result<Connecting> {
        self.inner
            .connect_with(config, address, &self.config.server_name)
            .map_err(Into::into)
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
    pub struct Accept<'a> {
        #[pin]
        inner: quinn::Accept<'a>,
    }
}

impl<'a> Future for Accept<'a> {
    type Output = Option<Connecting>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project()
            .inner
            .poll(cx)
            .map(|c| c.map(Connecting::new_inbound))
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Connecting {
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
}

impl Future for Connecting {
    type Output = Result<Connection>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx).map(|res| {
            res.map_err(anyhow::Error::from)
                .and_then(|c| Connection::new(c, self.origin))
                .map_err(|e| anyhow::anyhow!("failed establishing {} connection: {e}", self.origin))
        })
    }
}
