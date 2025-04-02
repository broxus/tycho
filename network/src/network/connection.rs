use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use metrics::Label;
use quinn::{ConnectionError, VarInt};
use webpki::types::CertificateDer;

use crate::network::config::ConnectionMetricsLevel;
use crate::network::crypto::peer_id_from_certificate;
use crate::types::{Direction, InboundRequestMeta, PeerId};

#[derive(Clone)]
pub struct Connection {
    inner: quinn::Connection,
    request_meta: Arc<InboundRequestMeta>,
}

macro_rules! emit_gauges {
    ($prefix:literal, $stats:expr, $labels:expr, [ $($field:ident),* $(,)? ]) => {
        $(
            metrics::gauge!(concat!($prefix, stringify!($field)), $labels.clone())
                .set($stats.$field as f64);
        )*
    };
}

impl Connection {
    pub const LIMIT_EXCEEDED_ERROR_CODE: VarInt = VarInt::from_u32(0xdead);

    pub fn with_peer_id(
        inner: quinn::Connection,
        origin: Direction,
        peer_id: PeerId,
        connection_metrics: Option<ConnectionMetricsLevel>,
    ) -> Self {
        let connection = Self {
            request_meta: Arc::new(InboundRequestMeta {
                peer_id,
                origin,
                remote_address: inner.remote_address(),
            }),
            inner,
        };

        let conn = connection.inner.clone();

        let Some(connection_metrics) = connection_metrics else {
            return connection;
        };

        let peer_id = connection.request_meta.peer_id;
        let remote_addr = connection.remote_address().to_string();

        // we can't use `spawn_metrics_loop` here because we can't get arc reference to connection
        tokio::spawn(async move {
            const INTERVAL: Duration = Duration::from_secs(5);

            let mut labels = vec![Label::new("peer_addr", remote_addr)];

            if connection_metrics.should_export_peer_id() {
                labels.push(Label::new("peer_id", peer_id.to_string()));
                labels.shrink_to_fit();
            }

            loop {
                let stats = conn.stats();

                metrics::gauge!("tycho_network_connection_rtt_ms", labels.clone())
                    .set(stats.path.rtt.as_millis() as f64);

                metrics::gauge!("tycho_network_connection_invalid_messages", labels.clone()).set(
                    stats.frame_rx.connection_close as f64 + stats.frame_rx.reset_stream as f64,
                );

                emit_gauges!("tycho_network_connection_", stats.path, labels, [
                    cwnd,              // Congestion window size
                    congestion_events, // Network congestion indicators
                    lost_packets,      // Total packet loss
                    sent_packets       // Baseline for loss calculations
                ]);

                emit_gauges!("tycho_network_connection_rx_", stats.udp_rx, labels, [
                    bytes
                ]);

                emit_gauges!("tycho_network_connection_tx_", stats.udp_tx, labels, [
                    bytes
                ]);

                // Frame RX
                emit_gauges!(
                    "tycho_network_connection_rx_",
                    stats.frame_rx,
                    labels.clone(),
                    [
                        acks,
                        crypto,
                        connection_close,
                        data_blocked,
                        max_data,
                        max_stream_data,
                        ping,
                        reset_stream,
                        stream_data_blocked,
                        streams_blocked_bidi,
                        stop_sending,
                        stream
                    ]
                );

                // Frame TX
                emit_gauges!("tycho_network_connection_tx_", stats.frame_tx, labels, [
                    acks,
                    crypto,
                    connection_close,
                    data_blocked,
                    max_data,
                    max_stream_data,
                    ping,
                    reset_stream,
                    stream_data_blocked,
                    streams_blocked_bidi,
                    stop_sending,
                    stream
                ]);

                tokio::select! {
                    _ = tokio::time::sleep(INTERVAL) => {}
                    _ = conn.closed() => {
                        tracing::debug!(
                            %peer_id,
                            addr = %conn.remote_address(),
                            "connection metrics loop stopped",
                        );
                        return;
                    },
                }
            }
        });

        connection
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

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.inner.stats()
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

pub(crate) fn extract_peer_id(connection: &quinn::Connection) -> Option<PeerId> {
    connection.peer_identity().and_then(parse_peer_identity)
}

pub(crate) fn parse_peer_identity(identity: Box<dyn std::any::Any>) -> Option<PeerId> {
    let certificate = identity
        .downcast::<Vec<CertificateDer<'static>>>()
        .ok()?
        .into_iter()
        .next()?;

    peer_id_from_certificate(&certificate).ok()
}
