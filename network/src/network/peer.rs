use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio_util::codec::{FramedRead, FramedWrite};
use tycho_util::metrics::{GaugeGuard, HistogramGuard};

use crate::network::config::NetworkConfig;
use crate::network::connection::Connection;
use crate::network::wire::{make_codec, recv_response, send_request};
use crate::types::{PeerId, Request, Response};

// Histograms
const METRIC_OUT_QUERIES_TIME: &str = "tycho_net_out_queries_time";
const METRIC_OUT_MESSAGES_TIME: &str = "tycho_net_out_messages_time";

// Counters
const METRIC_OUT_QUERIES_TOTAL: &str = "tycho_net_out_queries_total";
const METRIC_OUT_MESSAGES_TOTAL: &str = "tycho_net_out_messages_total";

// Gauges
const METRIC_OUT_QUERIES: &str = "tycho_net_out_queries";
const METRIC_OUT_MESSAGES: &str = "tycho_net_out_messages";

#[derive(Clone)]
pub struct Peer {
    connection: Connection,
    config: Arc<NetworkConfig>,
}

impl Peer {
    pub(crate) fn new(connection: Connection, config: Arc<NetworkConfig>) -> Self {
        Self { connection, config }
    }

    pub fn peer_id(&self) -> &PeerId {
        self.connection.peer_id()
    }

    pub async fn rpc(&self, request: Request) -> Result<Response> {
        metrics::counter!(METRIC_OUT_QUERIES_TOTAL).increment(1);
        let _gauge = GaugeGuard::increment(METRIC_OUT_QUERIES, 1);
        let _histogram = HistogramGuard::begin(METRIC_OUT_QUERIES_TIME);

        let (send_stream, recv_stream) = self.connection.open_bi().await?;
        let mut send_stream = FramedWrite::new(send_stream, make_codec(&self.config));
        let mut recv_stream = FramedRead::new(recv_stream, make_codec(&self.config));

        send_request(&mut send_stream, request).await?;
        send_stream.get_mut().finish()?;

        recv_response(&mut recv_stream).await.map_err(Into::into)
    }

    pub async fn send_message(&self, request: Request) -> Result<()> {
        metrics::counter!(METRIC_OUT_MESSAGES_TOTAL).increment(1);
        let _gauge = GaugeGuard::increment(METRIC_OUT_MESSAGES, 1);
        let _histogram = HistogramGuard::begin(METRIC_OUT_MESSAGES_TIME);

        let send_stream = self.connection.open_uni().await?;
        let mut send_stream = FramedWrite::new(send_stream, make_codec(&self.config));

        send_request(&mut send_stream, request).await?;
        send_stream.get_mut().finish()?;
        _ = send_stream.get_mut().stopped().await;

        Ok(())
    }

    pub fn send_datagram(&self, request: Bytes) -> Result<()> {
        self.connection.send_datagram(request)?;
        Ok(())
    }
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Peer")
            .field(&self.connection.peer_id())
            .finish()
    }
}
