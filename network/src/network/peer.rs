use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::network::config::NetworkConfig;
use crate::network::connection::Connection;
use crate::network::wire::{make_codec, recv_response, send_request};
use crate::types::{PeerId, Request, Response};

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
        let (send_stream, recv_stream) = self.connection.open_bi().await?;
        let mut send_stream = FramedWrite::new(send_stream, make_codec(&self.config));
        let mut recv_stream = FramedRead::new(recv_stream, make_codec(&self.config));

        send_request(&mut send_stream, request).await?;
        send_stream.get_mut().finish().await?;

        recv_response(&mut recv_stream).await
    }

    pub async fn send_message(&self, request: Request) -> Result<()> {
        let send_stream = self.connection.open_uni().await?;
        let mut send_stream = FramedWrite::new(send_stream, make_codec(&self.config));

        send_request(&mut send_stream, request).await?;
        send_stream.get_mut().finish().await?;

        Ok(())
    }

    pub fn send_datagram(&self, request: Bytes) -> Result<()> {
        self.connection.send_datagram(request)?;
        Ok(())
    }
}
