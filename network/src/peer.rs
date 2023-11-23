use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::config::Config;
use crate::connection::Connection;
use crate::proto::{make_codec, recv_response, send_request};
use crate::types::{PeerId, Request, Response};

#[derive(Clone)]
pub struct Peer {
    connection: Connection,
    config: Arc<Config>,
}

impl Peer {
    pub fn new(connection: Connection, config: Arc<Config>) -> Self {
        Self { connection, config }
    }

    pub fn peer_id(&self) -> &PeerId {
        self.connection.peer_id()
    }

    pub async fn rpc(&self, request: Request<Bytes>) -> Result<Response<Bytes>> {
        let (send_stream, recv_stream) = self.connection.open_bi().await?;
        let mut send_stream = FramedWrite::new(send_stream, make_codec(&self.config));
        let mut recv_stream = FramedRead::new(recv_stream, make_codec(&self.config));

        send_request(&mut send_stream, request).await?;
        send_stream.get_mut().finish().await?;

        recv_response(&mut recv_stream).await
    }
}
