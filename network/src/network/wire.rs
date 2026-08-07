use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::LengthDelimitedCodec;

use crate::network::config::NetworkConfig;
use crate::network::connection::Connection;
use crate::types::Direction;

pub(crate) fn make_codec(config: &NetworkConfig) -> LengthDelimitedCodec {
    let mut builder = LengthDelimitedCodec::builder();

    builder.max_frame_length(config.max_frame_size.0 as usize);

    builder.length_field_length(4).big_endian().new_codec()
}

pub(crate) async fn handshake(connection: &Connection) -> Result<(), HandshakeError> {
    match connection.origin() {
        Direction::Inbound => {
            let mut send_stream = connection
                .open_uni()
                .await
                .map_err(HandshakeError::ConnectionFailed)?;

            send_ready(&mut send_stream)
                .await
                .map_err(HandshakeError::WireError)?;

            // Finish the stream (ignore double-finish error)
            _ = send_stream.finish();

            match send_stream.stopped().await {
                Ok(None) => Ok(()),
                Ok(Some(code)) => Err(HandshakeError::WireError(std::io::Error::other(format!(
                    "ready stream stopped: {code}"
                )))),
                Err(quinn::StoppedError::ConnectionLost(e)) => {
                    Err(HandshakeError::ConnectionFailed(e))
                }
                Err(quinn::StoppedError::ZeroRttRejected) => {
                    Err(HandshakeError::WireError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        WireError::ZeroRttRejected,
                    )))
                }
            }
        }
        Direction::Outbound => {
            let mut recv_stream = connection
                .accept_uni()
                .await
                .map_err(HandshakeError::ConnectionFailed)?;

            recv_ready(&mut recv_stream)
                .await
                .map_err(HandshakeError::WireError)
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum HandshakeError {
    #[error(transparent)]
    ConnectionFailed(quinn::ConnectionError),
    #[error("wire error")]
    WireError(#[source] std::io::Error),
}

async fn send_ready<T: AsyncWrite + Unpin>(send_stream: &mut T) -> std::io::Result<()> {
    send_stream.write_all(READY).await
}

async fn recv_ready<T: AsyncRead + Unpin>(recv_stream: &mut T) -> std::io::Result<()> {
    let mut buffer: [u8; 4] = [0; 4];
    recv_stream.read_exact(&mut buffer).await?;

    if &buffer == READY {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            WireError::InvalidHeader,
        ))
    }
}

const READY: &[u8; 4] = b"done";

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum WireError {
    #[error("invalid header")]
    InvalidHeader,
    #[error("unexpected eof")]
    UnexpectedEof,
    #[error("0-rtt rejected")]
    ZeroRttRejected,
}
