use anyhow::Result;
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::network::config::NetworkConfig;
use crate::network::connection::Connection;
use crate::types::{Direction, Request, Response, Version};

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

            send_version(&mut send_stream, Version::V1)
                .await
                .map_err(HandshakeError::WireError)?;

            // Finish the stream (ignore double-finish error)
            _ = send_stream.finish();

            match send_stream.stopped().await {
                Ok(_) => Ok(()),
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

            match recv_version(&mut recv_stream).await {
                Ok(_) => Ok(()),
                Err(e) => Err(HandshakeError::WireError(e)),
            }
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

pub(crate) async fn send_request<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    request: Request,
) -> std::io::Result<()> {
    send_version(send_stream.get_mut(), request.version).await?;
    send_stream.send(request.body).await
}

pub(crate) async fn recv_request<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> std::io::Result<Request> {
    let version = recv_version(recv_stream.get_mut()).await?;
    match recv_stream.next().await {
        Some(body) => Ok(Request {
            version,
            body: body?.freeze(),
        }),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            WireError::UnexpectedEof,
        )),
    }
}

pub(crate) async fn send_response<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    response: Response,
) -> std::io::Result<()> {
    send_version(send_stream.get_mut(), response.version).await?;
    send_stream.send(response.body).await
}

pub(crate) async fn recv_response<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> std::io::Result<Response> {
    let version = recv_version(recv_stream.get_mut()).await?;
    match recv_stream.next().await {
        Some(body) => Ok(Response {
            version,
            body: body?.freeze(),
        }),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            WireError::UnexpectedEof,
        )),
    }
}

async fn send_version<T: AsyncWrite + Unpin>(
    send_stream: &mut T,
    version: Version,
) -> std::io::Result<()> {
    let mut buffer: [u8; 8] = [0; 8];
    buffer[0..=4].copy_from_slice(MAGIC);
    buffer[5..=6].copy_from_slice(&version.to_u16().to_be_bytes());
    send_stream.write_all(&buffer).await
}

async fn recv_version<T: AsyncRead + Unpin>(recv_stream: &mut T) -> std::io::Result<Version> {
    let mut buffer: [u8; 8] = [0; 8];
    recv_stream.read_exact(&mut buffer).await?;

    if &buffer[0..=4] != MAGIC || buffer[7] != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            WireError::InvalidHeader,
        ));
    }

    match Version::try_from_u16(u16::from_be_bytes([buffer[5], buffer[6]])) {
        Some(version) => Ok(version),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            WireError::InvalidVersion,
        )),
    }
}

const MAGIC: &[u8; 5] = b"tycho";

#[derive(Clone, Copy, Debug, thiserror::Error)]
enum WireError {
    #[error("invalid header")]
    InvalidHeader,
    #[error("invalid version")]
    InvalidVersion,
    #[error("unexpected eof")]
    UnexpectedEof,
    #[error("0-rtt rejected")]
    ZeroRttRejected,
}
