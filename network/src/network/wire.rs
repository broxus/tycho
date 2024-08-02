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

pub(crate) async fn handshake(connection: &Connection) -> Result<()> {
    match connection.origin() {
        Direction::Inbound => {
            let mut send_stream = connection.open_uni().await?;
            send_version(&mut send_stream, Version::V1).await?;
            send_stream.finish()?;
            // Wait for stream to close
            _ = send_stream.stopped().await;
        }
        Direction::Outbound => {
            let mut recv_stream = connection.accept_uni().await?;
            recv_version(&mut recv_stream).await?;
        }
    }
    Ok(())
}

pub(crate) async fn send_request<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    request: Request,
) -> Result<()> {
    send_version(send_stream.get_mut(), request.version).await?;
    send_stream.send(request.body).await?;
    Ok(())
}

pub(crate) async fn recv_request<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> Result<Request> {
    let version = recv_version(recv_stream.get_mut()).await?;
    let body = match recv_stream.next().await {
        Some(body) => body?.freeze(),
        None => anyhow::bail!("unexpected EOF"),
    };
    Ok(Request { version, body })
}

pub(crate) async fn send_response<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    response: Response,
) -> Result<()> {
    send_version(send_stream.get_mut(), response.version).await?;
    send_stream.send(response.body).await?;
    Ok(())
}

pub(crate) async fn recv_response<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> Result<Response> {
    let version = recv_version(recv_stream.get_mut()).await?;
    let body = match recv_stream.next().await {
        Some(body) => body?.freeze(),
        None => anyhow::bail!("unexpected EOF"),
    };
    Ok(Response { version, body })
}

async fn send_version<T: AsyncWrite + Unpin>(send_stream: &mut T, version: Version) -> Result<()> {
    let mut buffer: [u8; 8] = [0; 8];
    buffer[0..=4].copy_from_slice(MAGIC);
    buffer[5..=6].copy_from_slice(&version.to_u16().to_be_bytes());
    send_stream.write_all(&buffer).await.map_err(Into::into)
}

async fn recv_version<T: AsyncRead + Unpin>(recv_stream: &mut T) -> Result<Version> {
    let mut buffer: [u8; 8] = [0; 8];
    recv_stream.read_exact(&mut buffer).await?;
    anyhow::ensure!(
        &buffer[0..=4] == MAGIC && buffer[7] == 0,
        "invalid protocol header"
    );
    Version::try_from(u16::from_be_bytes([buffer[5], buffer[6]]))
}

const MAGIC: &[u8; 5] = b"tycho";
