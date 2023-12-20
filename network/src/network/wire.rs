use anyhow::Result;
use bytes::Bytes;
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::config::Config;
use crate::types::{Direction, Request, Response, Version};

pub fn make_codec(config: &Config) -> LengthDelimitedCodec {
    let mut builder = LengthDelimitedCodec::builder();

    if let Some(max_frame_size) = config.max_frame_size {
        builder.max_frame_length(max_frame_size);
    }

    builder.length_field_length(4).big_endian().new_codec()
}

pub async fn handshake(
    connection: crate::connection::Connection,
) -> Result<crate::connection::Connection> {
    match connection.origin() {
        Direction::Inbound => {
            let mut send_stream = connection.open_uni().await?;
            send_version(&mut send_stream, Version::V1).await?;
            send_stream.finish().await?;
        }
        Direction::Outbound => {
            let mut recv_stream = connection.accept_uni().await?;
            recv_version(&mut recv_stream).await?;
        }
    }
    Ok(connection)
}

pub async fn send_request<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    request: Request<Bytes>,
) -> Result<()> {
    send_version(send_stream.get_mut(), request.version).await?;
    send_stream.send(request.body).await?;
    Ok(())
}

pub async fn recv_request<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> Result<Request<Bytes>> {
    let version = recv_version(recv_stream.get_mut()).await?;
    let body = match recv_stream.next().await {
        Some(body) => body?.freeze(),
        None => anyhow::bail!("unexpected EOF"),
    };
    Ok(Request { version, body })
}

pub async fn send_response<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    response: Response<Bytes>,
) -> Result<()> {
    send_version(send_stream.get_mut(), response.version).await?;
    send_stream.send(response.body).await?;
    Ok(())
}

pub async fn recv_response<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> Result<Response<Bytes>> {
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
