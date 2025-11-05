use anyhow::Result;
use futures_util::StreamExt;
use futures_util::sink::SinkExt;
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
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(handshake)),
        file!(),
        19u32,
    );
    let connection = connection;
    match connection.origin() {
        Direction::Inbound => {
            let mut send_stream = {
                __guard.end_section(24u32);
                let __result = connection.open_uni().await;
                __guard.start_section(24u32);
                __result
            }
                .map_err(HandshakeError::ConnectionFailed)?;
            {
                __guard.end_section(28u32);
                let __result = send_version(&mut send_stream, Version::V1).await;
                __guard.start_section(28u32);
                __result
            }
                .map_err(HandshakeError::WireError)?;
            _ = send_stream.finish();
            match {
                __guard.end_section(34u32);
                let __result = send_stream.stopped().await;
                __guard.start_section(34u32);
                __result
            } {
                Ok(_) => Ok(()),
                Err(quinn::StoppedError::ConnectionLost(e)) => {
                    Err(HandshakeError::ConnectionFailed(e))
                }
                Err(quinn::StoppedError::ZeroRttRejected) => {
                    Err(
                        HandshakeError::WireError(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                WireError::ZeroRttRejected,
                            ),
                        ),
                    )
                }
            }
        }
        Direction::Outbound => {
            let mut recv_stream = {
                __guard.end_section(50u32);
                let __result = connection.accept_uni().await;
                __guard.start_section(50u32);
                __result
            }
                .map_err(HandshakeError::ConnectionFailed)?;
            match {
                __guard.end_section(53u32);
                let __result = recv_version(&mut recv_stream).await;
                __guard.start_section(53u32);
                __result
            } {
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
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(send_request)),
        file!(),
        72u32,
    );
    let send_stream = send_stream;
    let request = request;
    {
        __guard.end_section(73u32);
        let __result = send_version(send_stream.get_mut(), request.version).await;
        __guard.start_section(73u32);
        __result
    }?;
    {
        __guard.end_section(74u32);
        let __result = send_stream.send(request.body).await;
        __guard.start_section(74u32);
        __result
    }
}
pub(crate) async fn recv_request<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> std::io::Result<Request> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(recv_request)),
        file!(),
        79u32,
    );
    let recv_stream = recv_stream;
    let version = {
        __guard.end_section(80u32);
        let __result = recv_version(recv_stream.get_mut()).await;
        __guard.start_section(80u32);
        __result
    }?;
    match {
        __guard.end_section(81u32);
        let __result = recv_stream.next().await;
        __guard.start_section(81u32);
        __result
    } {
        Some(body) => {
            Ok(Request {
                version,
                body: body?.freeze(),
            })
        }
        None => {
            Err(
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    WireError::UnexpectedEof,
                ),
            )
        }
    }
}
pub(crate) async fn send_response<T: AsyncWrite + Unpin>(
    send_stream: &mut FramedWrite<T, LengthDelimitedCodec>,
    response: Response,
) -> std::io::Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(send_response)),
        file!(),
        96u32,
    );
    let send_stream = send_stream;
    let response = response;
    {
        __guard.end_section(97u32);
        let __result = send_version(send_stream.get_mut(), response.version).await;
        __guard.start_section(97u32);
        __result
    }?;
    {
        __guard.end_section(98u32);
        let __result = send_stream.send(response.body).await;
        __guard.start_section(98u32);
        __result
    }
}
pub(crate) async fn recv_response<T: AsyncRead + Unpin>(
    recv_stream: &mut FramedRead<T, LengthDelimitedCodec>,
) -> std::io::Result<Response> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(recv_response)),
        file!(),
        103u32,
    );
    let recv_stream = recv_stream;
    let version = {
        __guard.end_section(104u32);
        let __result = recv_version(recv_stream.get_mut()).await;
        __guard.start_section(104u32);
        __result
    }?;
    match {
        __guard.end_section(105u32);
        let __result = recv_stream.next().await;
        __guard.start_section(105u32);
        __result
    } {
        Some(body) => {
            Ok(Response {
                version,
                body: body?.freeze(),
            })
        }
        None => {
            Err(
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    WireError::UnexpectedEof,
                ),
            )
        }
    }
}
async fn send_version<T: AsyncWrite + Unpin>(
    send_stream: &mut T,
    version: Version,
) -> std::io::Result<()> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(send_version)),
        file!(),
        120u32,
    );
    let send_stream = send_stream;
    let version = version;
    let mut buffer: [u8; 8] = [0; 8];
    buffer[0..=4].copy_from_slice(MAGIC);
    buffer[5..=6].copy_from_slice(&version.to_u16().to_be_bytes());
    {
        __guard.end_section(124u32);
        let __result = send_stream.write_all(&buffer).await;
        __guard.start_section(124u32);
        __result
    }
}
async fn recv_version<T: AsyncRead + Unpin>(
    recv_stream: &mut T,
) -> std::io::Result<Version> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(recv_version)),
        file!(),
        127u32,
    );
    let recv_stream = recv_stream;
    let mut buffer: [u8; 8] = [0; 8];
    {
        __guard.end_section(129u32);
        let __result = recv_stream.read_exact(&mut buffer).await;
        __guard.start_section(129u32);
        __result
    }?;
    if &buffer[0..=4] != MAGIC || buffer[7] != 0 {
        {
            __guard.end_section(132u32);
            return Err(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    WireError::InvalidHeader,
                ),
            );
        };
    }
    match Version::try_from_u16(u16::from_be_bytes([buffer[5], buffer[6]])) {
        Some(version) => Ok(version),
        None => {
            Err(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    WireError::InvalidVersion,
                ),
            )
        }
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
