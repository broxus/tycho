use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.ping", scheme = "proto.tl")]
pub struct Ping;

#[derive(Debug, Clone, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.pong", scheme = "proto.tl")]
pub struct Pong;

/// A universal response for the all queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response<T> {
    Ok(T),
    Err(u32),
}

impl<T> Response<T> {
    const OK_ID: u32 = tl_proto::id!("overlay.response.ok", scheme = "proto.tl");
    const ERR_ID: u32 = tl_proto::id!("overlay.response.err", scheme = "proto.tl");
}

impl<T> TlWrite for Response<T>
where
    T: TlWrite,
{
    type Repr = T::Repr;

    #[inline(always)]
    fn max_size_hint(&self) -> usize {
        4 + match self {
            Self::Ok(data) => data.max_size_hint(),
            Self::Err(code) => code.max_size_hint(),
        }
    }

    #[inline(always)]
    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        match self {
            Self::Ok(data) => {
                packet.write_u32(Self::OK_ID);
                data.write_to(packet);
            }
            Self::Err(code) => {
                packet.write_u32(Self::ERR_ID);
                packet.write_u32(*code);
            }
        }
    }
}

impl<'a, T> TlRead<'a> for Response<T>
where
    T: TlRead<'a>,
{
    type Repr = T::Repr;

    #[inline(always)]
    fn read_from(packet: &mut &'a [u8]) -> TlResult<Self> {
        Ok(match u32::read_from(packet)? {
            Self::OK_ID => Response::Ok(T::read_from(packet)?),
            Self::ERR_ID => Response::Err(u32::read_from(packet)?),
            _ => return Err(TlError::UnknownConstructor),
        })
    }
}

/// Message broadcast prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, TlRead, TlWrite)]
#[tl(boxed, id = "overlay.broadcastPrefix", scheme = "proto.tl")]
pub struct BroadcastPrefix;
