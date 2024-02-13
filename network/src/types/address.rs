use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;

use tl_proto::{TlRead, TlWrite};

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Address(SocketAddr);

impl Address {
    pub fn resolve(&self) -> std::io::Result<SocketAddr> {
        std::net::ToSocketAddrs::to_socket_addrs(&self).and_then(|mut iter| {
            iter.next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "unable to resolve host")
            })
        })
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::net::ToSocketAddrs for Address {
    type Iter = <SocketAddr as std::net::ToSocketAddrs>::Iter;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        self.0.to_socket_addrs()
    }
}

impl TlWrite for Address {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + match &self.0 {
            SocketAddr::V4(_) => 4 + 4,
            SocketAddr::V6(_) => 16 + 4,
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match &self.0 {
            SocketAddr::V4(addr) => {
                packet.write_u32(ADDRESS_V4_TL_ID);
                packet.write_u32(u32::from(*addr.ip()));
                packet.write_u32(addr.port() as u32);
            }
            SocketAddr::V6(addr) => {
                packet.write_u32(ADDRESS_V6_TL_ID);
                packet.write_raw_slice(&addr.ip().octets());
                packet.write_u32(addr.port() as u32);
            }
        };
    }
}

impl<'a> TlRead<'a> for Address {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        use tl_proto::TlError;

        Ok(Address(match u32::read_from(packet, offset)? {
            ADDRESS_V4_TL_ID => {
                let ip = u32::read_from(packet, offset)?;
                let Ok(port) = u32::read_from(packet, offset)?.try_into() else {
                    return Err(TlError::InvalidData);
                };
                SocketAddr::V4(SocketAddrV4::new(ip.into(), port))
            }
            ADDRESS_V6_TL_ID => {
                let octets = <[u8; 16]>::read_from(packet, offset)?;
                let Ok(port) = u32::read_from(packet, offset)?.try_into() else {
                    return Err(TlError::InvalidData);
                };
                SocketAddr::V6(SocketAddrV6::new(octets.into(), port, 0, 0))
            }
            _ => return Err(TlError::UnknownConstructor),
        }))
    }
}

impl From<SocketAddr> for Address {
    #[inline]
    fn from(value: SocketAddr) -> Self {
        Self(value)
    }
}

impl From<SocketAddrV4> for Address {
    #[inline]
    fn from(value: SocketAddrV4) -> Self {
        Self(SocketAddr::V4(value))
    }
}

impl From<SocketAddrV6> for Address {
    #[inline]
    fn from(value: SocketAddrV6) -> Self {
        Self(SocketAddr::V6(value))
    }
}

impl From<(std::net::Ipv4Addr, u16)> for Address {
    #[inline]
    fn from((ip, port): (std::net::Ipv4Addr, u16)) -> Self {
        Self(SocketAddr::V4(SocketAddrV4::new(ip, port)))
    }
}

impl From<(std::net::Ipv6Addr, u16)> for Address {
    #[inline]
    fn from((ip, port): (std::net::Ipv6Addr, u16)) -> Self {
        Self(SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0)))
    }
}

impl FromStr for Address {
    type Err = std::net::AddrParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SocketAddr::from_str(s).map(Self)
    }
}

#[derive(Debug, Clone, TlWrite, Eq, PartialEq)]
pub struct AddressList {
    pub items: Vec<Address>,
    pub created_at: u32,
    pub expires_at: u32,
}

impl AddressList {
    pub const MAX_LEN: usize = 4;
}

impl<'a> TlRead<'a> for AddressList {
    type Repr = tl_proto::Bare;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        use tl_proto::TlError;

        let len = u32::read_from(packet, offset)? as usize;
        if len == 0 || len > Self::MAX_LEN {
            return Err(TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(Address::read_from(packet, offset)?);
        }

        Ok(Self {
            items,
            created_at: u32::read_from(packet, offset)?,
            expires_at: u32::read_from(packet, offset)?,
        })
    }
}

const ADDRESS_V4_TL_ID: u32 = tl_proto::id!("transport.address.ipv4", scheme = "proto.tl");
const ADDRESS_V6_TL_ID: u32 = tl_proto::id!("transport.address.ipv6", scheme = "proto.tl");
