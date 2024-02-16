use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tl_proto::{TlRead, TlWrite};

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Address(#[serde(with = "tycho_util::serde_helpers::socket_addr")] SocketAddr);

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

const ADDRESS_V4_TL_ID: u32 = tl_proto::id!("transport.address.ipv4", scheme = "proto.tl");
const ADDRESS_V6_TL_ID: u32 = tl_proto::id!("transport.address.ipv6", scheme = "proto.tl");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        const SOME_ADDR_V4: &str = "101.102.103.104:12345";
        const SOME_ADDR_V6: &str = "[2345:0425:2CA1:0:0:0567:5673:23b5]:12345";

        for addr in [SOME_ADDR_V4, SOME_ADDR_V6] {
            let from_json: Address = serde_json::from_str(&format!("\"{addr}\"")).unwrap();
            let from_str = Address::from_str(addr).unwrap();
            assert_eq!(from_json, from_str);

            let to_json = serde_json::to_string(&from_json).unwrap();
            let from_json: Address = serde_json::from_str(&to_json).unwrap();
            assert_eq!(from_json, from_str);
        }
    }
}
