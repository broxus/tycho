use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tl_proto::{TlRead, TlWrite};
use tycho_util::serde_helpers::StrVisitor;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Address {
    Ip { ip: IpAddr, port: u16 },
    Dns { hostname: Arc<str>, port: u16 },
}

impl Address {
    pub fn new_ip<T: Into<SocketAddr>>(addr: T) -> Self {
        let addr: SocketAddr = addr.into();
        Self::Ip {
            ip: addr.ip(),
            port: addr.port(),
        }
    }

    pub fn new_dns<T: Into<String>>(hostname: T, port: u16) -> Self {
        let hostname: String = hostname.into();
        Self::Dns {
            hostname: Arc::from(hostname),
            port,
        }
    }

    pub fn port(&self) -> u16 {
        match self {
            Self::Ip { port, .. } | Self::Dns { port, .. } => *port,
        }
    }

    pub fn set_port(&mut self, port: u16) {
        match self {
            Self::Ip { port: p, .. } | Self::Dns { port: p, .. } => *p = port,
        }
    }

    pub async fn resolve(&self) -> std::io::Result<SocketAddr> {
        match self {
            Self::Ip { ip, port } => Ok(SocketAddr::new(*ip, *port)),
            Self::Dns { hostname, port } => {
                let mut iter = tokio::net::lookup_host((hostname.as_ref(), *port)).await?;
                iter.next().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "unable to resolve host")
                })
            }
        }
    }
}

impl Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        enum Address<'a> {
            Ip(SocketAddr),
            Dns { hostname: &'a str, port: u16 },
        }

        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            match self {
                Self::Ip { ip, port } => Address::Ip(SocketAddr::new(*ip, *port)),
                Self::Dns { hostname, port } => Address::Dns {
                    hostname: hostname.as_ref(),
                    port: *port,
                },
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum Address {
            Ip(SocketAddr),
            Dns { hostname: String, port: u16 },
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(StrVisitor::new())
        } else {
            let addr = Address::deserialize(deserializer)?;
            Ok(match addr {
                Address::Ip(addr) => Self::Ip {
                    ip: addr.ip(),
                    port: addr.port(),
                },
                Address::Dns { hostname, port } => Self::Dns {
                    hostname: hostname.into(),
                    port,
                },
            })
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ip { ip, port } => std::fmt::Display::fmt(&SocketAddr::new(*ip, *port), f),
            Self::Dns { hostname, port } => write!(f, "{}:{port}", hostname.as_ref()),
        }
    }
}

impl std::net::ToSocketAddrs for Address {
    type Iter = std::option::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        match self {
            Self::Ip { ip, port } => (*ip, *port).to_socket_addrs(),
            Self::Dns { hostname, port } => {
                let resolved = (hostname.as_ref(), *port).to_socket_addrs()?;
                Ok(resolved.into_iter().next().into_iter())
            }
        }
    }
}

impl TlWrite for Address {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        let len = match self {
            Self::Ip {
                ip: IpAddr::V4(_), ..
            } => 4,
            Self::Ip {
                ip: IpAddr::V6(_), ..
            } => 16,
            Self::Dns { hostname: host, .. } => host.as_bytes().max_size_hint(),
        };
        // Constructor + len + port
        4 + len + 4
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: tl_proto::TlPacket,
    {
        match self {
            Self::Ip {
                ip: IpAddr::V4(ip),
                port,
            } => {
                packet.write_u32(ADDRESS_V4_TL_ID);
                packet.write_u32(u32::from(*ip));
                packet.write_u32(*port as u32);
            }
            Self::Ip {
                ip: IpAddr::V6(ip),
                port,
            } => {
                packet.write_u32(ADDRESS_V6_TL_ID);
                packet.write_raw_slice(&ip.octets());
                packet.write_u32(*port as u32);
            }
            Self::Dns {
                hostname: host,
                port,
            } => {
                packet.write_u32(ADDRESS_DNS_TL_ID);
                host.as_bytes().write_to(packet);
                packet.write_u32(*port as u32);
            }
        };
    }
}

impl<'a> TlRead<'a> for Address {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &mut &'a [u8]) -> tl_proto::TlResult<Self> {
        use tl_proto::TlError;

        fn read_port(packet: &mut &[u8]) -> tl_proto::TlResult<u16> {
            u32::read_from(packet)?
                .try_into()
                .map_err(|_e| TlError::InvalidData)
        }

        Ok(match u32::read_from(packet)? {
            ADDRESS_V4_TL_ID => {
                let ip = u32::read_from(packet)?;
                let port = read_port(packet)?;
                Self::Ip {
                    ip: IpAddr::V4(ip.into()),
                    port,
                }
            }
            ADDRESS_V6_TL_ID => {
                let octets = <[u8; 16]>::read_from(packet)?;
                let port = read_port(packet)?;
                Self::Ip {
                    ip: IpAddr::V6(octets.into()),
                    port,
                }
            }
            ADDRESS_DNS_TL_ID => {
                let hostname = <&[u8]>::read_from(packet)?;
                let Some(hostname) = validate_hostname(hostname) else {
                    return Err(TlError::InvalidData);
                };
                let port = read_port(packet)?;

                if hostname.parse::<IpAddr>().is_ok() {
                    return Err(TlError::InvalidData);
                }

                Self::Dns {
                    hostname: hostname.into(),
                    port,
                }
            }
            _ => return Err(TlError::UnknownConstructor),
        })
    }
}

impl From<SocketAddr> for Address {
    #[inline]
    fn from(value: SocketAddr) -> Self {
        Self::new_ip(value)
    }
}

impl From<SocketAddrV4> for Address {
    #[inline]
    fn from(value: SocketAddrV4) -> Self {
        Self::new_ip(value)
    }
}

impl From<SocketAddrV6> for Address {
    #[inline]
    fn from(value: SocketAddrV6) -> Self {
        Self::new_ip(value)
    }
}

impl From<(std::net::Ipv4Addr, u16)> for Address {
    #[inline]
    fn from((ip, port): (std::net::Ipv4Addr, u16)) -> Self {
        Self::Ip {
            ip: IpAddr::V4(ip),
            port,
        }
    }
}

impl From<(std::net::Ipv6Addr, u16)> for Address {
    #[inline]
    fn from((ip, port): (std::net::Ipv6Addr, u16)) -> Self {
        Self::Ip {
            ip: IpAddr::V6(ip),
            port,
        }
    }
}

impl FromStr for Address {
    type Err = std::net::AddrParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match SocketAddr::from_str(s) {
            Ok(addr) => Ok(Self::new_ip(addr)),
            Err(e) => {
                'host: {
                    let Some((hostname, port)) = s.split_once(':') else {
                        break 'host;
                    };

                    let Ok(port) = port.parse::<u16>() else {
                        break 'host;
                    };

                    let Some(hostname) = validate_hostname(hostname.as_bytes()) else {
                        break 'host;
                    };

                    return Ok(Self::Dns {
                        hostname: hostname.into(),
                        port,
                    });
                }

                Err(e)
            }
        }
    }
}

/// Validates a hostname according to [IETF RFC 1123](https://tools.ietf.org/html/rfc1123).
///
/// A hostname is valid if the following conditions are true:
///
/// - It does not start or end with `-` or `.`.
/// - It does not contain any characters outside of the alphanumeric range, except for `-` and `.`.
/// - It is not empty.
/// - It is 253 or fewer characters.
/// - Its labels (characters separated by `.`) are not empty.
/// - Its labels are 63 or fewer characters.
/// - Its labels do not start or end with '-' or '.'.
fn validate_hostname(hostname: &[u8]) -> Option<&str> {
    if hostname.is_empty() || hostname.len() > 253 {
        return None;
    }

    let mut label_length = 0;
    let mut previous_char = b'.'; // assume the previous character is a dot

    for &byte in hostname {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => {
                label_length += 1;
            }
            b'-' => {
                if label_length == 0 {
                    return None; // invalid label
                }
                label_length += 1;
            }
            b'.' => {
                if label_length == 0 || previous_char == b'-' {
                    return None; // invalid label
                }
                label_length = 0; // reset label length after each dot
            }
            _ => return None,
        }

        if label_length > 63 {
            return None; // invalid label
        }

        previous_char = byte;
    }

    if label_length == 0 || previous_char == b'-' {
        return None;
    }

    // SAFETY: `hostname` is guaranteed to contain only valid UTF-8 characters.
    Some(unsafe { std::str::from_utf8_unchecked(hostname) })
}

const ADDRESS_V4_TL_ID: u32 = tl_proto::id!("transport.address.ipv4", scheme = "proto.tl");
const ADDRESS_V6_TL_ID: u32 = tl_proto::id!("transport.address.ipv6", scheme = "proto.tl");
const ADDRESS_DNS_TL_ID: u32 = tl_proto::id!("transport.address.dns", scheme = "proto.tl");

#[cfg(test)]
mod tests {
    use super::*;

    const SOME_ADDR_V4: &str = "101.102.103.104:12345";
    const SOME_ADDR_V6: &str = "[2345:0425:2CA1:0:0:0567:5673:23b5]:12345";
    const SOME_ADDR_DNS: &str = "node-1.example.com:12345";

    #[test]
    fn serde() {
        for addr in [SOME_ADDR_V4, SOME_ADDR_V6, SOME_ADDR_DNS] {
            let from_json: Address = serde_json::from_str(&format!("\"{addr}\"")).unwrap();
            let from_str = Address::from_str(addr).unwrap();
            assert_eq!(from_json, from_str);

            let to_json = serde_json::to_string(&from_json).unwrap();
            let from_json: Address = serde_json::from_str(&to_json).unwrap();
            assert_eq!(from_json, from_str);
        }
    }

    #[test]
    fn tl() {
        // Valid
        let addrs = [
            Address::from_str(SOME_ADDR_V4).unwrap(),
            Address::from_str(SOME_ADDR_V6).unwrap(),
            Address::new_dns("node-1.example.com", 12345),
        ];

        for addr in addrs {
            let bytes = tl_proto::serialize(&addr);
            let parsed = tl_proto::deserialize::<Address>(&bytes).unwrap();
            assert_eq!(addr, parsed);
        }

        // Invalid
        let addrs = [
            Address::new_dns("test.com:12345", 12345),
            Address::new_dns("", 12345),
            Address::new_dns("...", 12345),
            Address::new_dns("127.0.0.1", 12345),
            Address::new_dns(SOME_ADDR_V6, 12345),
        ];

        for addr in addrs {
            assert!(matches!(
                tl_proto::deserialize::<Address>(&tl_proto::serialize(addr)),
                Err(tl_proto::TlError::InvalidData)
            ));
        }
    }

    #[test]
    fn valid_hostnames() {
        for hostname in &[
            "VaLiD-HoStNaMe",
            "50-name",
            "235235",
            "example.com",
            "VaLid.HoStNaMe",
            "123.456",
        ] {
            assert!(
                validate_hostname(hostname.as_bytes()).is_some(),
                "{hostname} is not valid"
            );
        }
    }

    #[test]
    fn invalid_hostnames() {
        for hostname in &[
            "-invalid-name",
            "also-invalid-",
            "asdf@fasd",
            "@asdfl",
            "asd f@",
            ".invalid",
            "invalid.name.",
            "foo.label-is-way-to-longgggggggggggggggggggggggggggggggggggggggggggg.org",
            "invalid.-starting.char",
            "invalid.ending-.char",
            "empty..label",
        ] {
            assert!(
                validate_hostname(hostname.as_bytes()).is_none(),
                "{hostname} should not be valid"
            );
        }
    }
}
