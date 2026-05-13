use std::net::IpAddr;

pub fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(ip) => {
            const IPV6_PREFIX_MASK: u128 = u128::MAX << 64;
            IpAddr::V6((u128::from(ip) & IPV6_PREFIX_MASK).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_ipv4() {
        let ip = "192.0.2.1".parse().unwrap();

        assert_eq!(normalize_ip(ip), ip);
    }

    #[test]
    fn normalize_ipv6() {
        let prefix: IpAddr = "2001:db8:abcd:1234::".parse().unwrap();

        assert_eq!(
            normalize_ip("2001:db8:abcd:1234:ffff:ffff:ffff:ffff".parse().unwrap()),
            prefix
        );
    }
}
