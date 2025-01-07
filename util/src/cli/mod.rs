use std::net::IpAddr;

pub mod config;
pub mod logger;
pub mod signal;

pub mod metrics;

pub async fn resolve_public_ip(ip: Option<IpAddr>) -> anyhow::Result<IpAddr> {
    match ip {
        Some(address) => Ok(address),
        None => match getip::addr_v4().await {
            Ok(address) => Ok(IpAddr::V4(address)),
            Err(e) => anyhow::bail!("failed to resolve public IP address: {e:?}"),
        },
    }
}
