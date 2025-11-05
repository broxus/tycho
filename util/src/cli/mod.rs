use std::net::IpAddr;
pub mod config;
pub mod logger;
pub mod signal;
pub mod metrics;
pub async fn resolve_public_ip(ip: Option<IpAddr>) -> anyhow::Result<IpAddr> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(resolve_public_ip)),
        file!(),
        9u32,
    );
    let ip = ip;
    match ip {
        Some(address) => Ok(address),
        None => {
            match {
                __guard.end_section(12u32);
                let __result = getip::addr_v4().await;
                __guard.start_section(12u32);
                __result
            } {
                Ok(address) => Ok(IpAddr::V4(address)),
                Err(e) => anyhow::bail!("failed to resolve public IP address: {e:?}"),
            }
        }
    }
}
