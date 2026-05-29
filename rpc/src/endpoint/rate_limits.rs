use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::extract::Request;
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_client_ip::ClientIp;
use serde::{Deserialize, Serialize};
use tycho_util::rate_limit::{
    RateLimitConfig, RateLimitPolicy, RateLimitVerdict, RateLimiter, TrafficLimit,
};
use tycho_util::{FastDashMap, FastHashSet};

use crate::util::ip::normalize_ip;

#[derive(Debug, Default, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcRateLimitsConfig {
    pub limiter: RateLimitConfig,
    pub traffic: RpcTrafficLimits,
    pub whitelist: Vec<IpAddr>,
}

impl From<RpcRateLimitsConfig> for RpcRateLimiter {
    fn from(config: RpcRateLimitsConfig) -> Self {
        RpcRateLimiter {
            limiter: RateLimiter::new(config.limiter),
            traffic: config.traffic,
            whitelist: config.whitelist.into_iter().map(normalize_ip).collect(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcTrafficLimits {
    pub requests: TrafficLimit,
    pub streams: TrafficLimit,
}

impl Default for RpcTrafficLimits {
    fn default() -> Self {
        Self {
            requests: TrafficLimit::new(NonZeroU32::new(10).unwrap(), NonZeroU32::new(10).unwrap()),
            streams: TrafficLimit::new(NonZeroU32::new(5).unwrap(), NonZeroU32::new(5).unwrap()),
        }
    }
}

impl RpcTrafficLimits {
    fn policy(&self, class: RpcTrafficClass) -> RateLimitPolicy<RpcTrafficClass> {
        let limit = match class {
            RpcTrafficClass::Request => self.requests,
            RpcTrafficClass::Stream => self.streams,
        };

        RateLimitPolicy { class, limit }
    }
}

#[derive(Clone)]
pub struct RpcRateLimiter {
    limiter: RateLimiter<IpAddr, RpcTrafficClass>,
    traffic: RpcTrafficLimits,
    whitelist: FastHashSet<IpAddr>,
}

impl RpcRateLimiter {
    fn check(&self, ip: IpAddr, class: RpcTrafficClass) -> RateLimitVerdict {
        let ip = normalize_ip(ip);
        if self.whitelist.contains(&ip) {
            return RateLimitVerdict::Allow;
        }

        self.limiter.check(&ip, self.traffic.policy(class))
    }

    fn classify_request(req: &Request) -> Option<RpcTrafficClass> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/stream") => Some(RpcTrafficClass::Stream),
            (&Method::POST, _) => Some(RpcTrafficClass::Request),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ActiveStreamLimiter {
    active: Arc<FastDashMap<IpAddr, u32>>,
    max_streams_per_addr: Option<NonZeroU32>,
    whitelist: Arc<FastHashSet<IpAddr>>,
}

impl ActiveStreamLimiter {
    pub fn new(max_streams_per_addr: Option<NonZeroU32>, whitelist: Vec<IpAddr>) -> Self {
        Self {
            active: Arc::new(FastDashMap::default()),
            max_streams_per_addr,
            whitelist: Arc::new(whitelist.into_iter().map(normalize_ip).collect()),
        }
    }

    pub fn try_acquire(&self, ip: IpAddr) -> Option<ActiveStreamGuard> {
        let Some(max_streams_per_addr) = self.max_streams_per_addr else {
            return Some(ActiveStreamGuard::whitelisted());
        };

        let ip = normalize_ip(ip);
        if self.whitelist.contains(&ip) {
            return Some(ActiveStreamGuard::whitelisted());
        }

        let mut entry = self.active.entry(ip).or_default();

        if *entry >= max_streams_per_addr.get() {
            return None;
        }

        *entry += 1;

        Some(ActiveStreamGuard::new(ip, self.clone()))
    }

    fn release(&self, ip: IpAddr) {
        self.active.remove_if_mut(&ip, |_, count| {
            *count = count.saturating_sub(1);
            *count == 0
        });
    }
}

pub enum ActiveStreamGuard {
    Counted {
        ip: IpAddr,
        active: ActiveStreamLimiter,
    },
    Whitelisted,
}

impl ActiveStreamGuard {
    fn new(ip: IpAddr, active: ActiveStreamLimiter) -> Self {
        Self::Counted { ip, active }
    }

    fn whitelisted() -> Self {
        Self::Whitelisted
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        if let Self::Counted { ip, active } = self {
            active.release(*ip);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RpcTrafficClass {
    Request,
    Stream,
}

pub async fn rate_limit(
    axum::extract::State(limiter): axum::extract::State<RpcRateLimiter>,
    ClientIp(ip): ClientIp,
    req: Request,
    next: Next,
) -> Response {
    let Some(class) = RpcRateLimiter::classify_request(&req) else {
        return next.run(req).await;
    };

    match limiter.check(ip, class) {
        RateLimitVerdict::Allow => next.run(req).await,
        RateLimitVerdict::Reject { retry_after } => {
            // Round up to whole seconds.
            let retry_after = retry_after.as_millis().div_ceil(1_000).max(1);

            (StatusCode::TOO_MANY_REQUESTS, [(
                header::RETRY_AFTER,
                retry_after.to_string(),
            )])
                .into_response()
        }
    }
}
