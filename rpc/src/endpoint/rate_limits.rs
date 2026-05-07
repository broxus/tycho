use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tycho_util::FastDashMap;
use tycho_util::rate_limit::{
    RateLimitConfig, RateLimitPolicy, RateLimitVerdict, RateLimiter, TokenBucketConfig,
};

#[derive(Debug, Default, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcRateLimitsConfig {
    pub limiter: RateLimitConfig,
    pub traffic: RpcTrafficLimits,
}

impl From<RpcRateLimitsConfig> for RpcRateLimiter {
    fn from(config: RpcRateLimitsConfig) -> Self {
        RpcRateLimiter {
            limiter: RateLimiter::new(config.limiter),
            traffic: config.traffic,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcTrafficLimits {
    pub requests: TokenBucketConfig,
    pub streams: TokenBucketConfig,
}

impl Default for RpcTrafficLimits {
    fn default() -> Self {
        Self {
            requests: TokenBucketConfig::new(
                NonZeroU32::new(10).unwrap(),
                NonZeroU32::new(10).unwrap(),
            ),
            streams: TokenBucketConfig::new(
                NonZeroU32::new(5).unwrap(),
                NonZeroU32::new(5).unwrap(),
            ),
        }
    }
}

impl RpcTrafficLimits {
    fn policy(&self, class: RpcTrafficClass) -> RateLimitPolicy<RpcTrafficClass> {
        let bucket = match class {
            RpcTrafficClass::Request => self.requests,
            RpcTrafficClass::Stream => self.streams,
        };

        RateLimitPolicy { class, bucket }
    }
}

#[derive(Clone)]
pub struct RpcRateLimiter {
    limiter: RateLimiter<IpAddr, RpcTrafficClass>,
    traffic: RpcTrafficLimits,
}

impl RpcRateLimiter {
    fn check(&self, ip: IpAddr, class: RpcTrafficClass) -> RateLimitVerdict {
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
    max_streams_per_addr: NonZeroU32,
}

impl ActiveStreamLimiter {
    pub fn new(max_streams_per_addr: u32) -> Self {
        Self {
            active: Arc::new(FastDashMap::default()),
            max_streams_per_addr: NonZeroU32::new(max_streams_per_addr).unwrap(),
        }
    }

    pub fn try_acquire(&self, ip: IpAddr) -> Option<ActiveStreamGuard> {
        let mut entry = self.active.entry(ip).or_default();

        if *entry >= self.max_streams_per_addr.get() {
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

pub struct ActiveStreamGuard {
    ip: IpAddr,
    active: ActiveStreamLimiter,
}

impl ActiveStreamGuard {
    fn new(ip: IpAddr, active: ActiveStreamLimiter) -> Self {
        Self { ip, active }
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.active.release(self.ip);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RpcTrafficClass {
    Request,
    Stream,
}

pub async fn rate_limit(
    axum::extract::State(limiter): axum::extract::State<RpcRateLimiter>,
    req: Request,
    next: Next,
) -> Response {
    let Some(ip) = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
    else {
        return next.run(req).await;
    };

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
