use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tycho_util::FastDashSet;
use tycho_util::rate_limit::{
    RateLimitConfig, RateLimitPolicy, RateLimitVerdict, RateLimiter, TokenBucketConfig,
};

#[derive(Debug, Default, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RpcRateLimitsConfig {
    limiter: RateLimitConfig,
    traffic: RpcTrafficLimits,
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
    requests: TokenBucketConfig,
    streams: TokenBucketConfig,
}

impl Default for RpcTrafficLimits {
    fn default() -> Self {
        Self {
            requests: TokenBucketConfig::new(
                NonZeroU32::new(5).unwrap(),
                NonZeroU32::new(5).unwrap(),
            ),
            streams: TokenBucketConfig::new(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
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

#[derive(Clone, Default)]
pub struct ActiveStreamLimiter {
    active: Arc<FastDashSet<IpAddr>>,
}

impl ActiveStreamLimiter {
    pub fn try_acquire(&self, ip: IpAddr) -> Option<ActiveStreamGuard> {
        self.active
            .insert(ip)
            .then(|| ActiveStreamGuard::new(ip, self.active.clone()))
    }
}

pub struct ActiveStreamGuard {
    ip: IpAddr,
    active: Arc<FastDashSet<IpAddr>>,
}

impl ActiveStreamGuard {
    fn new(ip: IpAddr, active: Arc<FastDashSet<IpAddr>>) -> Self {
        Self { ip, active }
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.active.remove(&self.ip);
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
        RateLimitVerdict::Reject => StatusCode::TOO_MANY_REQUESTS.into_response(),
    }
}
