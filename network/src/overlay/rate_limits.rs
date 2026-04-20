use std::hash::Hash;
use std::sync::Arc;

use tycho_util::rate_limit::{RateLimitConfig, RateLimitPolicy, RateLimitVerdict, RateLimiter};

use crate::types::{PeerId, ServiceRequest};

pub enum OverlayIngressPolicyDecision<C> {
    Allow(RateLimitPolicy<C>),
    Bypass,
    Drop,
}

pub trait PublicOverlayRateLimitPolicy: Send + Sync + 'static {
    type Class: Copy + Eq + Hash + Send + Sync + 'static;

    fn classify_query(&self, req: &ServiceRequest) -> OverlayIngressPolicyDecision<Self::Class>;

    fn classify_message(&self, req: &ServiceRequest) -> OverlayIngressPolicyDecision<Self::Class>;
}

trait PublicOverlayRateLimitHandler: Send + Sync + 'static {
    fn allow_query(&self, req: &ServiceRequest) -> bool;

    fn allow_message(&self, req: &ServiceRequest) -> bool;
}

#[derive(Clone)]
pub struct PublicOverlayRateLimiter {
    inner: Arc<dyn PublicOverlayRateLimitHandler>,
}

struct PolicyRateLimiter<P>
where
    P: PublicOverlayRateLimitPolicy,
{
    limiter: RateLimiter<PeerId, P::Class>,
    policy: P,
}

impl PublicOverlayRateLimiter {
    pub fn new<P>(config: RateLimitConfig, policy: P) -> Self
    where
        P: PublicOverlayRateLimitPolicy,
    {
        Self {
            inner: Arc::new(PolicyRateLimiter {
                limiter: RateLimiter::new(config),
                policy,
            }),
        }
    }

    pub(crate) fn allow_query(&self, req: &ServiceRequest) -> bool {
        self.inner.allow_query(req)
    }

    pub(crate) fn allow_message(&self, req: &ServiceRequest) -> bool {
        self.inner.allow_message(req)
    }
}

impl<P> PublicOverlayRateLimitHandler for PolicyRateLimiter<P>
where
    P: PublicOverlayRateLimitPolicy,
{
    fn allow_query(&self, req: &ServiceRequest) -> bool {
        self.check(&req.metadata.peer_id, self.policy.classify_query(req))
    }

    fn allow_message(&self, req: &ServiceRequest) -> bool {
        self.check(&req.metadata.peer_id, self.policy.classify_message(req))
    }
}

impl<P> PolicyRateLimiter<P>
where
    P: PublicOverlayRateLimitPolicy,
{
    fn check(&self, peer_id: &PeerId, decision: OverlayIngressPolicyDecision<P::Class>) -> bool {
        match decision {
            OverlayIngressPolicyDecision::Allow(policy) => {
                matches!(self.limiter.check(peer_id, policy), RateLimitVerdict::Allow)
            }
            OverlayIngressPolicyDecision::Bypass => true,
            OverlayIngressPolicyDecision::Drop => false,
        }
    }
}
