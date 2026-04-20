use std::hash::Hash;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{FastDashMap, FastHashMap, serde_helpers};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TokenBucketConfig {
    pub rate_per_sec: NonZeroU32,
    pub burst: NonZeroU32,
}

impl TokenBucketConfig {
    pub const fn new(rate_per_sec: NonZeroU32, burst: NonZeroU32) -> Self {
        Self {
            rate_per_sec,
            burst,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    pub rejects_before_cooldown: u8,
    #[serde(with = "serde_helpers::humantime")]
    pub cooldown: Duration,
    #[serde(with = "serde_helpers::humantime")]
    pub prune_interval: Duration,
    #[serde(with = "serde_helpers::humantime")]
    pub state_ttl: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            rejects_before_cooldown: 5,
            cooldown: Duration::from_secs(30),
            prune_interval: Duration::from_secs(30),
            state_ttl: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitVerdict {
    Allow,
    Reject,
}

#[derive(Debug, Clone, Copy)]
pub struct RateLimitPolicy<C> {
    pub class: C,
    pub bucket: TokenBucketConfig,
}

#[derive(Clone)]
pub struct RateLimiter<K, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    inner: Arc<RateLimiterInner<K, C>>,
}

struct RateLimiterInner<K, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    config: RateLimitConfig,
    states: FastDashMap<K, PeerState<C>>,
    last_prune: Mutex<Instant>,
}

impl<K, C> RateLimiter<K, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            inner: Arc::new(RateLimiterInner {
                config,
                states: FastDashMap::default(),
                last_prune: Mutex::new(Instant::now()),
            }),
        }
    }

    pub fn check(&self, key: &K, policy: RateLimitPolicy<C>) -> RateLimitVerdict {
        self.inner.check(key, policy)
    }
}

impl<K, C> RateLimiterInner<K, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    fn check(&self, key: &K, policy: RateLimitPolicy<C>) -> RateLimitVerdict {
        let now = Instant::now();

        self.maybe_prune(now);

        let mut entry = self
            .states
            .entry(key.clone())
            .or_insert_with(|| PeerState::new(now));

        if now.duration_since(entry.last_seen) >= self.config.state_ttl {
            *entry = PeerState::new(now);
        } else {
            entry.last_seen = now;
        }

        if entry.cooldown_until.is_some_and(|until| now < until) {
            return RateLimitVerdict::Reject;
        }

        let bucket = entry
            .buckets
            .entry(policy.class)
            .or_insert_with(|| TokenBucket::new(now, policy.bucket));

        if !bucket.try_take(now) {
            return Self::register_rejection(&self.config, now, &mut entry);
        }

        entry.rejects = 0;

        RateLimitVerdict::Allow
    }

    fn prune_expired(&self, now: Instant) {
        let state_ttl = self.config.state_ttl;
        self.states.retain(|_, state| {
            state.cooldown_until.is_some_and(|until| until > now)
                || now.duration_since(state.last_seen) < state_ttl
        });
    }

    fn maybe_prune(&self, now: Instant) {
        let mut last_prune = self.last_prune.lock();
        if now.duration_since(*last_prune) < self.config.prune_interval {
            return;
        }
        *last_prune = now;
        drop(last_prune);

        self.prune_expired(now);
    }

    fn register_rejection(
        config: &RateLimitConfig,
        now: Instant,
        state: &mut PeerState<C>,
    ) -> RateLimitVerdict {
        state.last_seen = now;
        state.rejects = state.rejects.saturating_add(1);

        if state.rejects >= config.rejects_before_cooldown {
            state.rejects = 0;
            state.cooldown_until = Some(now + config.cooldown);
        }

        RateLimitVerdict::Reject
    }
}

struct PeerState<C>
where
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    buckets: FastHashMap<C, TokenBucket>,
    rejects: u8,
    cooldown_until: Option<Instant>,
    last_seen: Instant,
}

impl<C> PeerState<C>
where
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    fn new(now: Instant) -> Self {
        Self {
            buckets: FastHashMap::default(),
            rejects: 0,
            cooldown_until: None,
            last_seen: now,
        }
    }
}

struct TokenBucket {
    rate_per_sec: f64,
    burst: f64,
    available: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(now: Instant, config: TokenBucketConfig) -> Self {
        let burst = config.burst.get() as f64;
        Self {
            rate_per_sec: config.rate_per_sec.get() as f64,
            burst,
            available: burst,
            last_refill: now,
        }
    }

    fn try_take(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.available >= 1.0 {
            self.available -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.available = (self.available + elapsed * self.rate_per_sec).min(self.burst);
    }
}
