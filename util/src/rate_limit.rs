use std::hash::Hash;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{FastDashMap, FastHashMap, serde_helpers, time};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficLimit {
    pub rate_per_sec: NonZeroU32,
    pub burst: NonZeroU32,
}

impl TrafficLimit {
    // Millisecond GCRA cannot represent intervals smaller than 1ms.
    pub const MAX_RATE_PER_SEC: u32 = 1_000;

    pub const fn new(rate_per_sec: NonZeroU32, burst: NonZeroU32) -> Self {
        Self {
            rate_per_sec,
            burst,
        }
    }

    fn normalize(&mut self) {
        if self.rate_per_sec.get() > Self::MAX_RATE_PER_SEC {
            self.rate_per_sec = NonZeroU32::new(Self::MAX_RATE_PER_SEC).unwrap();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl RateLimitConfig {
    pub const MIN_REJECTS_BEFORE_COOLDOWN: u8 = 1;
    pub const MAX_REJECTS_BEFORE_COOLDOWN: u8 = u8::MAX - 1;
    pub const MIN_STATE_TTL: Duration = Duration::from_secs(1);
    pub const MIN_PRUNE_INTERVAL: Duration = Duration::from_secs(1);

    fn normalize(&mut self) {
        self.rejects_before_cooldown = self.rejects_before_cooldown.clamp(
            Self::MIN_REJECTS_BEFORE_COOLDOWN,
            Self::MAX_REJECTS_BEFORE_COOLDOWN,
        );

        if self.prune_interval.is_zero() {
            self.prune_interval = Self::MIN_PRUNE_INTERVAL;
        }

        if self.state_ttl.is_zero() {
            self.state_ttl = Self::MIN_STATE_TTL;
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RateLimitVerdict {
    Allow,
    Reject { retry_after: Duration },
}

#[derive(Debug, Clone, Copy)]
pub struct RateLimitPolicy<C> {
    pub class: C,
    pub limit: TrafficLimit,
}

#[derive(Clone)]
pub struct RateLimiter<K, C> {
    inner: Arc<RateLimiterInner<K, C>>,
}

struct RateLimiterInner<K, C> {
    config: RateLimitConfig,
    states: FastDashMap<K, Arc<PeerLimiter<C>>>,
    last_prune_ms: AtomicU64,
}

impl<K, C> RateLimiter<K, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    pub fn new(mut config: RateLimitConfig) -> Self {
        config.normalize();

        Self {
            inner: Arc::new(RateLimiterInner {
                config,
                states: FastDashMap::default(),
                last_prune_ms: AtomicU64::new(time::now_millis()),
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
        let now = time::now_millis();

        self.maybe_prune(now);

        let peer = self.peer(key, now);
        peer.check(&self.config, policy, now)
    }

    fn peer(&self, key: &K, now: u64) -> Arc<PeerLimiter<C>> {
        let state_ttl = self.config.state_ttl.as_millis_u64();

        if let Some(peer) = self.states.get(key)
            && !peer.is_expired(now, state_ttl)
        {
            return peer.clone();
        }

        let mut entry = self
            .states
            .entry(key.clone())
            .or_insert_with(|| Arc::new(PeerLimiter::new(now)));

        if entry.is_expired(now, state_ttl) {
            *entry = Arc::new(PeerLimiter::new(now));
        }

        entry.clone()
    }

    fn prune_expired(&self, now: u64) {
        let state_ttl = self.config.state_ttl.as_millis_u64();

        self.states
            .retain(|_, peer| peer.in_cooldown(now) || !peer.is_expired(now, state_ttl));
    }

    fn maybe_prune(&self, now: u64) {
        let last = self.last_prune_ms.load(Ordering::Relaxed);

        if now.saturating_sub(last) < self.config.prune_interval.as_millis_u64() {
            return;
        }

        if self
            .last_prune_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.prune_expired(now);
        }
    }
}

struct PeerLimiter<C> {
    buckets: RwLock<FastHashMap<C, Arc<AtomicBucket>>>,
    rejects: AtomicU8,
    cooldown_until_ms: AtomicU64,
    last_seen_ms: AtomicU64,
}

impl<C> PeerLimiter<C>
where
    C: Copy + Eq + Hash + Send + Sync + 'static,
{
    fn new(now_ms: u64) -> Self {
        Self {
            buckets: RwLock::new(FastHashMap::default()),
            rejects: AtomicU8::new(0),
            cooldown_until_ms: AtomicU64::new(0),
            last_seen_ms: AtomicU64::new(now_ms),
        }
    }

    fn check(
        &self,
        config: &RateLimitConfig,
        policy: RateLimitPolicy<C>,
        now: u64,
    ) -> RateLimitVerdict {
        self.last_seen_ms.store(now, Ordering::Relaxed);

        let cooldown_until = self.cooldown_until_ms.load(Ordering::Acquire);
        if now < cooldown_until {
            return RateLimitVerdict::Reject {
                retry_after: Duration::from_millis(cooldown_until - now),
            };
        }

        let bucket = self.bucket(policy.class, policy.limit);
        let BucketResult::TryLater { after } = bucket.try_claim(now) else {
            self.rejects.store(0, Ordering::Relaxed);
            return RateLimitVerdict::Allow;
        };

        self.register_rejection(config, now);

        // Prefer cooldown wait time.
        let cooldown_until = self.cooldown_until_ms.load(Ordering::Acquire);
        let retry_after = if now < cooldown_until {
            cooldown_until - now
        } else {
            after.saturating_sub(now)
        };

        RateLimitVerdict::Reject {
            retry_after: Duration::from_millis(retry_after),
        }
    }

    fn bucket(&self, class: C, config: TrafficLimit) -> Arc<AtomicBucket> {
        if let Some(bucket) = self.buckets.read().get(&class) {
            return bucket.clone();
        }

        self.buckets
            .write()
            .entry(class)
            .or_insert_with(|| Arc::new(AtomicBucket::new(config)))
            .clone()
    }

    fn register_rejection(&self, config: &RateLimitConfig, now: u64) {
        let prev_rejects = self.rejects.fetch_add(1, Ordering::AcqRel);
        debug_assert!(prev_rejects < u8::MAX);

        let rejects = prev_rejects.saturating_add(1);
        if rejects >= config.rejects_before_cooldown {
            self.rejects.store(0, Ordering::Release);
            self.cooldown_until_ms.fetch_max(
                now.saturating_add(config.cooldown.as_millis_u64()),
                Ordering::AcqRel,
            );
        }
    }

    fn is_expired(&self, now: u64, state_ttl: u64) -> bool {
        let last_seen = self.last_seen_ms.load(Ordering::Relaxed);
        now.saturating_sub(last_seen) >= state_ttl
    }

    fn in_cooldown(&self, now: u64) -> bool {
        now < self.cooldown_until_ms.load(Ordering::Acquire)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BucketResult {
    Ok,
    TryLater { after: u64 },
}

/// GCRA bucket
struct AtomicBucket {
    /// Theoretical arrival time
    tat_ms: AtomicU64,
    /// Spacing between requests for configured rate
    interval_ms: u64,
    /// Burst allowance.
    tolerance_ms: u64,
}

impl AtomicBucket {
    const MILLIS_PER_SEC: u64 = 1_000;

    fn new(mut config: TrafficLimit) -> Self {
        config.normalize();

        let rate_per_sec = config.rate_per_sec.get() as u64;
        let interval_ms = Self::MILLIS_PER_SEC.div_ceil(rate_per_sec).max(1);
        let tolerance_ms = interval_ms.saturating_mul(config.burst.get().saturating_sub(1) as u64);

        Self {
            tat_ms: AtomicU64::new(0),
            interval_ms,
            tolerance_ms,
        }
    }

    fn try_claim(&self, now: u64) -> BucketResult {
        let mut tat = self.tat_ms.load(Ordering::Relaxed);

        loop {
            let allowed_at = tat.saturating_sub(self.tolerance_ms);
            if now < allowed_at {
                return BucketResult::TryLater { after: allowed_at };
            }

            let new_tat = tat.max(now).saturating_add(self.interval_ms);
            match self.tat_ms.compare_exchange_weak(
                tat,
                new_tat,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return BucketResult::Ok,
                Err(next) => tat = next,
            }
        }
    }
}

trait DurationExt {
    fn as_millis_u64(&self) -> u64;
}

impl DurationExt for Duration {
    fn as_millis_u64(&self) -> u64 {
        self.as_millis().try_into().unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    enum Class {
        A,
        B,
    }

    fn bucket_config(rate_per_sec: u32, burst: u32) -> TrafficLimit {
        TrafficLimit::new(
            NonZeroU32::new(rate_per_sec).unwrap(),
            NonZeroU32::new(burst).unwrap(),
        )
    }

    fn policy(class: Class) -> RateLimitPolicy<Class> {
        RateLimitPolicy {
            class,
            limit: bucket_config(1, 1),
        }
    }

    fn rate_limiter() -> RateLimiter<u32, Class> {
        RateLimiter::new(RateLimitConfig {
            rejects_before_cooldown: 2,
            ..Default::default()
        })
    }

    #[test]
    fn gcra_bucket_burst_and_refills() {
        let now = time::now_millis();

        let bucket = AtomicBucket::new(bucket_config(10, 2));

        // 10 req/s = refill every 100ms
        let delay = Duration::from_millis(100).as_millis_u64();

        // Spend burst capacity
        assert_eq!(bucket.try_claim(now), BucketResult::Ok);
        assert_eq!(bucket.try_claim(now), BucketResult::Ok);

        assert_eq!(bucket.try_claim(now), BucketResult::TryLater {
            after: now + delay,
        });

        // Wait for refilling tokens
        assert_eq!(bucket.try_claim(now + delay), BucketResult::Ok);
    }

    #[test]
    fn rate_limiter_cooldown() {
        let limiter = rate_limiter();

        let key = 1;

        // Spend burst capacity
        assert_eq!(
            limiter.check(&key, policy(Class::A)),
            RateLimitVerdict::Allow
        );

        // Spend rejects limit
        assert!(matches!(
            limiter.check(&key, policy(Class::A)),
            RateLimitVerdict::Reject { retry_after }
                if retry_after <= Duration::from_secs(1)
        ));

        // Check cooldown
        assert!(matches!(
            limiter.check(&key, policy(Class::A)),
            RateLimitVerdict::Reject { retry_after }
                if retry_after > Duration::from_secs(29)
        ));
    }

    #[test]
    fn rate_limiter_keep_buckets_per_class() {
        let limiter = rate_limiter();

        let key = 1;

        // Spend burst for A
        assert_eq!(
            limiter.check(&key, policy(Class::A)),
            RateLimitVerdict::Allow
        );

        // Spend burst for B
        assert_eq!(
            limiter.check(&key, policy(Class::B)),
            RateLimitVerdict::Allow
        );

        assert!(matches!(
            limiter.check(&key, policy(Class::A)),
            RateLimitVerdict::Reject { .. },
        ));

        assert!(matches!(
            limiter.check(&key, policy(Class::B)),
            RateLimitVerdict::Reject { .. },
        ));
    }
}
