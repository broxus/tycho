use std::time::{Duration, Instant};

pub struct TokenBucketConfig {
    pub window: Duration,
    pub tokens: u16,
    /// Used by [`SoftTokenBucket`] only; zero makes it act as [`TokenBucket`]
    pub soft_rejects: u16,
}

pub struct SoftTokenBucket {
    bucket: TokenBucket,
    soft_rejected: u16,
}

impl SoftTokenBucket {
    pub fn new(now: Instant, config: &TokenBucketConfig) -> Self {
        Self {
            bucket: TokenBucket::new(now, config),
            soft_rejected: 0,
        }
    }

    pub fn allows(&mut self, now: Instant, config: &TokenBucketConfig) -> Result<bool, ()> {
        if self.bucket.passes(now, config) {
            self.soft_rejected = 0;
            Ok(true)
        } else if self.soft_rejected < config.soft_rejects {
            self.soft_rejected += 1;
            Ok(false)
        } else {
            Err(())
        }
    }
}

pub struct TokenBucket {
    next_refill: Instant,
    left_tokens: u16,
}

impl TokenBucket {
    pub fn new(now: Instant, config: &TokenBucketConfig) -> Self {
        Self {
            next_refill: now + config.window,
            left_tokens: config.tokens,
        }
    }

    pub fn passes(&mut self, now: Instant, config: &TokenBucketConfig) -> bool {
        if self.next_refill <= now {
            self.next_refill = now + config.window;
            self.left_tokens = config.tokens;
        }
        if self.left_tokens > 0 {
            self.left_tokens -= 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    #[tokio::test]
    async fn test_bucket() {
        let config = TokenBucketConfig {
            window: Duration::from_secs(2),
            tokens: 5,
            soft_rejects: 0,
        };

        let start = now();

        let mut bucket = TokenBucket::new(start, &config);

        for n in (0..config.tokens).rev() {
            assert!(bucket.passes(start, &config), "left {n} tokens on start");
        }
        assert!(!bucket.passes(start, &config), "shall not pass on start");

        tokio::time::sleep(config.window / 2).await;

        assert!(!bucket.passes(now(), &config), "no refill");

        tokio::time::sleep(config.window / 2).await;

        assert!(bucket.passes(now(), &config), "refilled");

        assert!(bucket.passes(start, &config), "out-of-order");

        for n in (0..config.tokens - 2).rev() {
            assert!(bucket.passes(now(), &config), "left {n} tokens");
        }
        assert!(!bucket.passes(now(), &config), "shall not pass");
    }

    #[tokio::test]
    async fn test_soft_bucket() {
        let config = TokenBucketConfig {
            window: Duration::from_secs(2),
            tokens: 5,
            soft_rejects: 6,
        };

        let start = now();

        let mut sb = SoftTokenBucket::new(start, &config);

        for n in (0..config.tokens).rev() {
            assert_eq!(sb.allows(start, &config), Ok(true), "left {n} tokens");
        }

        for n in (0..config.soft_rejects).rev() {
            assert_eq!(sb.allows(start, &config), Ok(false), "left {n} soft");
        }

        assert_eq!(sb.allows(start, &config), Err(()), "hard after soft");
        assert_eq!(sb.allows(start, &config), Err(()), "hard again");

        tokio::time::sleep(config.window / 2).await;

        assert_eq!(sb.allows(now(), &config), Err(()), "no refill");

        tokio::time::sleep(config.window / 2).await;

        assert_eq!(sb.allows(now(), &config), Ok(true), "refilled");

        assert_eq!(sb.allows(start, &config), Ok(true), "out-of-order token");

        for n in (0..config.tokens - 2).rev() {
            assert_eq!(sb.allows(now(), &config), Ok(true), "left {n} tokens");
        }

        assert_eq!(sb.allows(now(), &config), Ok(false), "soft refilled");

        assert_eq!(sb.allows(start, &config), Ok(false), "out-of-order soft");

        for n in (0..config.soft_rejects - 2).rev() {
            assert_eq!(sb.allows(now(), &config), Ok(false), "left {n} soft");
        }

        assert_eq!(sb.allows(now(), &config), Err(()), "hard after refill");
    }
}
