use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tycho_network::PeerId;
use tycho_util::time::now_sec;

#[derive(Clone)]
#[repr(transparent)]
pub struct Neighbour {
    inner: Arc<Inner>,
}

impl Neighbour {
    pub fn new(peer_id: PeerId, expires_at: u32, default_roundtrip: &Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                peer_id,
                expires_at,
                stats: RwLock::new(TrackedStats::new(truncate_time(default_roundtrip))),
            }),
        }
    }

    #[inline]
    pub fn peer_id(&self) -> &PeerId {
        &self.inner.peer_id
    }

    #[inline]
    pub fn expires_at_secs(&self) -> u32 {
        self.inner.expires_at
    }

    pub fn get_stats(&self) -> NeighbourStats {
        let stats = self.inner.stats.read();
        NeighbourStats {
            score: stats.score,
            total_requests: stats.total,
            failed_requests: stats.failed,
            avg_roundtrip: stats
                .roundtrip
                .get_avg()
                .map(|avg| Duration::from_millis(avg as u64)),
            created: stats.created,
        }
    }

    pub fn cmp_score(&self, other: &Neighbour) -> std::cmp::Ordering {
        let own_stats = self.inner.stats.read().score;
        let other_stats = other.inner.stats.read().score;
        own_stats.cmp(&other_stats)
    }

    pub fn is_reliable(&self) -> bool {
        self.inner.stats.read().higher_than_threshold()
    }

    pub fn compute_selection_score(&self) -> Option<u8> {
        self.inner.stats.read().compute_selection_score()
    }

    pub fn get_roundtrip(&self) -> Option<Duration> {
        let roundtrip = self.inner.stats.read().roundtrip.get_avg()?;
        Some(Duration::from_millis(roundtrip as u64))
    }

    pub fn track_request(&self, roundtrip: &Duration, success: bool) {
        let roundtrip = truncate_time(roundtrip);
        self.inner.stats.write().update(roundtrip, success);
    }

    pub fn punish(&self, reason: PunishReason) {
        self.inner.stats.write().punish(reason);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PunishReason {
    Dumb,
    Slow,
    Malicious,
}

impl PunishReason {
    pub fn score(self) -> u8 {
        match self {
            Self::Dumb => 4,
            Self::Slow => 8,
            Self::Malicious => 128,
        }
    }
}

/// Neighbour request statistics.
#[derive(Debug, Clone)]
pub struct NeighbourStats {
    /// Current reliability score.
    pub score: u8,
    /// Total number of requests to the neighbour.
    pub total_requests: u64,
    /// The number of failed requests to the neighbour.
    pub failed_requests: u64,
    /// Average roundtrip.
    /// NONE if there were no requests to the neighbour.
    pub avg_roundtrip: Option<Duration>,
    /// Neighbour first appearance
    pub created: u32,
}

struct Inner {
    peer_id: PeerId,
    expires_at: u32,
    stats: parking_lot::RwLock<TrackedStats>,
}

struct TrackedStats {
    score: u8,
    total: u64,
    failed: u64,
    failed_requests_history: u64,
    roundtrip: PackedSmaBuffer,
    created: u32,
}

impl TrackedStats {
    const MAX_SCORE: u8 = 128;
    const SCORE_THRESHOLD: u8 = 16;
    const INITIAL_SCORE: u8 = Self::MAX_SCORE / 2;

    fn new(default_roundtrip_ms: u16) -> Self {
        Self {
            score: Self::INITIAL_SCORE,
            total: 0,
            failed: 0,
            failed_requests_history: 0,
            roundtrip: PackedSmaBuffer(default_roundtrip_ms as u64),
            created: now_sec(),
        }
    }

    fn higher_than_threshold(&self) -> bool {
        self.score >= TrackedStats::SCORE_THRESHOLD
    }

    fn compute_selection_score(&self) -> Option<u8> {
        const OK_ROUNDTRIP: u16 = 160; // ms
        const MAX_ROUNDTRIP_BONUS: u8 = 16;
        const ROUNDTRIP_BONUS_THRESHOLD: u8 = 120;

        const MAX_FAILED_REQUESTS: u8 = 4;
        const FAILURE_PENALTY: u8 = 16;

        const FAILED_REQUESTS_MASK: u64 = (1 << MAX_FAILED_REQUESTS) - 1;

        let mut score = self.score;
        if self.failed_requests_history & FAILED_REQUESTS_MASK == FAILED_REQUESTS_MASK {
            // Reduce the score if there were several sequential failures
            score = score.saturating_sub(FAILURE_PENALTY);
        } else if score >= ROUNDTRIP_BONUS_THRESHOLD {
            // Try to compute a score bonus for neighbours with short roundtrip
            if let Some(avg) = self.roundtrip.get_avg() {
                let max = OK_ROUNDTRIP;
                if let Some(inv_avg) = max.checked_sub(avg) {
                    // Scale bonus
                    let bonus = (inv_avg * MAX_ROUNDTRIP_BONUS as u16 / max) as u8;
                    score = score.saturating_add(std::cmp::max(bonus, 1));
                }
            }
        }

        (score >= Self::SCORE_THRESHOLD).then_some(score)
    }

    fn update(&mut self, roundtrip: u16, success: bool) {
        const SUCCESS_REQUEST_SCORE: u8 = 8;
        const FAILED_REQUEST_PENALTY: u8 = 8;

        self.failed_requests_history <<= 1;
        if success {
            self.score = std::cmp::min(
                self.score.saturating_add(SUCCESS_REQUEST_SCORE),
                Self::MAX_SCORE,
            );
        } else {
            self.score = self.score.saturating_sub(FAILED_REQUEST_PENALTY);
            self.failed += 1;
            self.failed_requests_history |= 1;
        }
        self.total += 1;

        let roundtrip_buffer = &mut self.roundtrip;
        roundtrip_buffer.add(roundtrip);
    }

    fn punish(&mut self, reason: PunishReason) {
        self.score = self.score.saturating_sub(reason.score());
    }
}

#[repr(transparent)]
struct PackedSmaBuffer(u64);

impl PackedSmaBuffer {
    fn add(&mut self, value: u16) {
        self.0 <<= 16;
        self.0 |= value as u64;
    }

    fn get_avg(&self) -> Option<u16> {
        let mut storage = self.0;
        let mut total = 0;
        let mut i = 0;
        while storage > 0 {
            total += storage & 0xffff;
            storage >>= 16;
            i += 1;
        }

        if i == 0 {
            None
        } else {
            Some((total / i) as u16)
        }
    }
}

fn truncate_time(roundtrip: &Duration) -> u16 {
    std::cmp::min(roundtrip.as_millis() as u64, u16::MAX as u64) as u16
}
