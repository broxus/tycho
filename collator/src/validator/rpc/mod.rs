use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use futures_util::future::Future;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tycho_network::PeerId;
use tycho_util::serde_helpers;

pub use self::client::ValidatorClient;
pub use self::service::ValidatorService;
use crate::tracing_targets;
use crate::validator::proto;

mod client;
mod service;

// TODO: Add jitter as well?
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExchangeSignaturesBackoff {
    #[serde(with = "serde_helpers::humantime")]
    pub min_interval: Duration,
    #[serde(with = "serde_helpers::humantime")]
    pub max_interval: Duration,
    pub factor: f32,
}

impl Default for ExchangeSignaturesBackoff {
    fn default() -> Self {
        Self {
            min_interval: Duration::from_millis(50),
            max_interval: Duration::from_secs(1),
            factor: 1.5,
        }
    }
}

static EXCHANGE_FAILURE_LOG: LazyLock<ExchangeFailureLog> =
    LazyLock::new(ExchangeFailureLog::default);

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct ExchangeFailureLogKey {
    direction: &'static str,
    peer_id: PeerId,
    kind: &'static str,
}

struct ExchangeFailureLogState {
    last_logged_at: Instant,
    suppressed_events: u64,
    suppressed_first_block_seqno: Option<u32>,
    suppressed_last_block_seqno: Option<u32>,
}

#[derive(Debug, PartialEq, Eq)]
struct SuppressedFailures {
    events: u64,
    first_block_seqno: Option<u32>,
    last_block_seqno: Option<u32>,
}

#[derive(Default)]
struct ExchangeFailureLog {
    state: Mutex<HashMap<ExchangeFailureLogKey, ExchangeFailureLogState>>,
}

#[derive(Clone, Copy)]
pub(crate) struct ExchangeSlotBounds {
    pub active_first_block_seqno: Option<u32>,
    pub active_last_block_seqno: Option<u32>,
    pub cache_first_block_seqno: Option<u32>,
    pub cache_last_block_seqno: Option<u32>,
}

impl ExchangeSlotBounds {
    fn classification(&self, block_seqno: u32) -> &'static str {
        let first = [self.active_first_block_seqno, self.cache_first_block_seqno]
            .into_iter()
            .flatten()
            .min();
        let last = [self.active_last_block_seqno, self.cache_last_block_seqno]
            .into_iter()
            .flatten()
            .max();

        match (first, last) {
            (Some(first), _) if block_seqno < first => "too_old",
            (_, Some(last)) if block_seqno > last => "ahead",
            (Some(_), Some(_)) => "gap",
            _ => "unknown",
        }
    }
}

impl ExchangeFailureLog {
    const INTERVAL: Duration = Duration::from_secs(1);

    fn record_at(
        &self,
        key: ExchangeFailureLogKey,
        block_seqno: u32,
        now: Instant,
    ) -> Option<SuppressedFailures> {
        let mut state = self.state.lock();
        match state.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry)
                if now.saturating_duration_since(entry.get().last_logged_at) < Self::INTERVAL =>
            {
                let state = entry.get_mut();
                state.suppressed_events += 1;
                state
                    .suppressed_first_block_seqno
                    .get_or_insert(block_seqno);
                state.suppressed_last_block_seqno = Some(block_seqno);
                None
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.last_logged_at = now;
                Some(SuppressedFailures {
                    events: std::mem::take(&mut state.suppressed_events),
                    first_block_seqno: state.suppressed_first_block_seqno.take(),
                    last_block_seqno: state.suppressed_last_block_seqno.take(),
                })
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(ExchangeFailureLogState {
                    last_logged_at: now,
                    suppressed_events: 0,
                    suppressed_first_block_seqno: None,
                    suppressed_last_block_seqno: None,
                });
                Some(SuppressedFailures {
                    events: 0,
                    first_block_seqno: None,
                    last_block_seqno: None,
                })
            }
        }
    }

    fn record_incoming_no_slot<E>(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        bounds: ExchangeSlotBounds,
        error: &E,
    ) where
        E: std::fmt::Debug,
    {
        if !tracing::enabled!(
            target: tracing_targets::VALIDATOR_EXCHANGE_FAILURE,
            tracing::Level::DEBUG
        ) {
            return;
        }
        let key = ExchangeFailureLogKey {
            direction: "incoming",
            peer_id: *peer_id,
            kind: "no_slot",
        };
        let Some(suppressed) = self.record_at(key, block_seqno, Instant::now()) else {
            return;
        };

        tracing::debug!(
            target: tracing_targets::VALIDATOR_EXCHANGE_FAILURE,
            direction = "incoming",
            %peer_id,
            block_seqno,
            result_kind = "error",
            error_kind = "no_slot",
            classification = bounds.classification(block_seqno),
            active_slots_present = bounds.active_first_block_seqno.is_some(),
            active_first_block_seqno = bounds.active_first_block_seqno.unwrap_or_default(),
            active_last_block_seqno = bounds.active_last_block_seqno.unwrap_or_default(),
            cache_slots_present = bounds.cache_first_block_seqno.is_some(),
            cache_first_block_seqno = bounds.cache_first_block_seqno.unwrap_or_default(),
            cache_last_block_seqno = bounds.cache_last_block_seqno.unwrap_or_default(),
            suppressed_events = suppressed.events,
            suppressed_first_block_seqno = suppressed.first_block_seqno.unwrap_or_default(),
            suppressed_last_block_seqno = suppressed.last_block_seqno.unwrap_or_default(),
            ?error,
            "signature exchange request rejected without an available slot",
        );
    }
}

pub(crate) fn record_incoming_no_slot<E>(
    peer_id: &PeerId,
    block_seqno: u32,
    bounds: ExchangeSlotBounds,
    error: &E,
) where
    E: std::fmt::Debug,
{
    EXCHANGE_FAILURE_LOG.record_incoming_no_slot(peer_id, block_seqno, bounds, error);
}

pub trait ExchangeSignatures: Send + Sync + 'static {
    type Err: std::fmt::Debug + Send;

    fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> impl Future<Output = Result<proto::Exchange, Self::Err>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_failure_log_aggregates_independent_peers() {
        let log = ExchangeFailureLog::default();
        let now = Instant::now();
        let peer_id = PeerId([1; 32]);
        let other_peer_id = PeerId([2; 32]);
        let key = ExchangeFailureLogKey {
            direction: "incoming",
            peer_id,
            kind: "no_slot",
        };

        assert_eq!(
            log.record_at(key, 10, now),
            Some(SuppressedFailures {
                events: 0,
                first_block_seqno: None,
                last_block_seqno: None,
            })
        );
        assert_eq!(
            log.record_at(
                ExchangeFailureLogKey {
                    peer_id: other_peer_id,
                    ..key
                },
                20,
                now,
            ),
            Some(SuppressedFailures {
                events: 0,
                first_block_seqno: None,
                last_block_seqno: None,
            })
        );
    }

    #[test]
    fn exchange_failure_log_reports_suppressed_range_on_rollover() {
        let log = ExchangeFailureLog::default();
        let now = Instant::now();
        let key = ExchangeFailureLogKey {
            direction: "incoming",
            peer_id: PeerId([1; 32]),
            kind: "no_slot",
        };

        assert!(log.record_at(key, 10, now).is_some());
        assert_eq!(
            log.record_at(key, 11, now + Duration::from_millis(250)),
            None
        );
        assert_eq!(
            log.record_at(key, 13, now + Duration::from_millis(500)),
            None
        );
        assert_eq!(
            log.record_at(key, 14, now + ExchangeFailureLog::INTERVAL),
            Some(SuppressedFailures {
                events: 2,
                first_block_seqno: Some(11),
                last_block_seqno: Some(13),
            })
        );
        assert_eq!(
            log.record_at(key, 15, now + ExchangeFailureLog::INTERVAL),
            None
        );
    }

    #[test]
    fn exchange_slot_bounds_classify_missing_seqno() {
        let bounds = ExchangeSlotBounds {
            active_first_block_seqno: Some(10),
            active_last_block_seqno: Some(12),
            cache_first_block_seqno: Some(13),
            cache_last_block_seqno: Some(15),
        };

        assert_eq!(bounds.classification(9), "too_old");
        assert_eq!(bounds.classification(16), "ahead");
        assert_eq!(bounds.classification(11), "gap");
        assert_eq!(
            ExchangeSlotBounds {
                active_first_block_seqno: None,
                active_last_block_seqno: None,
                cache_first_block_seqno: None,
                cache_last_block_seqno: None,
            }
            .classification(10),
            "unknown"
        );
    }
}
