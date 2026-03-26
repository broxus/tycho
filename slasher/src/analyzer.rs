use std::collections::{BTreeMap, BTreeSet};

use tycho_slasher_traits::ValidationSessionId;
use tycho_types::cell::HashBytes;

use crate::BlocksBatch;

#[derive(Debug, PartialEq, Eq)]
pub struct ObservedBlocksBatch {
    pub observer_validator_idx: u16,
    pub batch: BlocksBatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMeta {
    pub session_id: ValidationSessionId,
    pub epoch_start_session_id: ValidationSessionId,
    pub validator_indices: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VsetEpoch {
    pub start_session_id: ValidationSessionId,
    pub vset_hash: HashBytes,
    pub next_epoch_start_session_id: Option<ValidationSessionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPenaltyReport {
    pub session_id: ValidationSessionId,
    pub epoch_start_session_id: ValidationSessionId,
    pub session_weight: u32,
    pub validators: Box<[SessionValidatorScore]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionValidatorScore {
    pub validator_idx: u16,
    pub earned_points: u64,
    pub max_points: u64,
    pub is_bad: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VsetPenaltyReport {
    pub epoch_start_session_id: ValidationSessionId,
    pub vset_hash: HashBytes,
    pub validators: Box<[VsetValidatorPenalty]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VsetValidatorPenalty {
    pub validator_idx: u16,
    pub bad_sessions_weight: u64,
    pub total_sessions_weight: u64,
    pub is_bad: bool,
}

pub fn analyze_session(
    meta: &SessionMeta,
    batches: &[ObservedBlocksBatch],
) -> SessionPenaltyReport {
    let mut committed_blocks = BTreeSet::new();
    let mut observed_rows = BTreeSet::new();
    // observer -> observed : points * session weight
    let mut validator_points = BTreeMap::<(u16, u16), u64>::new();

    for item in batches {
        observed_rows.insert(item.observer_validator_idx);

        for offset in 0..item.batch.committed_blocks.len() {
            if !item.batch.committed_blocks.get(offset) {
                continue;
            }

            committed_blocks.insert(item.batch.start_seqno + offset as u32);

            for history in &item.batch.signatures_history {
                let bit_offset = offset * 2;
                let has_invalid_signature = history.bits.get(bit_offset);
                let has_valid_signature = history.bits.get(bit_offset + 1);

                if !(has_invalid_signature && has_valid_signature) {
                    tracing::warn!(
                        "slasher analyzer invariant violated: observer {} saw validator {} as both valid and invalid in session {:?}",
                        item.observer_validator_idx,
                        history.validator_idx,
                        meta.session_id,
                    );
                    continue;
                }

                if has_valid_signature {
                    *validator_points
                        .entry((item.observer_validator_idx, history.validator_idx))
                        .or_default() += 1;
                }
            }
        }
    }

    let session_weight = committed_blocks.len() as u64;

    let mut validator_indices = meta.validator_indices.clone();
    validator_indices.sort_unstable();
    validator_indices.dedup();

    let validators = validator_indices
        .into_iter()
        .map(|validator_idx| {
            let max_rows =
                observed_rows.len() as u64 - u64::from(observed_rows.contains(&validator_idx));
            let max_points = max_rows
                .saturating_mul(session_weight)
                .saturating_mul(session_weight);

            let earned_points = observed_rows
                .iter()
                .copied()
                .filter(|observer| *observer != validator_idx)
                .map(|observer| {
                    validator_points
                        .get(&(observer, validator_idx))
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>()
                .saturating_mul(session_weight);

            SessionValidatorScore {
                validator_idx,
                earned_points,
                max_points,
                is_bad: max_points > 0 && earned_points.saturating_mul(2) < max_points,
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();

    SessionPenaltyReport {
        session_id: meta.session_id,
        epoch_start_session_id: meta.epoch_start_session_id,
        session_weight: session_weight as u32,
        validators,
    }
}

pub fn analyze_vset_epoch(
    epoch: &VsetEpoch,
    session_reports: &[SessionPenaltyReport],
    bad_sessions_weight_threshold: u64,
) -> VsetPenaltyReport {
    let mut validators = BTreeMap::<u16, VsetValidatorPenalty>::new();

    for report in session_reports {
        let session_weight = u64::from(report.session_weight);

        for item in &report.validators {
            let penalty = validators
                .entry(item.validator_idx)
                .or_insert(VsetValidatorPenalty {
                    validator_idx: item.validator_idx,
                    bad_sessions_weight: 0,
                    total_sessions_weight: 0,
                    is_bad: false,
                });
            penalty.total_sessions_weight =
                penalty.total_sessions_weight.saturating_add(session_weight);
            if item.is_bad {
                penalty.bad_sessions_weight =
                    penalty.bad_sessions_weight.saturating_add(session_weight);
            }
        }
    }

    for item in validators.values_mut() {
        item.is_bad = item.bad_sessions_weight > bad_sessions_weight_threshold;
    }

    VsetPenaltyReport {
        epoch_start_session_id: epoch.start_session_id,
        vset_hash: epoch.vset_hash,
        validators: validators
            .into_values()
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}
