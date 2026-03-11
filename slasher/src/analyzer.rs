use std::collections::BTreeMap;

use tycho_slasher_traits::ValidationSessionId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::BlocksBatch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPenaltyReport {
    pub session_id: ValidationSessionId,
    pub total_blocks_in_session: u32,
    pub offenders: Box<[ValidatorPenalty]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorPenalty {
    pub validator_idx: u16,
    pub missing_signatures: u32,
    pub invalid_signatures: u32,
}

#[derive(Debug, Default, Clone, Copy)]
struct ObservedSignature {
    has_valid_signature: bool,
    has_invalid_signature: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct SignatureTotals {
    missing_signatures: u32,
    invalid_signatures: u32,
}

pub fn analyze_session(
    session_id: ValidationSessionId,
    batches: &[BlocksBatch],
) -> SessionPenaltyReport {
    let mut validators = FastHashSet::default();
    let mut blocks = BTreeMap::<u32, FastHashMap<u16, ObservedSignature>>::new();

    for batch in batches {
        for history in &batch.signatures_history {
            validators.insert(history.validator_idx);
        }

        for offset in 0..batch.committed_blocks.len() {
            if !batch.committed_blocks.get(offset) {
                continue;
            }

            let seqno = batch.start_seqno + offset as u32;
            let signatures = blocks.entry(seqno).or_default();

            // Different validators can submit overlapping matrices for the same block.
            // We merge them by taking the union of observed bits, but a
            // `(block, validator_idx)` pair must never end up with both `valid`
            // and `invalid` states at once. If that happens, the input data is
            // internally inconsistent and we fail fast instead of guessing.
            for history in &batch.signatures_history {
                let offset = offset * 2;
                let has_invalid_signature = history.bits.get(offset);
                let has_valid_signature = history.bits.get(offset + 1);
                assert!(
                    !(has_invalid_signature && has_valid_signature),
                    "slasher analyzer invariant violated: validator {} has both valid and invalid bits for block {}",
                    history.validator_idx,
                    seqno,
                );

                let observed = signatures.entry(history.validator_idx).or_default();
                observed.has_invalid_signature |= has_invalid_signature;
                observed.has_valid_signature |= has_valid_signature;
                assert!(
                    !(observed.has_invalid_signature && observed.has_valid_signature),
                    "slasher analyzer invariant violated: validator {} has conflicting observations for block {}",
                    history.validator_idx,
                    seqno,
                );
            }
        }
    }

    let total_blocks_in_session = blocks.len() as u32;
    let threshold = total_blocks_in_session / 2;

    let mut validators = validators.into_iter().collect::<Vec<_>>();
    validators.sort_unstable();

    let mut totals = FastHashMap::<u16, SignatureTotals>::default();
    for signatures in blocks.values() {
        for &validator_idx in &validators {
            let observed = signatures.get(&validator_idx).copied().unwrap_or_default();
            let totals = totals.entry(validator_idx).or_default();
            if !observed.has_valid_signature {
                totals.missing_signatures += 1;
            }
            if observed.has_invalid_signature {
                totals.invalid_signatures += 1;
            }
        }
    }

    let offenders = validators
        .into_iter()
        .filter_map(|validator_idx| {
            let totals = totals.get(&validator_idx).copied().unwrap_or_default();
            let penalty_score = totals
                .missing_signatures
                .saturating_add(totals.invalid_signatures);
            (penalty_score > threshold).then_some(ValidatorPenalty {
                validator_idx,
                missing_signatures: totals.missing_signatures,
                invalid_signatures: totals.invalid_signatures,
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();

    SessionPenaltyReport {
        session_id,
        total_blocks_in_session,
        offenders,
    }
}

pub fn emit_report_metrics(report: &SessionPenaltyReport) {
    let labels = session_labels(report.session_id);
    metrics::gauge!("tycho_slasher_session_blocks_total", &labels)
        .set(report.total_blocks_in_session as f64);
    metrics::gauge!("tycho_slasher_session_penalty_candidates_total", &labels)
        .set(report.offenders.len() as f64);

    for offender in &report.offenders {
        let validator_idx = format!("{}", offender.validator_idx);
        let labels = [
            (
                "catchain_seqno",
                format!("{}", report.session_id.catchain_seqno),
            ),
            (
                "vset_switch_round",
                format!("{}", report.session_id.vset_switch_round),
            ),
            ("validator_idx", validator_idx.clone()),
        ];
        metrics::gauge!("tycho_slasher_penalty_candidate", &labels).set(1);

        let labels = [
            (
                "catchain_seqno",
                format!("{}", report.session_id.catchain_seqno),
            ),
            (
                "vset_switch_round",
                format!("{}", report.session_id.vset_switch_round),
            ),
            ("validator_idx", validator_idx),
        ];
        metrics::gauge!(
            "tycho_slasher_penalty_candidate_missing_signatures",
            &labels
        )
        .set(offender.missing_signatures as f64);
        metrics::gauge!(
            "tycho_slasher_penalty_candidate_invalid_signatures",
            &labels
        )
        .set(offender.invalid_signatures as f64);
    }
}

pub fn clear_report_metrics(report: &SessionPenaltyReport) {
    let labels = session_labels(report.session_id);
    metrics::gauge!("tycho_slasher_session_blocks_total", &labels).set(0);
    metrics::gauge!("tycho_slasher_session_penalty_candidates_total", &labels).set(0);

    for offender in &report.offenders {
        let validator_idx = format!("{}", offender.validator_idx);
        let labels = [
            (
                "catchain_seqno",
                format!("{}", report.session_id.catchain_seqno),
            ),
            (
                "vset_switch_round",
                format!("{}", report.session_id.vset_switch_round),
            ),
            ("validator_idx", validator_idx.clone()),
        ];
        metrics::gauge!("tycho_slasher_penalty_candidate", &labels).set(0);

        let labels = [
            (
                "catchain_seqno",
                format!("{}", report.session_id.catchain_seqno),
            ),
            (
                "vset_switch_round",
                format!("{}", report.session_id.vset_switch_round),
            ),
            ("validator_idx", validator_idx),
        ];
        metrics::gauge!(
            "tycho_slasher_penalty_candidate_missing_signatures",
            &labels
        )
        .set(0);
        metrics::gauge!(
            "tycho_slasher_penalty_candidate_invalid_signatures",
            &labels
        )
        .set(0);
    }
}

fn session_labels(session_id: ValidationSessionId) -> [(&'static str, String); 2] {
    [
        ("catchain_seqno", format!("{}", session_id.catchain_seqno)),
        (
            "vset_switch_round",
            format!("{}", session_id.vset_switch_round),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tycho_slasher_traits::{ReceivedSignature, ValidationSessionId};

    use super::*;

    #[test]
    fn analyzes_combined_penalty_threshold() {
        let session_id = ValidationSessionId {
            catchain_seqno: 7,
            vset_switch_round: 9,
        };
        let batch = make_batch(100, &[
            (100, &[(1, 0), (2, 0b10), (3, 0b01)]),
            (101, &[(1, 0), (2, 0b01), (3, 0b01)]),
            (102, &[(1, 0b10), (2, 0b10), (3, 0b01)]),
            (103, &[(1, 0b01), (2, 0b01), (3, 0b01)]),
        ]);

        let report = analyze_session(session_id, &[batch]);

        assert_eq!(report.total_blocks_in_session, 4);
        assert_eq!(report.offenders.as_ref(), &[
            ValidatorPenalty {
                validator_idx: 1,
                missing_signatures: 3,
                invalid_signatures: 1,
            },
            ValidatorPenalty {
                validator_idx: 2,
                missing_signatures: 2,
                invalid_signatures: 2,
            },
        ]);
    }

    #[test]
    fn merges_overlapping_batches_from_multiple_observers() {
        let session_id = ValidationSessionId {
            catchain_seqno: 11,
            vset_switch_round: 13,
        };
        let missing = make_batch(200, &[(200, &[(1, 0)])]);
        let valid = make_batch(200, &[(200, &[(1, 0b01)])]);

        let report = analyze_session(session_id, &[missing, valid]);

        assert_eq!(report.total_blocks_in_session, 1);
        assert!(report.offenders.is_empty());
    }

    #[test]
    #[should_panic(expected = "slasher analyzer invariant violated")]
    fn panics_on_dual_signature_bits() {
        let session_id = ValidationSessionId {
            catchain_seqno: 17,
            vset_switch_round: 19,
        };
        let batch = make_batch(300, &[(300, &[(1, 0b11)])]);

        let _ = analyze_session(session_id, &[batch]);
    }

    fn make_batch(start_seqno: u32, blocks: &[(u32, &[(u16, u8)])]) -> BlocksBatch {
        let end_seqno = blocks.iter().map(|(seqno, _)| *seqno).max().unwrap();
        let mut validators = blocks
            .iter()
            .flat_map(|(_, signatures)| signatures.iter().map(|(validator_idx, _)| *validator_idx))
            .collect::<Vec<_>>();
        validators.sort_unstable();
        validators.dedup();

        let mut batch = BlocksBatch::new(
            start_seqno,
            NonZeroU32::new(end_seqno - start_seqno + 1).unwrap(),
            &validators,
        );

        for (seqno, signatures) in blocks {
            let mut slots = validators
                .iter()
                .map(|validator_idx| {
                    let bits = signatures
                        .iter()
                        .find_map(|(item, bits)| (*item == *validator_idx).then_some(*bits))
                        .unwrap_or(0);
                    ReceivedSignature(bits)
                })
                .collect::<Vec<_>>();
            assert!(batch.commit_signatures(*seqno, &slots));
            slots.clear();
        }

        batch
    }
}
