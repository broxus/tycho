use anyhow::Result;

use crate::storage::SlasherStorageSnapshot;
use crate::{ParsedVset, SlasherConfig};

// See https://en.wikipedia.org/wiki/Z-test
const Z_95: f64 = 1.96;

#[tracing::instrument(skip_all, fields(vset_hash = %vset.hash))]
pub fn analyze_vset(
    snapshot: SlasherStorageSnapshot,
    vset: &ParsedVset,
    last_seqno: u32,
    own_validator_idx: usize,
    config: &SlasherConfig,
) -> Result<Vec<u16>> {
    let vset_hash = &vset.hash;
    let vset = &vset.vset;

    // Compute vset block range.
    let mut start_seqno = u32::MAX;
    for item in snapshot.iter_sessions(vset_hash) {
        let session = item?;
        start_seqno = std::cmp::min(start_seqno, session.start_seqno);
    }
    let vset_len = last_seqno.saturating_add(1).saturating_sub(start_seqno);
    if vset_len < config.vset_len_threshold {
        tracing::warn!(vset_len, "too short vset");
        return Ok(Vec::new());
    }

    let n = vset.list.len().min(vset.main.get() as _);
    if n <= 1 {
        tracing::warn!(n, "not enough nodes in vset");
        return Ok(Vec::new());
    }

    let mut scores = vec![vec![Score::default(); n]; n];
    let mut observed = vec![Observed::default(); n];

    let max_weight = vset.list.iter().take(n).map(|v| v.weight).sum::<u64>();
    let weight_threshold = max_weight.saturating_mul(2) / 3 + 1;

    // Build a matrix from all known block batches.
    let mut weight_per_block = Vec::with_capacity(100);
    for item in snapshot.iter_block_batches(vset_hash) {
        let (observer, batch) = item?;
        let observer = observer as usize;
        // NOTE: This is a hard error because we must not store invalid batches.
        anyhow::ensure!(observer < n, "invalid validator idx: idx={observer}, n={n}");
        let observer_weight = vset.list[observer].weight;

        let block_count = batch.committed_blocks.len();

        weight_per_block.clear();
        weight_per_block.resize(block_count, 0u64);

        let mut malformed = false;
        let has_bad_filled_rounds = has_bad_filled_rounds(&batch);
        if has_bad_filled_rounds {
            tracing::warn!(
                observer,
                reason = "has_bad_filled_rounds",
                "malformed batch"
            );
        }

        // Count weight of valid signatures per committed column (block).
        for history in &batch.observed {
            let other = history.validator_idx as usize;
            if other >= n {
                malformed = true;
                tracing::warn!(
                    observer,
                    history_entry_idx = other,
                    n,
                    reason = "invalid_history_entry",
                    "malformed batch",
                );
                continue;
            } else if other == observer {
                // Self signatures are not used.
                continue;
            }

            let weight = vset.list[other].weight;
            for (block, total_block_weight) in weight_per_block.iter_mut().enumerate() {
                if !batch.committed_blocks.get(block) {
                    // Ignore blocks which observer did not collate.
                    continue;
                }

                let valid_bit = block * 2 + 1;
                if history.bits.get(valid_bit) {
                    *total_block_weight += weight;
                }
            }
        }

        // Count samples and adjust weight per block.
        for weight in &mut weight_per_block {
            if *weight == 0 {
                continue;
            }

            *weight += observer_weight;
            if *weight >= weight_threshold {
                observed[observer].samples += 1;
            } else {
                // TODO: Should we treat this as malformed?
            }
        }

        if !has_bad_filled_rounds {
            observed[observer].filled_rounds += batch.filled_rounds as u64;
        }

        // Update scores.
        for history in &batch.observed {
            let other = history.validator_idx as usize;
            if other >= n || other == observer {
                continue;
            }

            for (block, weight) in weight_per_block.iter().enumerate() {
                if *weight < weight_threshold {
                    continue;
                }

                let scores = &mut scores[observer][other];
                scores.invalid_signatures += history.bits.get(block * 2) as u64;
                scores.valid_signatures += history.bits.get(block * 2 + 1) as u64;
            }

            if !has_bad_filled_rounds && batch.filled_rounds > 0 {
                scores[observer][other].points_proven += history.points_proven as u64;
            }
        }

        // Update malformed
        if malformed || has_bad_filled_rounds {
            observed[observer].malformed += 1;
        }
    }

    // Finally reduce scores and observations into accusations.
    let mut signature_accusation_weights = vec![0; n];
    let mut proven_accusation_weights = vec![0; n];
    let mut rates = Vec::with_capacity(n - 1);
    for observer in 0..n {
        if observed[observer].samples >= config.block_samples_threshold.get() {
            apply_signature_votes(
                observer,
                vset.list[observer].weight,
                &vset.list[observer].public_key,
                own_validator_idx,
                observed[observer].samples,
                &scores[observer],
                config.slow_node_factor,
                &mut rates,
                &mut signature_accusation_weights,
            );
        }

        if observed[observer].filled_rounds >= config.filled_rounds_threshold.get() {
            // those with `has_bad_filled_rounds` have zero filled rounds and will be skipped too
            apply_mempool_votes(
                observer,
                vset.list[observer].weight,
                &vset.list[observer].public_key,
                own_validator_idx,
                observed[observer].filled_rounds,
                &scores[observer],
                config.proven_node_factor,
                &mut rates,
                &mut proven_accusation_weights,
            );
        }
    }

    tracing::debug!(
        %vset_hash,
        ?signature_accusation_weights,
        ?proven_accusation_weights,
        weight_threshold,
        "computed accusation weights"
    );

    let accusations = (0..n)
        .filter_map(|idx| {
            let should_accuse = signature_accusation_weights[idx] >= weight_threshold
                || proven_accusation_weights[idx] >= weight_threshold
                || observed[idx].malformed >= config.malformed_samples_threshold;
            if should_accuse && idx == own_validator_idx {
                tracing::warn!(
                    own_validator_idx,
                    signature_weight = signature_accusation_weights[idx],
                    proven_weight = proven_accusation_weights[idx],
                    weight_threshold,
                    "our node is considered bad"
                );
                return None;
            }

            should_accuse.then_some(idx as u16)
        })
        .collect::<Vec<_>>();

    Ok(accusations)
}

#[derive(Default, Clone, Copy)]
struct Score {
    valid_signatures: u64,
    invalid_signatures: u64,
    points_proven: u64,
}

#[derive(Default, Clone, Copy)]
struct Observed {
    samples: u64,
    filled_rounds: u64,
    malformed: u64,
}

#[allow(clippy::too_many_arguments)]
fn apply_signature_votes(
    observer: usize,
    observer_weight: u64,
    observer_public_key: &impl std::fmt::Display,
    own_validator_idx: usize,
    samples: u64,
    scores: &[Score],
    slow_node_factor: f64,
    rates: &mut Vec<f64>,
    accusation_weights: &mut [u64],
) {
    // Compute the rate of valid signatures from other nodes.
    rates.clear();
    rates.extend(
        scores
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != observer)
            .map(|(_, score)| score.valid_signatures as f64 / samples as f64),
    );
    rates.sort_by(|a, b| a.total_cmp(b));

    let baseline = rates[rates.len() / 2];
    let slow_threshold = baseline * slow_node_factor;

    tracing::debug!(
        observer,
        baseline,
        slow_threshold,
        "computed valid signature rates"
    );

    for (other, score) in scores.iter().enumerate() {
        if other == observer {
            continue;
        }

        let rate = wilson_upper_bound(score.valid_signatures, samples, Z_95);
        if rate <= slow_threshold {
            if other == own_validator_idx {
                tracing::warn!(
                    own_validator_idx,
                    observer,
                    reason = "slow_signatures",
                    "our node may be accused by {}",
                    observer_public_key
                );
            } else {
                tracing::debug!(
                    observer,
                    other,
                    reason = "slow_signatures",
                    "accusation found"
                );
            }
            accusation_weights[other] += observer_weight;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_mempool_votes(
    observer: usize,
    observer_weight: u64,
    observer_public_key: &impl std::fmt::Display,
    own_validator_idx: usize,
    filled_rounds: u64,
    scores: &[Score],
    proven_node_factor: f64,
    rates: &mut Vec<f64>,
    accusation_weights: &mut [u64],
) {
    rates.clear();
    rates.extend(
        scores
            .iter()
            .enumerate()
            .filter(|(target, _)| *target != observer)
            .map(|(_, score)| score.points_proven as f64 / filled_rounds as f64),
    );
    rates.sort_by(|a, b| a.total_cmp(b));

    let baseline = rates[rates.len() / 2];
    let low_proven_threshold = baseline * proven_node_factor;

    tracing::debug!(
        observer,
        baseline,
        low_proven_threshold,
        "computed proven point rates"
    );

    for (other, score) in scores.iter().enumerate() {
        if other == observer {
            continue;
        }

        let rate = wilson_upper_bound(score.points_proven, filled_rounds, Z_95);
        if rate <= low_proven_threshold {
            if other == own_validator_idx {
                tracing::warn!(
                    own_validator_idx,
                    observer,
                    reason = "low_proven_points",
                    "our node may be accused by {}",
                    observer_public_key
                );
            } else {
                tracing::debug!(
                    observer,
                    other,
                    reason = "low_proven_points",
                    "accusation found"
                );
            }
            accusation_weights[other] += observer_weight;
        }
    }
}

fn has_bad_filled_rounds(batch: &crate::BlocksBatch) -> bool {
    let range_len = (batch.round_range.as_ref())
        .filter(|r| !r.is_empty())
        .map(|r| (*r.end() - *r.start()) as u64 + 1)
        .unwrap_or_default();
    (batch.filled_rounds as u64 > range_len)
        || (batch.observed.iter()).any(|history| history.points_proven as u32 > batch.filled_rounds)
}

// NOTE: We don't really need an exact determenism in decisions so we can
// use floating point math here. If its a concert, this can be rewritten
// to fixed point.
fn wilson_upper_bound(hits: u64, samples: u64, z: f64) -> f64 {
    debug_assert!(hits <= samples);
    debug_assert!(z.is_finite() && z >= 0.0);

    if samples == 0 {
        return 1.0;
    }

    let samples_f = samples as f64;
    let hits_f = hits.min(samples) as f64;
    let p = hits_f / samples_f;

    let z2 = z * z;
    let denom = 1.0 + z2 / samples_f;
    let center = p + z2 / (2.0 * samples_f);
    let margin = z * (p * (1.0 - p) / samples_f + z2 / (4.0 * samples_f * samples_f)).sqrt();

    ((center + margin) / denom).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::BlocksBatch;

    #[test]
    fn proven_votes_use_observed_filled_rounds() {
        let scores = vec![
            Score::default(),
            Score {
                points_proven: 100,
                ..Default::default()
            },
            Score {
                points_proven: 30,
                ..Default::default()
            },
        ];
        let mut rates = Vec::new();
        let mut accusation_weights = vec![0; 3];

        apply_mempool_votes(
            0,
            10,
            &"observer",
            usize::MAX,
            100,
            &scores,
            0.67,
            &mut rates,
            &mut accusation_weights,
        );

        assert_eq!(accusation_weights, [0, 0, 10]);
    }

    #[test]
    fn signature_votes_still_use_observed_samples() {
        let scores = vec![
            Score::default(),
            Score {
                valid_signatures: 100,
                ..Default::default()
            },
            Score {
                valid_signatures: 30,
                ..Default::default()
            },
        ];
        let mut rates = Vec::new();
        let mut accusation_weights = vec![0; 3];

        apply_signature_votes(
            0,
            10,
            &"observer",
            usize::MAX,
            100,
            &scores,
            0.5,
            &mut rates,
            &mut accusation_weights,
        );

        assert_eq!(accusation_weights, [0, 0, 10]);
    }

    #[test]
    fn detects_impossible_proof_stats() {
        let mut batch = BlocksBatch::new(0, NonZeroU32::new(1).unwrap(), &[0, 1]);
        batch.round_range = Some(10..=10);
        batch.filled_rounds = 1;
        batch.observed[0].points_proven = 1;
        batch.observed[1].points_proven = 0;
        assert!(!has_bad_filled_rounds(&batch));

        batch.filled_rounds = 2;
        assert!(has_bad_filled_rounds(&batch));

        batch.filled_rounds = 1;
        batch.observed[0].points_proven = 2;
        assert!(has_bad_filled_rounds(&batch));

        batch.filled_rounds = 1;
        batch.observed[0].points_proven = 1;
        batch.round_range = None;
        assert!(has_bad_filled_rounds(&batch));
    }
}
