use anyhow::Result;

use crate::ParsedVset;
use crate::bc::SlasherParams;
use crate::storage::SlasherStorageSnapshot;

// TODO: Move these constants into config.

// Session should contain at least this amount of blocks to do some reporting.
const MIN_VSET_LENGTH: u32 = 1000;
// At least this number of samples must be collected to accuse someone.
const MIN_SAMPLES: u64 = 100;
// At least this number of malformed batches must be collected to accuse someone.
const MIN_MALFORMED: u64 = 5;
// We treat the node as slow if its block rate is this times the median rate.
const SLOW_FACTOR: f64 = 0.5;
// See https://en.wikipedia.org/wiki/Z-test
const Z_95: f64 = 1.96;

#[tracing::instrument(skip_all, fields(vset_hash = %vset.hash))]
pub fn analyze_vset(
    snapshot: SlasherStorageSnapshot,
    vset: &ParsedVset,
    last_seqno: u32,
    own_validator_idx: usize,
    _params: &SlasherParams,
) -> Result<Vec<u16>> {
    let vset_hash = &vset.hash;
    let vset = &vset.vset;

    // Compute vset block range.
    let session_ids = snapshot.load_vset_sessions(vset_hash)?;
    let start_seqno = session_ids
        .iter()
        .map(|item| item.start_seqno)
        .min()
        .unwrap_or(u32::MAX);
    let vset_len = last_seqno.saturating_sub(start_seqno);
    if vset_len < MIN_VSET_LENGTH {
        tracing::warn!(vset_len, "too short vset");
        return Ok(Vec::new());
    }

    let n = vset.list.len();
    if n <= 1 {
        tracing::warn!(n, "not enough nodes in vset");
        return Ok(Vec::new());
    }

    let mut scores = vec![vec![Score::default(); n]; n];
    let mut observed = vec![Observed::default(); n];

    let max_weight = vset
        .list
        .iter()
        .take(vset.main.get() as usize)
        .map(|v| v.weight)
        .sum::<u64>();
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

        // Count weight of valid signatures per committed column (block).
        for history in &batch.signatures_history {
            let other = history.validator_idx as usize;
            if other >= n || other == observer {
                malformed = true;
                tracing::warn!(
                    observer,
                    history_entry_idx = other,
                    n,
                    reason = "invalid_history_entry",
                    "malformed batch",
                );
                continue;
            }

            let weight = vset.list[other].weight;
            for block in 0..block_count {
                if !batch.committed_blocks.get(block) {
                    // Ignore blocks which observer did not collate.
                    continue;
                }

                let valid_bit = block * 2 + 1;
                if history.bits.get(valid_bit) {
                    weight_per_block[block] += weight;
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

        // Update scores.
        for history in &batch.signatures_history {
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
        }

        // Update malformed
        if malformed {
            observed[observer].malformed += 1;
        }
    }

    // Finally reduce scores and observations into accusations.
    let mut accusation_weights = vec![0; n];
    let mut rates = Vec::with_capacity(n - 1);
    for (observer, (observed, scores)) in std::iter::zip(&observed, &scores).enumerate() {
        if observed.samples < MIN_SAMPLES {
            continue;
        }
        let observer_weight = vset.list[observer].weight;

        // Compute the rate of valid signatures from other nodes.
        rates.clear();
        rates.extend(scores.iter().enumerate().filter_map(|(i, score)| {
            (i != observer).then(|| score.valid_signatures as f64 / observed.samples as f64)
        }));
        rates.sort_by(|a, b| a.total_cmp(b));

        let baseline = rates[rates.len() / 2];
        let slow_threshold = baseline * SLOW_FACTOR;

        tracing::debug!(baseline, slow_threshold, "computed valid signature rates");

        for (other, score) in scores.iter().enumerate() {
            if other == observer {
                continue;
            }

            let rate = wilson_upper_bound(score.valid_signatures, observed.samples, Z_95);
            if rate <= slow_threshold {
                tracing::debug!(observer, other, "accusation found");
                if other == own_validator_idx {
                    tracing::warn!(
                        own_validator_idx,
                        observer,
                        "our node may be accused by {}",
                        vset.list[observer].public_key
                    );
                }
                accusation_weights[other] += observer_weight;
            }
        }
    }

    tracing::debug!(
        %vset_hash,
        ?accusation_weights,
        weight_threshold,
        "computed accusation weights"
    );

    let accusations = std::iter::zip(observed, accusation_weights)
        .enumerate()
        .filter_map(|(idx, (observed, weight))| {
            let should_accuse = weight >= weight_threshold || observed.malformed >= MIN_MALFORMED;
            if idx == own_validator_idx {
                tracing::warn!(
                    own_validator_idx,
                    weight,
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
}

#[derive(Default, Clone, Copy)]
struct Observed {
    samples: u64,
    malformed: u64,
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
