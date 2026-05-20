use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::BufMut;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use tycho_core::storage::shard_state::counters::{Counters, Idx, NextIdx};
use tycho_util::compression::zstd_compress;

const COUNTER_REF_COUNTS: &str = include_str!("counter_ref_counts.csv");
const SNAPSHOT_COMPRESSION_LEVEL: i32 = 1;
const BLOCK_NEW_CELLS: u64 = 400_000;

fn real_distribution_ops(c: &mut Criterion) {
    let real = build_real_counters();
    let mut group = c.benchmark_group("counters_real_distribution");

    group.throughput(Throughput::Elements(BLOCK_NEW_CELLS));
    group.bench_function("get_existing_400k_realistic_sample", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for &(idx, _old_raw) in &real.sampled_existing {
                sum = sum.wrapping_add(real.counters.get(idx));
            }
            black_box(sum);
        });
    });

    group.bench_function("get_implicit_one_400k_realistic_sample", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for &idx in &real.implicit {
                sum = sum.wrapping_add(real.counters.get(idx));
            }
            black_box(sum);
        });
    });

    group.bench_function("store_new_cells_400k_normal_batch", |b| {
        b.iter_batched(
            || clone_with_batch_capacity(&real.counters, real.normal_new_counts.len()),
            |mut counters| {
                fill_new_cell_transitions(&mut counters, &real.normal_new_counts);
                counters.shrink_if_needed();
                black_box(counters);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("bump_existing_400k_realistic_batch", |b| {
        b.iter_batched(
            || clone_with_batch_capacity(&real.counters, real.sampled_existing.len()),
            |mut counters| {
                let mut counter_batch = counters.begin();
                for &(idx, old_raw) in &real.sampled_existing {
                    counter_batch.update_raw(
                        idx,
                        old_raw,
                        old_raw.checked_add(1).expect("fixture refcount overflow"),
                    );
                }
                counter_batch.apply();
                counters.shrink_if_needed();
                black_box(counters);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("decrement_existing_400k_realistic_batch", |b| {
        b.iter_batched(
            || clone_with_batch_capacity(&real.counters, real.sampled_existing.len()),
            |mut counters| {
                let mut counter_batch = counters.begin();
                for &(idx, old_raw) in &real.sampled_existing {
                    counter_batch.update_raw(idx, old_raw, old_raw - 1);
                }
                counter_batch.apply();
                counters.shrink_if_needed();
                black_box(counters);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("delete_existing_400k_realistic_batch", |b| {
        b.iter_batched(
            || clone_with_batch_capacity(&real.counters, real.sampled_existing.len()),
            |mut counters| {
                let mut counter_batch = counters.begin();
                for &(idx, old_raw) in &real.sampled_existing {
                    counter_batch.update_raw(idx, old_raw, 0);
                }
                counter_batch.apply();
                counters.shrink_if_needed();
                black_box(counters);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn edge_distribution_ops(c: &mut Criterion) {
    let real = build_real_counters();
    let mut group = c.benchmark_group("counters_edge_distribution");

    group.throughput(Throughput::Elements(BLOCK_NEW_CELLS));
    for (name, new_raw) in [
        ("implicit_400k_ones_to_8_batch", 8),
        ("implicit_400k_ones_to_256_batch", 256),
        ("implicit_400k_ones_to_257_batch", 257),
        ("implicit_400k_ones_to_zero_batch", 0),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || clone_with_batch_capacity(&real.counters, real.implicit.len()),
                |mut counters| {
                    let mut counter_batch = counters.begin();
                    for &idx in &real.implicit {
                        counter_batch.update_raw(idx, 1, new_raw);
                    }
                    counter_batch.apply();
                    counters.shrink_if_needed();
                    black_box(counters);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.bench_function("implicit_400k_weird_mixed_batch", |b| {
        b.iter_batched(
            || clone_with_batch_capacity(&real.counters, real.implicit.len()),
            |mut counters| {
                let mut counter_batch = counters.begin();
                for (i, &idx) in real.implicit.iter().enumerate() {
                    let new_raw = match i % 4 {
                        0 => 8,
                        1 => 256,
                        2 => 257,
                        _ => 0,
                    };
                    counter_batch.update_raw(idx, 1, new_raw);
                }
                counter_batch.apply();
                counters.shrink_if_needed();
                black_box(counters);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn snapshot_ops(c: &mut Criterion) {
    let mut real = build_real_counters();
    let raw = real.counters.serialize();
    let mut compressed = Vec::new();
    zstd_compress(&raw, &mut compressed, SNAPSHOT_COMPRESSION_LEVEL);

    let mut group = c.benchmark_group("counters_snapshot");

    group.bench_function(
        format!(
            "serialize/raw_bytes_{}_compressed_bytes_{}",
            raw.len(),
            compressed.len()
        ),
        |b| {
            b.iter(|| black_box(real.counters.serialize()));
        },
    );

    group.bench_function("deserialize", |b| {
        b.iter(|| black_box(Counters::deserialize_trusted(&raw, pool()).unwrap()));
    });

    group.bench_function("compress_zstd_level_1", |b| {
        b.iter(|| {
            let mut compressed = Vec::new();
            zstd_compress(&raw, &mut compressed, SNAPSHOT_COMPRESSION_LEVEL);
            black_box(compressed);
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = real_distribution_ops, edge_distribution_ops, snapshot_ops
}
criterion_main!(benches);

#[derive(Clone, Copy)]
struct RefCountGroup {
    ref_count: u64,
    count: u64,
}

struct Distribution {
    groups: Vec<RefCountGroup>,
    rows: u64,
    stored: u64,
    small: u64,
    big: u64,
}

#[derive(Clone)]
struct RealCounters {
    counters: Counters,
    normal_new_counts: Vec<u64>,
    sampled_existing: Vec<(Idx, u64)>,
    implicit: Vec<Idx>,
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
}

fn pool() -> Arc<rayon::ThreadPool> {
    static POOL: LazyLock<Arc<rayon::ThreadPool>> = LazyLock::new(|| {
        Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        )
    });

    Arc::clone(&POOL)
}

fn load_distribution() -> Distribution {
    let mut groups = Vec::new();
    let mut rows = 0u64;
    let mut stored = 0u64;
    let mut small = 0u64;
    let mut big = 0u64;

    for line in COUNTER_REF_COUNTS.lines().skip(1) {
        let (ref_count, count) = line.split_once(',').expect("invalid counter fixture row");
        let ref_count = ref_count.parse::<u64>().expect("invalid refcount");
        let count = count.parse::<u64>().expect("invalid refcount count");

        rows += count;
        if ref_count > 1 {
            stored += count;
            if ref_count <= 256 {
                small += count;
            } else {
                big += count;
            }
        }

        groups.push(RefCountGroup { ref_count, count });
    }

    assert_eq!(rows, 729_305_529);
    assert_eq!(stored, 2_037_799);
    assert_eq!(small, 2_028_274);
    assert_eq!(big, 9_525);
    assert_eq!(groups.len(), 3_455);

    Distribution {
        groups,
        rows,
        stored,
        small,
        big,
    }
}

fn idx_for_stored_ordinal(ordinal: u64, distribution: &Distribution) -> Idx {
    let idx = (u128::from(ordinal) * u128::from(distribution.rows)
        / u128::from(distribution.stored)) as u64;
    Idx::new(idx)
}

fn sample_ref_counts(distribution: &Distribution, len: usize) -> Vec<u64> {
    let mut result = Vec::with_capacity(len);
    let mut group_idx = 0usize;
    let mut group_end = distribution.groups[0].count;

    for i in 0..len {
        let target = (i as u128 * u128::from(distribution.rows) / len as u128) as u64;
        while target >= group_end {
            group_idx += 1;
            group_end += distribution.groups[group_idx].count;
        }
        result.push(distribution.groups[group_idx].ref_count);
    }

    result
}

fn sample_implicit_idxs(rows: u64, stored: &[u64], len: usize) -> Vec<Idx> {
    let mut result = Vec::with_capacity(len);
    let implicit = rows - stored.len() as u64;
    let mut stored = stored.iter().copied().peekable();

    for i in 0..len {
        let mut raw = (i as u128 * u128::from(implicit) / len as u128) as u64;
        while stored.peek().is_some_and(|stored| *stored <= raw) {
            stored.next();
            raw += 1;
        }
        result.push(Idx::new(raw));
    }

    result
}

fn sample_existing_rows(
    ref_counts: &[u64],
    implicit: &[Idx],
    stored_by_count: &BTreeMap<u64, Vec<Idx>>,
    rng: &mut StdRng,
) -> Vec<(Idx, u64)> {
    let mut counts = BTreeMap::<u64, usize>::new();
    for &ref_count in ref_counts {
        *counts.entry(ref_count).or_default() += 1;
    }

    let mut result = Vec::with_capacity(ref_counts.len());

    for (ref_count, amount) in counts {
        if ref_count == 1 {
            assert!(amount <= implicit.len());
            result.extend(
                implicit
                    .choose_multiple(rng, amount)
                    .copied()
                    .map(|idx| (idx, ref_count)),
            );
            continue;
        }

        let idxs = stored_by_count
            .get(&ref_count)
            .expect("sampled refcount must exist in stored rows");
        assert!(amount <= idxs.len());
        result.extend(
            idxs.choose_multiple(rng, amount)
                .copied()
                .map(|idx| (idx, ref_count)),
        );
    }

    result
}

fn build_real_counters() -> RealCounters {
    let distribution = load_distribution();
    let mut counters = Counters::new(NextIdx::new(distribution.rows), pool());
    let mut counter_batch = counters.begin();
    let mut stored_by_count = BTreeMap::<u64, Vec<Idx>>::new();
    let mut stored_raw_idxs = Vec::with_capacity(distribution.stored as usize);

    let mut ordinal = 0u64;
    let mut small_seen = 0u64;
    let mut big_seen = 0u64;
    for group in &distribution.groups {
        if group.ref_count == 1 {
            continue;
        }

        for _ in 0..group.count {
            let idx = idx_for_stored_ordinal(ordinal, &distribution);
            stored_by_count
                .entry(group.ref_count)
                .or_default()
                .push(idx);
            stored_raw_idxs.push(idx.get());
            counter_batch.update_raw(idx, 1, group.ref_count);

            if group.ref_count <= 256 {
                small_seen += 1;
            } else {
                big_seen += 1;
            }

            ordinal += 1;
        }
    }

    assert_eq!(ordinal, distribution.stored);
    assert_eq!(small_seen, distribution.small);
    assert_eq!(big_seen, distribution.big);

    counter_batch.apply();
    counters = with_total_cells(counters, distribution.rows);
    counters.shrink_if_needed();

    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_FEED_DEAD);
    let mut normal_new_counts = sample_ref_counts(&distribution, BLOCK_NEW_CELLS as usize);
    let sampled_ref_counts = sample_ref_counts(&distribution, BLOCK_NEW_CELLS as usize);
    let mut implicit = sample_implicit_idxs(
        distribution.rows,
        &stored_raw_idxs,
        BLOCK_NEW_CELLS as usize,
    );
    let mut sampled_existing =
        sample_existing_rows(&sampled_ref_counts, &implicit, &stored_by_count, &mut rng);

    normal_new_counts.shuffle(&mut rng);
    implicit.shuffle(&mut rng);
    sampled_existing.shuffle(&mut rng);

    RealCounters {
        counters,
        normal_new_counts,
        sampled_existing,
        implicit,
    }
}

fn with_total_cells(mut counters: Counters, total_cells: u64) -> Counters {
    let offset = size_of::<u8>() + size_of::<u64>();
    let mut snapshot = counters.serialize();
    (&mut snapshot[offset..offset + size_of::<u64>()]).put_u64_le(total_cells);
    Counters::deserialize_trusted(&snapshot, pool()).unwrap()
}

fn fill_new_cell_transitions(counters: &mut Counters, counts: &[u64]) {
    let mut batch = counters.begin();
    for &new_raw in counts {
        let idx = batch.alloc_idx();
        batch.update_raw(idx, 0, new_raw);
    }
    batch.apply();
}

fn clone_with_batch_capacity(counters: &Counters, capacity: usize) -> Counters {
    let mut counters = counters.clone();
    let mut batch = counters.begin();
    batch.reserve(capacity);
    batch.apply();
    counters
}
