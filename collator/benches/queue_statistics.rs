use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use tycho_collator::internal_queue::types::stats::{AccountStatistics, QueueStatistics};
use tycho_types::cell::HashBytes;
use tycho_types::models::{IntAddr, StdAddr};
use tycho_util::transactional::Transactional;
use tycho_util::{FastHashMap, FastHashSet};

const ACCOUNTS_COUNT: usize = 1_000_000;
const SPARSE_CHANGED_COUNT: usize = 50_000;
const HOTSET_CHANGED_COUNT: usize = 5_000;
const TX_OPS_COUNT: usize = 50_000;

fn config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(20))
}

fn make_addr(i: u32) -> IntAddr {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&i.to_le_bytes());
    IntAddr::Std(StdAddr::new(0, HashBytes(bytes)))
}

fn build_base_stats(accounts_count: usize) -> QueueStatistics {
    let mut accounts: AccountStatistics = FastHashMap::default();
    accounts.reserve(accounts_count);

    for i in 0..accounts_count as u32 {
        accounts.insert(make_addr(i), 1);
    }

    QueueStatistics::with_statistics(accounts)
}

fn build_prepared_stats_for_commit(accounts_count: usize, changed_count: usize) -> QueueStatistics {
    assert!(changed_count <= accounts_count);

    let mut stats = build_base_stats(accounts_count);
    stats.begin();

    for i in 0..changed_count as u32 {
        stats.increment_for_account(make_addr(i), 1);
    }

    stats
}

fn build_increment_ops(unique_accounts: usize, ops_count: usize) -> Vec<IntAddr> {
    assert!(unique_accounts > 0);
    let mut ops = Vec::with_capacity(ops_count);
    for i in 0..ops_count {
        ops.push(make_addr((i % unique_accounts) as u32));
    }
    ops
}

fn queue_statistics_commit_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_statistics_commit");
    group.throughput(Throughput::Elements(ACCOUNTS_COUNT as u64));

    for (name, changed_count) in [
        ("1m_accounts_all_staged", ACCOUNTS_COUNT),
        ("1m_accounts_50k_staged", SPARSE_CHANGED_COUNT),
    ] {
        let prepared = build_prepared_stats_for_commit(ACCOUNTS_COUNT, changed_count);

        group.bench_function(name, |b| {
            b.iter_batched(
                || prepared.clone(),
                |mut stats| {
                    stats.commit();
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn dirty_accounts_allocation_benchmark(c: &mut Criterion) {
    let addresses: Vec<IntAddr> = (0..SPARSE_CHANGED_COUNT as u32).map(make_addr).collect();

    let mut group = c.benchmark_group("dirty_accounts_allocation");
    group.throughput(Throughput::Elements(SPARSE_CHANGED_COUNT as u64));

    group.bench_function("vec_50k_clone", |b| {
        b.iter(|| {
            let mut dirty = Vec::with_capacity(SPARSE_CHANGED_COUNT);
            dirty.extend(addresses.iter().cloned());
            black_box(dirty);
        });
    });

    group.bench_function("fast_hash_set_50k_insert", |b| {
        b.iter(|| {
            let mut dirty: FastHashSet<IntAddr> = FastHashSet::default();
            dirty.reserve(SPARSE_CHANGED_COUNT);
            for addr in &addresses {
                dirty.insert(addr.clone());
            }
            black_box(dirty);
        });
    });

    group.finish();
}

fn queue_statistics_tx_total_benchmark(c: &mut Criterion) {
    let base = build_base_stats(ACCOUNTS_COUNT);
    let ops_50k_unique = build_increment_ops(SPARSE_CHANGED_COUNT, TX_OPS_COUNT);
    let ops_5k_unique = build_increment_ops(HOTSET_CHANGED_COUNT, TX_OPS_COUNT);

    let mut group = c.benchmark_group("queue_statistics_tx_total");
    group.throughput(Throughput::Elements(TX_OPS_COUNT as u64));

    for (name, ops) in [
        ("1m_base_50k_ops_50k_unique", &ops_50k_unique),
        ("1m_base_50k_ops_5k_unique", &ops_5k_unique),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || base.clone(),
                |mut stats| {
                    stats.begin();
                    for addr in ops {
                        stats.increment_for_account(addr.clone(), 1);
                    }
                    stats.commit();
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = queue_statistics_commit_benchmark, dirty_accounts_allocation_benchmark, queue_statistics_tx_total_benchmark
}
criterion_main!(benches);
