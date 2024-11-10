use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tycho_util::time::P2;

fn bench_p2_operations(c: &mut Criterion) {
    c.bench_function("p2_single_insert", |b| {
        b.iter_with_setup(
            || P2::new(0.95).unwrap(),
            |mut p2| {
                p2.append(black_box(42));
            },
        );
    });

    c.bench_function("p2_single_value_read", |b| {
        let mut p2 = P2::new(0.95).unwrap();
        // Pre-fill with some data
        for i in 0..100 {
            p2.append(i);
        }

        b.iter(|| {
            black_box(p2.value());
        });
    });

    c.bench_function("p2_amortized_insert", |b| {
        b.iter_with_setup(
            || P2::new(0.95).unwrap(),
            |mut p2| {
                for i in 0..1000 {
                    p2.append(black_box(i));
                }
            },
        );
    });

    // 4. Insert + read pattern (default pattern for timeout estimator)
    c.bench_function("p2_insert_and_read", |b| {
        b.iter_with_setup(
            || P2::new(0.95).unwrap(),
            |mut p2| {
                p2.append(black_box(42));
                black_box(p2.value());
            },
        );
    });
}

criterion_group!(benches, bench_p2_operations);
criterion_main!(benches);
