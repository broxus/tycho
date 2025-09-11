use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tycho_collator::collator::bench_export::{
    IncludeAllMessages, MessageGroup, MessagesBuffer, make_stub_internal_parsed_message,
};
use tycho_types::cell::HashBytes;
use tycho_types::models::{IntAddr, ShardIdent, StdAddr};

fn config() -> Criterion {
    Criterion::default().measurement_time(std::time::Duration::from_secs(40))
}

fn make_buffer(accounts: usize, msgs_per_account: usize, shard: ShardIdent) -> MessagesBuffer {
    let mut buf = MessagesBuffer::default();
    let mut rng = StdRng::seed_from_u64(42);
    let mut lt: u64 = 1;
    for _ in 0..accounts {
        let rand_bytes: [u8; 32] = rng.random();
        let addr = IntAddr::Std(StdAddr::new(shard.workchain() as i8, HashBytes(rand_bytes)));
        let count = rng.random_range(5..=msgs_per_account.max(5));
        for _ in 0..count {
            buf.add_message(make_stub_internal_parsed_message(
                shard,
                addr.clone(),
                lt,
                false,
            ));
            lt += 1;
        }
    }
    buf
}

fn fill_message_group_benchmark(c: &mut Criterion) {
    let shard = ShardIdent::new_full(0);

    let mut group = c.benchmark_group("fill_message_group");
    for (accounts, per_acc) in [
        black_box((100, 100)),
        black_box((1000, 100)),
        black_box((10000, 100)),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("a{}_m{}", accounts, per_acc)),
            &(accounts, per_acc),
            |b, &(accounts, per_acc)| {
                b.iter_batched(
                    || {
                        let buf1 = make_buffer(accounts, per_acc, shard);
                        let buf2 = make_buffer(accounts, per_acc, shard);
                        (buf1, buf2)
                    },
                    |(mut buf1, mut buf2)| {
                        let slots_count = 100;
                        let slot_vert_size = 10;
                        let mut filter = IncludeAllMessages;
                        for _ in 0..20 {
                            let mut msg_group = MessageGroup::default();
                            let _res = buf1.fill_message_group(
                                &mut msg_group,
                                slots_count,
                                slot_vert_size,
                                |_| (false, 0),
                                &mut filter,
                            );
                            let _res = buf2.fill_message_group(
                                &mut msg_group,
                                slots_count,
                                slot_vert_size,
                                |acc_id| {
                                    let exist = buf1.account_messages_count(acc_id) > 0;
                                    (exist, 0)
                                },
                                &mut filter,
                            );
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = fill_message_group_benchmark
}
criterion_main!(benches);
