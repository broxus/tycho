use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use everscale_types::prelude::Boc;
use tycho_block_util::message::ExtMsgRepr;

use self::common::create_big_message;

mod common;

fn big_message_benchmark(c: &mut Criterion) {
    let boc = {
        let cell = create_big_message().unwrap();
        Boc::encode(&cell)
    };

    let bytes = Bytes::from(boc);
    c.bench_function("big-message", |b| {
        b.iter(|| {
            let _ = ExtMsgRepr::validate(bytes.clone());
        });
    });
}

criterion_group!(benches, big_message_benchmark);
criterion_main!(benches);
