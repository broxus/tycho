use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use tycho_block_util::message::ExtMsgRepr;
use tycho_types::prelude::Boc;

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
