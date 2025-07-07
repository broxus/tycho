use base64::prelude::Engine as _;
use criterion::{Criterion, criterion_group, criterion_main};
use tycho_block_util::message::MsgStorageStat;
use tycho_types::boc::Boc;
use tycho_types::cell::{Cell, CellTreeStats};
use tycho_types::models::MsgInfo;
use tycho_types::prelude::{CellFamily, Load};

use self::common::create_big_message;

mod common;

fn decode_benchmark(c: &mut Criterion) {
    let boc = {
        let cell = create_big_message().unwrap();
        Boc::encode_base64(&cell)
    };

    fn decode_base64_impl(data: &[u8]) -> Result<Vec<u8>, base64::DecodeError> {
        base64::engine::general_purpose::STANDARD.decode(data)
    }

    c.bench_function("decode-base64", |b| {
        b.iter(|| decode_base64_impl(boc.as_ref()).unwrap());
    });

    let x = decode_base64_impl(boc.as_ref()).unwrap();

    c.bench_function("boc-decode-ext-base64", |b| {
        b.iter(|| Boc::decode_ext(x.as_ref(), Cell::empty_context()));
    });

    let result = Boc::decode_ext(x.as_ref(), Cell::empty_context()).unwrap();

    c.bench_function("owned-message-load", |b| {
        b.iter(|| MsgInfo::load_from(&mut result.as_slice().unwrap()).unwrap());
    });
    let cs = &mut result.as_slice().unwrap();
    let _ = MsgInfo::load_from(cs).unwrap();

    c.bench_function("traverse", |b| {
        b.iter(|| {
            MsgStorageStat::check_slice(cs, 2, CellTreeStats {
                bit_count: 1 << 21,
                cell_count: 1 << 13,
            })
        });
    });
}

criterion_group!(benches, decode_benchmark);
criterion_main!(benches);
