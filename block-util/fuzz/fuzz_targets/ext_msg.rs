#![no_main]
#![allow(clippy::disallowed_methods)]

use libfuzzer_sys::fuzz_target;
use tycho_block_util::message::{ExtMsgRepr, normalize_external_message};
use tycho_types::prelude::Boc;

fuzz_target!(|data: &[u8]| {
    let Ok(cell) = ExtMsgRepr::validate(data) else {
        return;
    };

    // If validate succeeded, normalize must also succeed.
    let normalized =
        normalize_external_message(cell.as_ref()).expect("validated message must normalize");

    // Normalized BOC must re-validate.
    let normalized_boc = Boc::encode(&normalized);
    let reparsed =
        ExtMsgRepr::validate(&normalized_boc).expect("normalized message must re-validate");

    // Normalization must be idempotent.
    let renormalized = normalize_external_message(reparsed.as_ref())
        .expect("normalized message must normalize again");

    assert_eq!(
        normalized.repr_hash(),
        renormalized.repr_hash(),
        "normalization must be idempotent"
    );
});
