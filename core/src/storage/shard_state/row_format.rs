use bytes::{Buf, BufMut};

use super::counters::Idx;

pub fn encode_indexed_value(idx: Idx, data: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(size_of::<u64>() + data.len());
    out.put_u64_le(idx.get());
    out.put_slice(data);
}

pub fn decode_indexed_value(mut value: &[u8]) -> Option<(Idx, &[u8])> {
    // Prefix-only split. Payload validity is caller-owned because some paths
    // only need the idx, while load/delete paths validate cell bytes later.
    if value.remaining() < size_of::<u64>() {
        return None;
    }
    let idx = Idx::new(value.get_u64_le());
    Some((idx, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_value_helpers_roundtrip() {
        let idx = Idx::new(42);
        let payload = [1u8, 2, 3, 4];
        let mut value = Vec::new();

        encode_indexed_value(idx, &payload, &mut value);

        assert_eq!(
            decode_indexed_value(&value),
            Some((idx, payload.as_slice()))
        );

        // value shorter than index is rejected
        assert_eq!(decode_indexed_value(&[1, 2, 3]), None);
    }
}
