use std::sync::atomic;
use std::sync::atomic::AtomicU32;

use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

use crate::models::UnixTime;

/// Resets on reboot and wraps on overflow
static RECORD_SEQ_NO: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct RecordKey {
    pub created: UnixTime,
    /// May be stored with gaps (some may be discarded during restore), but order is historical
    seq_no: u32,
}

impl RecordKey {
    pub const _TL_ID: u32 = tl_proto::id!("journal.recordKey", scheme = "proto.tl");
    pub const MAX_TL_BYTES: usize = UnixTime::MAX_TL_BYTES + size_of::<u32>();

    pub fn new() -> Self {
        Self {
            created: UnixTime::now(),
            seq_no: RECORD_SEQ_NO.fetch_add(1, atomic::Ordering::Relaxed),
        }
    }
}

/// Manual encoding because we need big-endian key in rocksdb for ranged deletes
impl TlWrite for RecordKey {
    type Repr = tl_proto::Bare;

    fn max_size_hint(&self) -> usize {
        Self::MAX_TL_BYTES
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        packet.write_raw_slice(&self.created.millis().to_be_bytes());
        packet.write_raw_slice(&self.seq_no.to_be_bytes());
    }
}

/// Manual encoding because we need big-endian key in rocksdb for ranged deletes
impl<'tl> TlRead<'tl> for RecordKey {
    type Repr = tl_proto::Bare;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if packet.len() < Self::MAX_TL_BYTES {
            return Err(tl_proto::TlError::UnexpectedEof);
        }
        Ok(Self {
            created: UnixTime::from_millis(u64::from_be_bytes(<_>::read_from(packet)?)),
            seq_no: u32::from_be_bytes(<_>::read_from(packet)?),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_event_key_tl() {
        _ = RecordKey::new(); // skip a key with zero seq_no
        let key = RecordKey::new();
        assert!(key.seq_no > 0, "zero seq_no must be skipped");
        let mut buf = Vec::new();
        key.write_to(&mut buf);
        let k2 = RecordKey::read_from(&mut &buf[..]).expect("read event key");
        assert_eq!(key, k2, "event key after tl must match");
    }
}
