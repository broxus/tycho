use serde::Serialize;
use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

use crate::models::UnixTime;

/// Resets on reboot and wraps on overflow
#[derive(Default)]
pub struct RecordKeyFactory(u32);

impl RecordKeyFactory {
    fn next_seq_no(&mut self) -> u32 {
        let next = self.0.wrapping_add(1);
        std::mem::replace(&mut self.0, next)
    }

    pub fn new_key(&mut self) -> RecordKey {
        RecordKey {
            created: UnixTime::now(),
            seq_no: self.next_seq_no(),
        }
    }

    #[cfg(test)]
    pub fn new_millis(&mut self, created: u64) -> RecordKey {
        RecordKey {
            created: UnixTime::from_millis(created),
            seq_no: self.next_seq_no(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct RecordKey {
    pub created: UnixTime, // first field for Ord in both DB and runtime
    /// May be stored with gaps (some may be discarded during restore), but order is historical
    seq_no: u32,
}

impl RecordKey {
    pub const _TL_ID: u32 = tl_proto::id!("journal.recordKey", scheme = "proto.tl");
    pub const MAX_TL_BYTES: usize = UnixTime::MAX_TL_BYTES + size_of::<u32>();

    pub fn seq_no(&self) -> u32 {
        self.seq_no
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
        let mut key_factory = RecordKeyFactory::default();
        _ = key_factory.new_key(); // skip a key with zero seq_no
        let key = key_factory.new_key();
        assert!(key.seq_no > 0, "zero seq_no must be skipped");
        let mut buf = Vec::new();
        key.write_to(&mut buf);
        let k2 = RecordKey::read_from(&mut &buf[..]).expect("read event key");
        assert_eq!(key, k2, "event key after tl must match");
    }
}
