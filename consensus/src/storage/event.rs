use std::sync::atomic;
use std::sync::atomic::AtomicU32;

use serde::{Deserialize, Serialize};
use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};
use tycho_network::PeerId;

use crate::models::UnixTime;
use crate::storage::POINT_KEY_LEN;

/// Resets on reboot and wraps on overflow
static EVENT_SEQ_NO: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EventKey {
    pub occurred_at: UnixTime,
    seq_no: u32,
}

impl EventKey {
    pub fn new() -> Self {
        Self {
            occurred_at: UnixTime::now(),
            seq_no: EVENT_SEQ_NO.fetch_add(1, atomic::Ordering::Relaxed),
        }
    }
}

/// Manual encoding because we need big-endian key in rocksdb for ranged deletes
impl TlWrite for EventKey {
    type Repr = tl_proto::Bare;

    fn max_size_hint(&self) -> usize {
        super::EVENT_KEY_LEN
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        packet.write_raw_slice(&self.occurred_at.millis().to_be_bytes());
        packet.write_raw_slice(&self.seq_no.to_be_bytes());
    }
}

/// Manual encoding because we need big-endian key in rocksdb for ranged deletes
impl<'tl> TlRead<'tl> for EventKey {
    type Repr = tl_proto::Bare;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if packet.len() < super::EVENT_KEY_LEN {
            return Err(tl_proto::TlError::UnexpectedEof);
        }
        Ok(Self {
            occurred_at: UnixTime::from_millis(u64::from_be_bytes(<_>::read_from(packet)?)),
            seq_no: u32::from_be_bytes(<_>::read_from(packet)?),
        })
    }
}

#[derive(TlRead, TlWrite)]
#[tl(boxed, id = "consensus.storedEvent", scheme = "proto.tl")]
pub struct StoredEventData {
    pub tag: EventTag,
    pub severity: EventSeverity,
    pub peer_id: PeerId,
    /// ordered (Round, Digest) in big endian encoding ready to join points by id
    pub point_keys: Vec<[u8; POINT_KEY_LEN]>,
    #[tl(with = "tl_string")]
    pub message: String,
}

#[derive(TlRead)]
#[tl(boxed, id = "consensus.storedEvent", scheme = "proto.tl")]
pub struct ShortEventData {
    pub tag: EventTag,
    pub severity: EventSeverity,
    pub peer_id: PeerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TlRead, TlWrite, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[tl(boxed, scheme = "proto.tl")]
pub enum EventTag {
    #[tl(id = "consensus.eventTag.nodeStarted")]
    NodeStarted,
    #[tl(id = "consensus.eventTag.banned")]
    Banned,
    #[tl(id = "consensus.eventTag.unbanned")]
    Unbanned,
    #[tl(id = "consensus.eventTag.unknownQuery")]
    UnknownQuery,
    #[tl(id = "consensus.eventTag.queryLimitReached")]
    QueryLimitReached,
    #[tl(id = "consensus.eventTag.badRequest")]
    BadRequest,
    #[tl(id = "consensus.eventTag.badResponse")]
    BadResponse,
    #[tl(id = "consensus.eventTag.badPoint")]
    BadPoint,
    #[tl(id = "consensus.eventTag.senderNotAuthor")]
    SenderNotAuthor,
    #[tl(id = "consensus.eventTag.illFormed")]
    IllFormed,
    #[tl(id = "consensus.eventTag.invalid")]
    Invalid,
    #[tl(id = "consensus.eventTag.equivocated")]
    Equivocated,
    #[tl(id = "consensus.eventTag.evidenceNoInclusion")]
    EvidenceNoInclusion,
}

/// Event without severity is not stored and is simply ignored
#[derive(Debug, Clone, Copy, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum EventSeverity {
    /// Does not participate in bans
    #[tl(id = "consensus.eventSeverity.info")]
    Info,
    /// May become a reason for a ban according to config
    #[tl(id = "consensus.eventSeverity.warn")]
    Warn,
}

mod tl_string {
    use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

    pub fn size_hint(items: &str) -> usize {
        items.len()
    }

    pub fn write<P: TlPacket>(item: &str, packet: &mut P) {
        item.as_bytes().write_to(packet);
    }

    pub fn read(packet: &mut &[u8]) -> TlResult<String> {
        String::from_utf8(<_>::read_from(packet)?).map_err(|_err| TlError::InvalidData)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_event_key_tl() {
        _ = EventKey::new(); // skip a key with zero seq_no
        let key = EventKey::new();
        assert!(key.seq_no > 0, "zero seq_no must be skipped");
        let mut buf = Vec::new();
        key.write_to(&mut buf);
        let k2 = EventKey::read_from(&mut &buf[..]).expect("read event key");
        assert_eq!(key, k2, "event key after tl must match");
    }

    #[test]
    fn check_string_tl() {
        let data = "Hello world!";
        let mut buf = Vec::new();
        tl_string::write(data, &mut buf);
        let d2 = tl_string::read(&mut &buf[..]).expect("read tl string");
        assert_eq!(data, d2, "string after tl must match");
    }
}
