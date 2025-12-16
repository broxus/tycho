use serde::{Deserialize, Serialize};
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::models::{PointKey, UnixTime};
use crate::moderator::RecordKey;

#[cfg_attr(any(test, feature = "test"), derive(Debug, PartialEq))]
pub struct RecordFull {
    pub key: RecordKey,
    pub value: RecordValue,
}

#[derive(TlRead, TlWrite)]
#[cfg_attr(any(test, feature = "test"), derive(Debug, PartialEq))]
#[tl(boxed, id = "journal.recordValue", scheme = "proto.tl")]
pub struct RecordValue {
    pub is_ban_related: bool,
    pub kind: RecordKind,
    pub peer_id: PeerId,
    pub point_keys: Vec<PointKey>,
    #[tl(with = "tl_string")]
    pub message: String,
}

#[derive(TlRead)]
#[tl(boxed, id = "journal.recordValue", scheme = "proto.tl")]
pub struct RecordValueShort {
    pub is_ban_related: bool,
    pub kind: RecordKind,
    pub peer_id: PeerId,
}

#[derive(Debug, Clone, Copy, TlRead, TlWrite)]
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
#[tl(boxed, scheme = "proto.tl")]
pub enum RecordKind {
    #[tl(id = "journal.recordKind.nodeStarted")]
    NodeStarted,
    #[tl(id = "journal.recordKind.banned")]
    Banned(UnixTime),
    #[tl(id = "journal.recordKind.unbanned")]
    Unbanned,
    #[tl(id = "journal.recordKind.event")]
    Event(EventTag),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TlRead, TlWrite, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[tl(boxed, scheme = "proto.tl")]
pub enum EventTag {
    #[tl(id = "journal.eventTag.badQuery")]
    BadQuery,
    #[tl(id = "journal.eventTag.queryLimitReached")]
    QueryLimitReached,
    #[tl(id = "journal.eventTag.pointIntegrityError")]
    PointIntegrityError,
    #[tl(id = "journal.eventTag.replacedPoint")]
    ReplacedPoint,
    #[tl(id = "journal.eventTag.illFormedPoint")]
    IllFormedPoint,
    #[tl(id = "journal.eventTag.invalidPoint")]
    InvalidPoint,
    #[tl(id = "journal.eventTag.forkedPoint")]
    ForkedPoint,
    #[tl(id = "journal.eventTag.evidenceNoInclusion")]
    EvidenceNoInclusion,
}

impl EventTag {
    pub const VALUES: [Self; 8] = {
        macro_rules! enum_exhaustive {
            ($Enum:path, $($variant:ident),* $(,)?) => {
                {
                    use $Enum as Enum;
                    // pattern match won't compile if some branch is missed
                    let _ = |dummy: Enum| {
                        #[deny(unreachable_patterns, reason = "reject duplicated variant")]
                        match dummy {
                            $(Enum::$variant => ()),*
                        }
                    };
                    [$(Enum::$variant),*]
                }
            }
        }
        enum_exhaustive!(
            EventTag,
            BadQuery,
            QueryLimitReached,
            PointIntegrityError,
            ReplacedPoint,
            IllFormedPoint,
            InvalidPoint,
            ForkedPoint,
            EvidenceNoInclusion,
        )
    };
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
    fn check_string_tl() {
        let data = "Hello world!";
        let mut buf = Vec::new();
        tl_string::write(data, &mut buf);
        let d2 = tl_string::read(&mut &buf[..]).expect("read tl string");
        assert_eq!(data, d2, "string after tl must match");
    }
}
