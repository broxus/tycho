use serde::{Deserialize, Serialize};
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::models::UnixTime;
use crate::moderator::RecordKey;

#[derive(TlRead, TlWrite)]
#[tl(boxed, id = "journal.recordValue", scheme = "proto.tl")]
pub struct RecordValue {
    pub kind: RecordKind,
    pub action: RecordAction,
    pub peer_id: PeerId,
    /// ordered (Round, Digest) in big endian encoding ready to join points by id
    pub point_keys: Vec<[u8; crate::storage::POINT_KEY_LEN]>,
    #[tl(with = "tl_string")]
    pub message: String,
}

#[derive(TlRead)]
#[tl(boxed, id = "journal.recordValue", scheme = "proto.tl")]
pub struct RecordValueShort {
    pub kind: RecordKind,
    pub action: RecordAction,
    pub peer_id: PeerId,
}

#[derive(Debug, Clone, Copy, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum RecordKind {
    #[tl(id = "journal.recordKind.nodeStarted")]
    NodeStarted,
    #[tl(id = "journal.recordKind.banned")]
    Banned {
        until: UnixTime,
        origin: RecordOrigin,
    },
    #[tl(id = "journal.recordKind.unbanned")]
    Unbanned(RecordOrigin),
    #[tl(id = "journal.recordKind.event")]
    Event(EventTag),
}

#[derive(Debug, Clone, Copy, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum RecordAction {
    /// Does not participate in bans
    #[tl(id = "journal.recordAction.store")]
    Store,
    /// Participates in bans but should not be stored (assume already stored)
    #[tl(id = "journal.recordAction.countPenalty")]
    CountPenalty,
    /// May become a reason for a ban according to config
    #[tl(id = "journal.recordAction.storeAndCountPenalty")]
    StoreAndCountPenalty,
}

#[derive(Debug, Clone, Copy, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum RecordOrigin {
    #[tl(id = "journal.recordOrigin.manual")]
    Manual,
    /// Record that triggered the ban or a ban that was unbanned
    #[tl(id = "journal.recordOrigin.parentKey")]
    ParentKey(RecordKey),
    /// Record that triggered the ban or a ban that was unbanned
    #[tl(id = "journal.recordOrigin.parentKeyTag")]
    ParentKeyTag { key: RecordKey, tag: EventTag },
}

impl std::fmt::Display for RecordOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manual => f.write_str("manually"),
            Self::ParentKey(_) => f.write_str("auto by no event"),
            Self::ParentKeyTag { tag, .. } => write!(f, "auto by {tag:?} event"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TlRead, TlWrite, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[tl(boxed, scheme = "proto.tl")]
pub enum EventTag {
    #[tl(id = "journal.eventTag.unknownQuery")]
    UnknownQuery,
    #[tl(id = "journal.eventTag.queryLimitReached")]
    QueryLimitReached,
    #[tl(id = "journal.eventTag.badRequest")]
    BadRequest,
    #[tl(id = "journal.eventTag.badResponse")]
    BadResponse,
    #[tl(id = "journal.eventTag.badPoint")]
    BadPoint,
    #[tl(id = "journal.eventTag.senderNotAuthor")]
    SenderNotAuthor,
    #[tl(id = "journal.eventTag.illFormed")]
    IllFormed,
    #[tl(id = "journal.eventTag.invalid")]
    Invalid,
    #[tl(id = "journal.eventTag.equivocated")]
    Equivocated,
    #[tl(id = "journal.eventTag.evidenceNoInclusion")]
    EvidenceNoInclusion,
}

impl EventTag {
    pub const VALUES: [Self; 10] = {
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
            UnknownQuery,
            QueryLimitReached,
            BadRequest,
            BadResponse,
            BadPoint,
            SenderNotAuthor,
            IllFormed,
            Invalid,
            Equivocated,
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
