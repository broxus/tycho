use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};

use tycho_network::PeerId;

use crate::models::{Digest, Point, Round, UnixTime};
use crate::moderator::journal::event::JournalEvent;
use crate::moderator::{EventTag, RecordKey, RecordKind};

pub struct JournalItemFull {
    pub key: RecordKey,
    pub item: JournalItem,
}

pub enum JournalItem {
    NodeStarted(PeerId, String),
    Banned(BanItem),
    Unbanned(UnbanItem),
    Event(JournalEvent),
}

impl JournalItem {
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::NodeStarted(_, _) => RecordKind::NodeStarted,
            Self::Banned(data) => RecordKind::Banned(data.until),
            Self::Unbanned(_) => RecordKind::Unbanned,
            Self::Event(event) => RecordKind::Event(event.tag()),
        }
    }
    pub fn peer_id(&self) -> &PeerId {
        match self {
            Self::NodeStarted(peer_id, _) => peer_id,
            Self::Banned(data) => &data.peer_id,
            Self::Unbanned(data) => &data.peer_id,
            Self::Event(event) => event.peer_id(),
        }
    }
    pub fn action(&self) -> JournalAction {
        match self {
            JournalItem::NodeStarted(_, _) => JournalAction::Store,
            JournalItem::Banned(_) | JournalItem::Unbanned(_) => JournalAction::StoreAndCheckBan,
            JournalItem::Event(event) => event.action(),
        }
    }
    pub fn fill_points<'a>(
        &'a self,
        points: &mut Vec<&'a Point>,
        point_keys: &mut Vec<(Round, &'a Digest)>,
    ) {
        match self {
            Self::NodeStarted(_, _) | Self::Banned { .. } | Self::Unbanned { .. } => {}
            Self::Event(event) => event.fill_points(points, point_keys),
        }
    }
}

impl Display for JournalItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeStarted(_, msg) => {
                write!(f, "started {msg}")
            }
            Self::Banned(data) => {
                let system_time =
                    SystemTime::UNIX_EPOCH + Duration::from_millis(data.until.millis());
                let human_time = humantime::format_rfc3339_millis(system_time);
                write!(f, "ban until {human_time} {}", data.origin)
            }
            Self::Unbanned(data) => {
                write!(f, "ban removed {}", &data.origin)
            }
            Self::Event(event) => Display::fmt(event, f),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum JournalAction {
    /// No action needed
    Ignore,
    /// Does not participate in bans
    Store,
    /// Participates in bans but should not be stored (assume already stored)
    CheckBan,
    /// May become a reason for a ban according to config
    StoreAndCheckBan,
}
impl JournalAction {
    pub fn store(&self) -> bool {
        match self {
            JournalAction::Ignore | JournalAction::CheckBan => false,
            JournalAction::Store | JournalAction::StoreAndCheckBan => true,
        }
    }
    pub fn is_ban_related(&self) -> bool {
        match self {
            JournalAction::Ignore | JournalAction::Store => false,
            JournalAction::CheckBan | JournalAction::StoreAndCheckBan => true,
        }
    }
}

#[derive(Clone)]
pub struct BanItem {
    pub peer_id: PeerId,
    pub until: UnixTime,
    pub origin: ItemOrigin,
}

pub struct UnbanItem {
    pub peer_id: PeerId,
    pub origin: ItemOrigin,
}

#[derive(Debug, Clone, Copy)]
pub enum ItemOrigin {
    /// Record that triggered the ban or a ban that was unbanned
    Parent {
        key: RecordKey,
        tag: Option<EventTag>,
    },
}

impl Display for ItemOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parent { key, tag } => match tag {
                None => write!(f, "auto without tag by {key}"),
                Some(tag) => write!(f, "auto by {tag:?} {key}"),
            },
        }
    }
}
