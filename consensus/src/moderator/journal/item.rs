use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};

use tycho_network::PeerId;

use crate::models::{Point, PointKey, UnixTime};
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
    pub fn fill_points<'a>(&'a self, points: &mut Vec<&'a Point>, point_keys: &mut Vec<PointKey>) {
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
    /// May become a reason for a ban according to config
    StoreAndCheckBan,
}
impl JournalAction {
    pub fn store(&self) -> bool {
        match self {
            JournalAction::Ignore => false,
            JournalAction::Store | JournalAction::StoreAndCheckBan => true,
        }
    }
    pub fn is_ban_related(&self) -> bool {
        match self {
            JournalAction::Ignore | JournalAction::Store => false,
            JournalAction::StoreAndCheckBan => true,
        }
    }
}

#[derive(Clone)]
pub struct BanItem {
    pub peer_id: PeerId,
    pub until: UnixTime,
    pub origin: BanOrigin,
}

#[derive(Debug, Clone, Copy)]
pub enum BanOrigin {
    Manual,
    Parent { key: RecordKey, tag: EventTag },
}

pub struct UnbanItem {
    pub peer_id: PeerId,
    pub origin: UnbanOrigin,
}

#[derive(Debug, Clone, Copy)]
pub enum UnbanOrigin {
    Manual,
    Parent(RecordKey),
}

impl Display for BanOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manual => f.write_str("manual"),
            Self::Parent { key, tag } => write!(
                f,
                "parent {tag:?} key {} # {}",
                key.created.millis(),
                key.seq_no()
            ),
        }
    }
}

impl Display for UnbanOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manual => f.write_str("manual"),
            Self::Parent(key) => {
                write!(f, "parent key {} # {}", key.created.millis(), key.seq_no())
            }
        }
    }
}
