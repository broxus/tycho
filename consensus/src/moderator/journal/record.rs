use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};

use tycho_network::PeerId;

use crate::models::{Digest, Point, Round, UnixTime};
use crate::moderator::RecordKey;
use crate::moderator::journal::event::JournalEvent;
use crate::moderator::stored::{RecordAction, RecordKind, RecordOrigin};

pub struct JournalRecord {
    pub key: RecordKey,
    pub data: RecordData,
}

pub enum RecordData {
    NodeStarted(PeerId, String),
    Banned {
        peer_id: PeerId,
        origin: RecordOrigin,
        until: UnixTime,
    },
    Unbanned {
        peer_id: PeerId,
        origin: RecordOrigin,
    },
    Event(JournalEvent),
}

impl RecordData {
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::NodeStarted(_, _) => RecordKind::NodeStarted,
            Self::Banned { origin, until, .. } => RecordKind::Banned {
                until: *until,
                origin: *origin,
            },
            Self::Unbanned { origin, .. } => RecordKind::Unbanned(*origin),
            Self::Event(event) => RecordKind::Event(event.tag()),
        }
    }
    pub fn peer_id(&self) -> &PeerId {
        match self {
            Self::NodeStarted(peer_id, _)
            | Self::Banned { peer_id, .. }
            | Self::Unbanned { peer_id, .. } => peer_id,
            Self::Event(event) => event.peer_id(),
        }
    }
    pub fn action(&self) -> RecordAction {
        match self {
            RecordData::NodeStarted(_, _)
            | RecordData::Banned { .. }
            | RecordData::Unbanned { .. } => RecordAction::Store,
            RecordData::Event(event) => event.action(),
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

impl Display for RecordData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeStarted(_, msg) => {
                write!(f, "started {msg}")
            }
            Self::Banned { origin, until, .. } => {
                let system_time = SystemTime::UNIX_EPOCH + Duration::from_millis(until.millis());
                let numan_time = humantime::format_rfc3339_millis(system_time);
                write!(f, "ban {origin} until {numan_time}")
            }
            Self::Unbanned { origin, .. } => {
                write!(f, "ban removed {origin}")
            }
            Self::Event(event) => Display::fmt(event, f),
        }
    }
}
