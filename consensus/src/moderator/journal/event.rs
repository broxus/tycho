use std::fmt::{Display, Formatter};

use tycho_network::PeerId;

use crate::models::{Point, PointKey};
use crate::moderator::EventTag;
use crate::moderator::journal::item::JournalAction;

pub enum JournalEvent {}

impl JournalEvent {
    pub fn tag(&self) -> EventTag {
        todo!()
    }

    pub fn peer_id(&self) -> &PeerId {
        todo!()
    }

    pub fn action(&self) -> JournalAction {
        todo!()
    }

    pub fn fill_points<'a>(
        &'a self,
        _points: &mut Vec<&'a Point>,
        _point_keys: &mut Vec<PointKey>,
    ) {
        todo!()
    }
}

impl Display for JournalEvent {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
