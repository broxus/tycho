use std::sync::Arc;
use std::{cmp, fmt};

use cmp::Ordering;

use crate::effects::{AltFmt, AltFormat};
use crate::models::{DagPoint, Point, Round};

#[derive(Debug)]
pub enum ConsensusEvent {
    // allows not to peek but poll the channel when local dag is not ready yet
    Forward(Round),
    // well-formed, but not yet validated against DAG
    Verified(Arc<Point>),
    Invalid(DagPoint),
}

impl ConsensusEvent {
    pub fn priority(a: &Self, b: &Self) -> Ordering {
        #[allow(clippy::match_same_arms)]
        match (a, b) {
            // all forwards first
            (ConsensusEvent::Forward(a), ConsensusEvent::Forward(b)) => a.cmp(b),
            (ConsensusEvent::Forward(_), _) => Ordering::Greater,
            // then all invalid ones - by round
            (ConsensusEvent::Invalid(_), ConsensusEvent::Forward(_)) => Ordering::Less,
            (ConsensusEvent::Invalid(a), ConsensusEvent::Invalid(b)) => {
                a.location().round.cmp(&b.location().round)
            }
            (ConsensusEvent::Invalid(_), ConsensusEvent::Verified(_)) => Ordering::Greater,
            // then all valid ones - by round
            (ConsensusEvent::Verified(a), ConsensusEvent::Verified(b)) => {
                a.body.location.round.cmp(&b.body.location.round)
            }
            (ConsensusEvent::Verified(_), _) => Ordering::Less,
        }
    }
}

impl AltFormat for ConsensusEvent {}
impl fmt::Display for AltFmt<'_, ConsensusEvent> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match AltFormat::unpack(self) {
            ConsensusEvent::Forward(_) => "Forward",
            ConsensusEvent::Verified(_) => "Verified",
            ConsensusEvent::Invalid(_) => "Invalid",
        })
    }
}
