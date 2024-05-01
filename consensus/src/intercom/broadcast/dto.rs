use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::models::{DagPoint, Point, Round, Ugly, UglyPrint};

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

impl Debug for UglyPrint<'_, ConsensusEvent> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ConsensusEvent::Verified(point) => write!(f, "Verified({:?})", point.ugly())?,
            fwd => Debug::fmt(fwd, f)?,
        };
        Ok(())
    }
}
