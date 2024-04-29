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

impl Debug for UglyPrint<'_, ConsensusEvent> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ConsensusEvent::Verified(point) => write!(f, "Verified({:?})", point.ugly())?,
            fwd => Debug::fmt(fwd, f)?,
        };
        Ok(())
    }
}
