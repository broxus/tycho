use std::sync::Arc;

use crate::models::{DagPoint, Point, Round};

#[derive(Debug)]
pub enum ConsensusEvent {
    // allows not to peek but poll the channel when local dag is not ready yet
    Forward(Round),
    // well-formed, but not yet validated against DAG
    Verified(Arc<Point>),
    Invalid(DagPoint),
}

/// collector may run without broadcaster, as if broadcaster signalled Ok
#[derive(Debug)]
pub enum CollectorSignal {
    Finish,
    Err,
    Retry,
}
