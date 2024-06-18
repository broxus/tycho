use futures_util::future::BoxFuture;

use crate::dag::InclusionState;
use crate::models::{PointId, Round};

pub enum ConsensusEvent {
    // allows not to peek but poll the channel when local dag is not ready yet
    Forward(Round),
    // well-formed, but not yet validated against DAG
    Validating {
        point_id: PointId,
        task: BoxFuture<'static, InclusionState>,
    },
}
