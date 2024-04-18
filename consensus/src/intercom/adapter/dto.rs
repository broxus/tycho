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

/// * signer signals (Ok) when ready, broadcaster signals () when ready
/// * signer finishes only after broadcaster signalled ()
/// * broadcaster finishes Ok only if signer signalled (Ok), signalling () to signer
/// * broadcaster must finish Ok/Err if signer signalled (Err), signalling () to signer
/// * broadcaster may finish Err, signalling () to signer
///
/// => signer may run without broadcaster, as if broadcaster signalled ()
#[derive(Debug)]
pub enum SignerSignal {
    Ok,
    Err,
    Retry,
}
