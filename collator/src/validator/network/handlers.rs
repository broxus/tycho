use crate::validator::network::dto::SignaturesQuery;
use crate::validator::state::SessionInfo;
use crate::validator::{process_candidate_signature_response, ValidatorEventListener};
use everscale_types::models::BlockIdShort;
use std::sync::Arc;
use tracing::trace;
use tycho_network::Response;
use crate::tracing_targets;

pub async fn handle_signatures_query(
    session: Option<Arc<SessionInfo>>,
    session_seqno: u32,
    block_id_short: BlockIdShort,
    signatures: Vec<([u8; 32], [u8; 64])>,
    listeners: &[Arc<dyn ValidatorEventListener>],
) -> Result<Option<Response>, anyhow::Error>
where
{
    let response = match session {
        None => SignaturesQuery {
            session_seqno,
            block_id_short,
            signatures: vec![],
        },
        Some(session) => {
            trace!(target: tracing_targets::VALIDATOR, "Processing signatures query for block {:?} with {} signatures", block_id_short, signatures.len());
            process_candidate_signature_response(
                session.clone(),
                block_id_short,
                signatures,
                listeners,
            )
            .await?;

            trace!(target: tracing_targets::VALIDATOR, "Getting valid signatures for block {:?}", block_id_short);
            let signatures = session
                .get_valid_signatures(&block_id_short)
                .await
                .into_iter()
                .map(|(k, v)| (k.0, v.0))
                .collect::<Vec<_>>();

            SignaturesQuery {
                session_seqno,
                block_id_short,
                signatures,
            }
        }
    };
    Ok(Some(Response::from_tl(response)))
}
