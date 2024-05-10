use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, Signature};
use tycho_network::Response;

use crate::validator::network::dto::SignaturesQuery;
use crate::validator::state::SessionInfo;
use crate::validator::{process_new_signatures, ValidatorEventListener};

pub async fn handle_signatures_query(
    session: Option<Arc<SessionInfo>>,
    session_seqno: u32,
    block_id_short: BlockIdShort,
    signatures: Vec<(HashBytes, Signature)>,
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
            process_new_signatures(session.clone(), block_id_short, signatures, listeners).await?;

            let signatures = session.get_valid_signatures(&block_id_short);

            SignaturesQuery::new(session_seqno, block_id_short, signatures)
        }
    };
    Ok(Some(Response::from_tl(response)))
}
