use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, Signature};
use tokio::sync::broadcast::Sender;
use tycho_network::Response;

use crate::validator::network::dto::SignaturesQuery;
use crate::validator::state::{NotificationStatus, SessionInfo};
use crate::validator::{process_new_signatures, ValidatorEventListener};

pub async fn handle_signatures_query(
    session: Option<Arc<SessionInfo>>,
    session_seqno: u32,
    block_id_short: BlockIdShort,
    signatures: Vec<(HashBytes, Signature)>,
    listeners: &[Arc<dyn ValidatorEventListener>],
    validation_finish_broadcaster: Sender<BlockIdShort>,
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
            let process_new_signatures_result =
                process_new_signatures(session.clone(), block_id_short, signatures, listeners)
                    .await?;

            if process_new_signatures_result.0.is_finished()
                && process_new_signatures_result.1 == NotificationStatus::Notified
            {
                validation_finish_broadcaster.send(block_id_short)?;
            }

            let signatures = session.get_valid_signatures(&block_id_short).await;

            SignaturesQuery::new(session_seqno, block_id_short, signatures)
        }
    };
    Ok(Some(Response::from_tl(response)))
}
