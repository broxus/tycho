use std::sync::Arc;

use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockIdShort, Signature};
use tokio::sync::broadcast::Sender;
use tycho_network::{PeerId, Response};

use crate::validator::network::dto::SignaturesQueryResponse;
use crate::validator::state::{NotificationStatus, SessionInfo};
use crate::validator::types::StopValidationCommand;
use crate::validator::{process_new_signatures, ValidatorEventListener};

pub async fn handle_signatures_query(
    session: Option<Arc<SessionInfo>>,
    session_seqno: u32,
    block_id_short: BlockIdShort,
    signature: Signature,
    listeners: &[Arc<dyn ValidatorEventListener>],
    validation_finish_broadcaster: Sender<StopValidationCommand>,
    remove_peer_id: PeerId,
) -> Result<Option<Response>, anyhow::Error>
where
{
    let response = match session {
        None => SignaturesQueryResponse {
            session_seqno,
            block_id_short,
            signatures: vec![],
        },
        Some(session) => {
            let process_new_signatures_result = process_new_signatures(
                session.clone(),
                block_id_short,
                vec![(HashBytes(remove_peer_id.0), signature)],
                listeners,
            )
            .await?;

            if process_new_signatures_result.0.is_finished()
                && process_new_signatures_result.1 == NotificationStatus::Notified
            {
                validation_finish_broadcaster
                    .send(StopValidationCommand::ByBlock(block_id_short))?;
            }

            let signatures = session.get_valid_signatures(&block_id_short);

            SignaturesQueryResponse::new(session_seqno, block_id_short, signatures)
        }
    };
    Ok(Some(Response::from_tl(response)))
}
