use std::sync::Arc;

use anyhow::anyhow;
use everscale_types::models::BlockIdShort;

use tycho_network::Response;

use crate::method_to_async_task_closure;
use crate::state_node::StateNodeAdapter;
use crate::utils::async_queued_dispatcher::AsyncQueuedDispatcher;
use crate::validator::network::dto::SignaturesQuery;
use crate::validator::state::ValidationState;
use crate::validator::validator_processor::{ValidatorProcessor, ValidatorTaskResult};

pub async fn handle_signatures_query<W, ST, VS>(
    dispatcher: &Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
    session_seqno: u32,
    block_id_short: BlockIdShort,
    signatures: Vec<([u8; 32], [u8; 64])>,
) -> Result<Option<Response>, anyhow::Error>
where
    W: ValidatorProcessor<ST, VS> + Send + Sync,
    ST: StateNodeAdapter + Send + Sync,
    VS: ValidationState + Send + Sync,
{
    let mut receiver = dispatcher
        .enqueue_task_with_responder(method_to_async_task_closure!(
            get_block_signatures,
            session_seqno,
            &block_id_short
        ))
        .await
        .map_err(|e| anyhow!("Error getting receiver: {:?}", e))?;

    let task_result = receiver
        .await
        .map_err(|e| anyhow!("Receiver error: {:?}", e))?;

    dispatcher
        .enqueue_task(method_to_async_task_closure!(
            process_candidate_signature_response,
            session_seqno,
            block_id_short,
            signatures
        ))
        .await
        .map_err(|e| anyhow!("Error enqueueing task: {:?}", e))?;

    match task_result {
        Ok(ValidatorTaskResult::Signatures(received_signatures)) => {
            let signatures = received_signatures
                .into_iter()
                .map(|(k, v)| (k.0, v.0))
                .collect::<Vec<_>>();

            let response = SignaturesQuery {
                session_seqno,
                block_id_short,
                signatures,
            };
            Ok(Some(Response::from_tl(response)))
        }
        Ok(ValidatorTaskResult::Void) | Ok(ValidatorTaskResult::ValidationStatus(_)) => Err(
            anyhow!("Invalid response type received from get_block_signatures."),
        ),
        Err(e) => Err(anyhow!("Error processing task result: {:?}", e)),
    }
}
