use crate::state_node::{
    StateNodeAdapter, StateNodeAdapterStdImpl, StateNodeEventListener, StateNodeProcessor,
};
use crate::types::{CollationSessionInfo, ValidatedBlock};
use crate::utils::async_queued_dispatcher::{
    AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
};
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::validator_processor::{
    ValidatorProcessor, ValidatorProcessorStdImpl, ValidatorTaskResult,
};
use crate::validator::{Validator, ValidatorEventListener, ValidatorStdImpl};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::BlockId;
use rand::thread_rng;
use std::sync::{Arc, RwLock};
use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_network::Network;

struct StateNodeEventListenerStdImpl {}

#[async_trait]
impl StateNodeEventListener for StateNodeEventListenerStdImpl {
    async fn on_mc_block(&self, mc_block_id: BlockId) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ValidatorEventListener for Arc<StateNodeEventListenerStdImpl> {
    async fn on_block_validated(&self, validated_block: ValidatedBlock) -> anyhow::Result<()> {
        Ok(())
    }
}
//
//
// #[tokio::test]
// async fn test_validator() -> anyhow::Result<()> {
//     let state_node_event_listener = Arc::new(StateNodeEventListenerStdImpl {});
//
//     let state_node_adapter = StateNodeAdapterStdImpl::create(state_node_event_listener.clone());
//     let block_id = BlockId{
//         shard: Default::default(),
//         seqno: 0,
//         root_hash: Default::default(),
//         file_hash: Default::default(),
//     };
//     let block = state_node_adapter.get_block(block_id).await?;
//
//
//     let validator: ValidatorStdImpl<
//         ValidatorProcessorStdImpl<StateNodeAdapterStdImpl, ValidationStateStdImpl>,
//         StateNodeAdapterStdImpl,
//         ValidationStateStdImpl
//     > = ValidatorStdImpl::create(
//         Arc::new(state_node_event_listener),
//         Arc::new(state_node_adapter),
//         ValidationStateStdImpl::new(),
//         Network::builder().with_service_name("network").,
//     );
//
//     let block_id = BlockId{
//         shard: Default::default(),
//         seqno: 0,
//         root_hash: Default::default(),
//         file_hash: Default::default(),
//     };
//
//
//     let validators = ValidatorSubsetInfo{ validators: vec![], short_hash: 0 };
//
//
//     let mut rng = thread_rng();
//     let keypair = KeyPair::generate(&mut rng);
//
//     let collator_session_info = CollationSessionInfo::new(0, validators, Some(keypair));
//     let session_info = Arc::new(collator_session_info);
//     validator.enqueue_candidate_validation(block_id, session_info).await.unwrap();
//     Ok(())
// }
