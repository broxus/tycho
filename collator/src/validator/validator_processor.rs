use std::collections::HashMap;
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Duration;
use std::{future::Future, sync::Arc};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_crypto::tl::PublicKey;
use everscale_types::boc::BocRepr;
use everscale_types::cell::HashBytes;

use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature, ValidatorDescription};
use futures_util::future::join_all;
use log::info;
use tl_proto::TlResult;
use tokio::time::interval;
use tracing::field::debug;
use tracing::{debug, error, trace, warn};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{Network, OverlayId, PeerId, PrivateOverlay, Request};

use crate::state_node::StateNodeTaskResult;
use crate::types::{BlockCandidate, ValidatorNetwork};
use crate::utils::task_descr::TaskResponseReceiver;
use crate::validator::network::network_service::{NetworkService, SignaturesQuery};
use crate::validator::state::{SessionInfo, ValidationState, ValidationStateStdImpl};
use crate::validator::types::{
    BlockValidationCandidate, OverlayNumber, ValidationResult, ValidationSessionInfo,
    ValidatorInfo, ValidatorInfoError,
};
use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    types::{BlockStuff, CollationSessionInfo, ValidatedBlock},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{ValidatorEventEmitter, ValidatorEventListener};

type ValidatorStateTaskReceiver<T> = TaskResponseReceiver<ValidatorTaskResult, T>;

#[derive(PartialEq, Debug)]
pub enum ValidatorTaskResult {
    Void,
    Signatures(HashMap<HashBytes, Signature>),
    ValidationStatus(ValidationResult),
}

#[allow(private_bounds)]
#[async_trait]
pub trait ValidatorProcessor<ST, VS>:
ValidatorProcessorSpecific<ST, VS> + ValidatorEventEmitter + Sized + Send + Sync + 'static
    where
        ST: StateNodeAdapter,
        VS: ValidationState,
{
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        validation_state: VS,
        network: ValidatorNetwork,
    ) -> Self;

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>;

    fn get_state_node_adapter(&self) -> Arc<ST>;

    fn get_validation_state(&mut self) -> &mut VS;
    fn get_validation_state_ref(&self) -> &VS;

    fn get_network(&self) -> &ValidatorNetwork;

    async fn try_add_session(
        &mut self,
        session: Arc<ValidationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        if self
            .get_validation_state()
            .get_session(session.seqno)
            .is_none()
        {
            // Pre-fetch necessary data from `self.get_network()` outside of the longer-lived borrows
            let (peer_resolver, local_peer_id) = {
                let network = self.get_network();
                // Clone or extract only the necessary parts to reduce the scope of the borrow
                (
                    network.clone().peer_resolver,
                    network.dht_client.network().peer_id().0.clone(),
                )
            };

            // Now `self.get_network()`'s borrow scope ends, allowing `self` to be mutably borrowed again
            let overlay_id = OverlayNumber {
                session_seqno: session.seqno,
            };
            let overlay_id = OverlayId(tl_proto::hash(overlay_id));
            let network_service = NetworkService::new(self.get_dispatcher().clone());

            let private_overlay = PrivateOverlay::builder(overlay_id)
                .with_peer_resolver(peer_resolver)
                .build(network_service);

            let overlay_added = self
                .get_network()
                .overlay_service
                .add_private_overlay(&private_overlay);

            debug!("OVERLAY ADDED {:?} {:?}", overlay_added, overlay_id);
            self.get_validation_state()
                .add_session(session.clone(), private_overlay.clone());
            debug!("SESSION ADDED");
            debug!(
                "NETWORK CLOSED {:?}",
                self.get_network().dht_client.network().is_closed()
            );

            let mut entries = private_overlay.write_entries();

            for validator in session.validators.values() {
                if &validator.public_key.to_bytes() == &local_peer_id {
                    continue;
                }
                entries.insert(&PeerId(validator.public_key.to_bytes()));
            }
        }
        Ok(ValidatorTaskResult::Void)
    }

    /// Start block candidate validation process
    async fn start_candidate_validation(
        &mut self,
        candidate_id: BlockId,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        let current_validator_keypair = match session_info.current_collator_keypair() {
            Some(keypair) => keypair.clone(),
            None => {
                debug!("Collator keypair is not set, skip candidate validation");
                return Ok(ValidatorTaskResult::Void);
            }
        };

        if session_info.current_collator_keypair().is_none() {
            debug!("Collator keypair is not set, skip candidate validation");
            return Ok(ValidatorTaskResult::Void);
        }

        let mut validators = HashMap::new();
        for validator_descr in session_info.collators().validators.iter() {
            let validator_info: Result<ValidatorInfo, ValidatorInfoError> =
                validator_descr.try_into();
            match validator_info {
                Ok(validator_info) => {
                    validators.insert(
                        validator_info.public_key.to_bytes(),
                        Arc::new(validator_info),
                    );
                }
                Err(_) => {}
            }
        }

        if validators.len() == 0 {
            warn!("No validators found, skip candidate validation");
            return Ok(ValidatorTaskResult::Void);
        }

        // let validation_session = Arc::new(ValidationSessionInfo {
        //     seqno: session_info.seqno(),
        //     validators,
        //     current_validator_keypair,
        // });

        let session_seqno = session_info.seqno();
        let session = self.get_validation_state().get_mut_session(session_seqno);


        let session = match session {
            None => {
                error!("failed to start_candidate_validation. session not found");
                panic!("failed to start_candidate_validation. session not found")
            }
            Some(session) => session
        };


        let our_signature = sign_block(&current_validator_keypair, &candidate_id)?;

        session.add_block(session_info.seqno(), candidate_id.clone());

        session.add_signature(
            &candidate_id,
            HashBytes(current_validator_keypair.public_key.to_bytes()),
            our_signature,
            true,
        );

        // let candidate_id_clone = candidate_id.clone();
        // let validation_session_clone = validation_session.clone();

        let dispatcher = self.get_dispatcher().clone();
        let current_validator_pubkey = current_validator_keypair.public_key.clone();
        // let seqno = session;
        tokio::spawn(async move {
            let mut retry_interval = interval(Duration::from_secs(1));
            let max_retries = 100;
            let mut attempts = 0;
            let dispatcher_clone = dispatcher.clone();

            loop {
                // let validation_session_clone = validation_session.clone();
                let dispatcher_clone = dispatcher_clone.clone();
                let candidate_id_clone = candidate_id.clone();
                // let validation_session_clone = validation_session.clone();
                tokio::select! {
                    _ = retry_interval.tick() => {
                        attempts += 1;
                                tokio::spawn(async move {

                            let dispatcher_clone = dispatcher_clone.clone();
                        let validation_status = dispatcher_clone.enqueue_task_with_responder(
                                    method_to_async_task_closure!(
                                        get_validation_status,
                                    session_seqno,
                                    &candidate_id.as_short_id())
                                ).await;



                       let validation_status = match validation_status {

                            Ok(validation_status) => {
                                    match validation_status.await{
                                        Ok(validation_status) => validation_status,
                                        Err(e) => {
                                            error!(err = %e, "Failed to get validation status");
                                            panic!("Failed to get validation status");
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!(err = %e, "Failed to get validation status");
                                    panic!("Failed to get validation status");
                                }
                            };


                        match validation_status {
                            Ok(ValidatorTaskResult::ValidationStatus(validation_status)) => {
                                if validation_status == ValidationResult::Valid || validation_status == ValidationResult::Invalid {
                                    return;
                                }
                                    dispatcher_clone.enqueue_task(method_to_async_task_closure!(
                                    validate_candidate,
                                        candidate_id_clone.clone(),
                                        session_seqno,
                                        current_validator_pubkey.clone()

                                )).await.expect("Failed to validate candidate");

                            }
                            Ok(_) => {}
                            _ => {}
                            }
                        // if *stop_signal.borrow() {
                        //     // If stop signal received, terminate validation
                        //     debug!("Validation for block {:?} stopped by signal.", candidate_id);
                        //     return Ok(ValidatorTaskResult::Void);
                        // }

                        // if let Err(e) = res {
                        //     error!(err = %e, "Failed to run validate block candidate");
                        // }

                            });
                        if attempts >= max_retries {
                            debug!("Max retries reached without successful validation for block {:?}.", candidate_id);
                            return ;
                        }
                    }
                }
            }
        });
        Ok(ValidatorTaskResult::Void)
    }

    async fn get_block_signatures(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult> {
        let session = self.get_validation_state().get_session(session_seqno).context("session not found")?;
        let signatures = session
            .get_valid_signatures(block_id_short);
        Ok(ValidatorTaskResult::Signatures(signatures))
    }

    async fn get_validation_status(
        &mut self,
        session_seqno: u32,
        block_id_short: &BlockIdShort,
    ) -> Result<ValidatorTaskResult> {
        let session = self.get_validation_state().get_session(session_seqno).context("session not found")?;
        let validation_status = session
            .validation_status(block_id_short);
        Ok(ValidatorTaskResult::ValidationStatus(validation_status))
    }

    async fn stop_candidate_validation(
        &self,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        Ok(ValidatorTaskResult::Void)
    }

    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;

    async fn process_candidate_signature_response(
        &mut self,
        session_seqno: u32,
        block_id_short: BlockIdShort,
        signatures: Vec<([u8; 32], [u8; 64])>,
    ) -> Result<ValidatorTaskResult> {

        let session = self.get_validation_state().get_mut_session(session_seqno);

        let session = match session {
            None => {
                info!("failed to process_candidate_signature_response. session not found");
                return Ok(ValidatorTaskResult::Void);
            }
            Some(session) => session
        };

        debug!("PROCESS RESPONSE SIGNATURE");
        // session.add_signature(
        //     &block_id_short,
        //     HashBytes(session.current_validator_keypair.public_key.to_bytes()),
        //     Signature(signatures[0].1),
        //     true,
        // );
        // let candidate = BlockValidationCandidate::from(candidate_id);
        // if signatures.block_validation_candidate != candidate {
        //     panic!("Invalid block validation candidate");
        // }
        // let validation_status = self
        //     .get_validation_state()
        //     .validation_status(session_id, &candidate);
        //
        // if validation_status == ValidationResult::Valid
        //     || validation_status == ValidationResult::Invalid
        // {
        //     return Ok(ValidatorTaskResult::Void);
        // }
        //
        // for signature in signatures.signatures {
        //     let validator_id = HashBytes(signature.0);
        //     let signature = Signature(signature.1);
        //
        //     let validator = session
        //         .get_validation_session_info()
        //         .get(&validator_id)
        //         .context("validator not found")?;
        //
        //     let is_valid = validator
        //         .public_key
        //         .verify(candidate.to_bytes(), &signature.0);
        //
        //     self.get_validation_state().add_signature(
        //         session_id,
        //         candidate_id,
        //         validator_id,
        //         signature,
        //         is_valid,
        //     );
        // }
        // let validation_status = self
        //     .get_validation_state()
        //     .validation_status(session_id, &candidate);
        //
        // match validation_status {
        //     ValidationResult::Valid => {
        //         let signatures = self
        //             .get_validation_state()
        //             .get_valid_signatures(session_id, candidate);
        //         let signatures = signatures.into_iter().map(|(k, v)| (k, v)).collect();
        //         self.on_block_validated_event(ValidatedBlock::new(candidate_id, signatures, true))
        //             .await?;
        //     }
        //     ValidationResult::Invalid => {
        //         self.on_block_validated_event(ValidatedBlock::new(candidate_id, vec![], false))
        //             .await?;
        //     }
        //     ValidationResult::Insufficient => {}
        // }
        Ok(ValidatorTaskResult::Void)
    }

    async fn validate_candidate(
        &self,
        candidate_id: BlockId,
        session_seqno: u32,
        current_validator_pubkey: everscale_crypto::ed25519::PublicKey
        // session_info: Arc<ValidationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {


        // let sessionx = self.get_validation_state_ref().get_session(session_seqno).ok_or(anyhow!("Session not found"))?;

        let block_id_short = candidate_id.as_short_id();

        let validation_state = self.get_validation_state_ref();
        let session = validation_state.get_session(session_seqno).ok_or(anyhow!("Session not found"))?;
        let dispatcher = self.get_dispatcher();

        let receiver = self
            .get_state_node_adapter()
            .request_block(candidate_id)
            .await?;

        let validators =
            session.validators_without_signatures(&block_id_short);

        let private_overlay =session
            .get_overlay()
            .clone();

        let current_signatures =
            session.get_valid_signatures(&candidate_id.as_short_id());

        let network = self.get_network().clone();
        // let session_info_cloned = session_info.clone();

        tokio::spawn(async move {
            if let Ok(Some(_)) = receiver.try_recv().await {
                let result = dispatcher
                    .clone()
                    .enqueue_task(method_to_async_task_closure!(
                        validate_candidate_by_block_from_bc,
                        candidate_id
                    ))
                    .await;

                if let Err(e) = result {
                    error!(err = %e, "Failed to validate block by state");
                    panic!("Failed to validate block by state")
                }
            } else {
                let payload = SignaturesQuery::create(
                    session_seqno,
                    candidate_id.as_short_id(),
                    &current_signatures,
                );

                for validator in validators {
                    if validator.public_key != current_validator_pubkey {
                        debug!("send request");
                        let response = private_overlay
                            .query(
                                &network.dht_client.network(),
                                &PeerId(validator.public_key.to_bytes()),
                                Request::from_tl(payload.clone()),
                            )
                            .await;
                        debug!("request sent");

                        match response {
                            Ok(response) => {
                                let response = response.parse_tl::<SignaturesQuery>();
                                debug!("Got response from network");
                                match response {
                                    Ok(signatures) => {
                                        // let enqueue_task_result = dispatcher
                                        //     .enqueue_task(method_to_async_task_closure!(
                                        //         process_candidate_signature_response,
                                        //         session_info.seqno,
                                        //         signatures,
                                        //         candidate_id
                                        //     ))
                                        //     .await;
                                        //
                                        // if let Err(e) = enqueue_task_result {
                                        //     error!(err = %e, "Failed to enqueue task for processing signatures response");
                                        // }
                                    }
                                    Err(e) => {
                                        error!(err = %e, "Failed convert signatures response to SignaturesQuery");
                                    }
                                }
                            }
                            Err(e) => {
                                error!(err = %e, "Failed to get response from overlay");
                            }
                        }
                    }
                }
                //TODO: need to add a block waiting timeout and proceed to the signature request after it expires
            }
        });
        Ok(ValidatorTaskResult::Void)
    }
}

fn sign_block(key_pair: &KeyPair, block: &BlockId) -> Result<Signature> {
    let block_validation_candidate = BlockValidationCandidate::from(block.clone());
    let signature = Signature(key_pair.sign(block_validation_candidate.to_bytes()));
    Ok(signature)
}

/// Trait declares functions that need specific implementation.
/// For test purposes you can re-implement only this trait.
#[async_trait]
pub(crate) trait ValidatorProcessorSpecific<ST, VS>: Sized {
    // Find a neighbor info by id in local sessions info
    // fn find_neighbor(&self, neighbor: &ValidatorInfo) -> Option<&ValidatorInfo>;

    /// Use signatures of existing block from blockchain to validate candidate
    async fn validate_candidate_by_block_from_bc(
        &mut self,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult>;
}

pub(crate) struct ValidatorProcessorStdImpl<ST, VS>
    where
        ST: StateNodeAdapter,
        VS: ValidationState,
{
    dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
    listener: Arc<dyn ValidatorEventListener>,
    validation_state: VS,
    state_node_adapter: Arc<ST>,
    network: ValidatorNetwork,
}

#[async_trait]
impl<ST, VS> ValidatorEventEmitter for ValidatorProcessorStdImpl<ST, VS>
    where
        ST: StateNodeAdapter,
        VS: ValidationState,
{
    async fn on_block_validated_event(&self, validated_block: ValidatedBlock) -> Result<()> {
        self.listener.on_block_validated(validated_block).await
    }
}

#[async_trait]
impl<ST, VS> ValidatorProcessor<ST, VS> for ValidatorProcessorStdImpl<ST, VS>
    where
        ST: StateNodeAdapter,
        VS: ValidationState,
{
    fn new(
        dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        validation_state: VS,
        network: ValidatorNetwork,
    ) -> Self {
        Self {
            dispatcher,
            listener,
            state_node_adapter,
            validation_state,
            network,
        }
    }

    fn get_state_node_adapter(&self) -> Arc<ST> {
        self.state_node_adapter.clone()
    }

    fn get_dispatcher(&self) -> Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>> {
        self.dispatcher.clone()
    }

    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        todo!()
    }

    fn get_validation_state(&mut self) -> &mut VS {
        &mut self.validation_state
    }

    fn get_network(&self) -> &ValidatorNetwork {
        &self.network
    }

    fn get_validation_state_ref(&self) -> &VS {
        &self.validation_state
    }
}

#[async_trait]
impl<ST, VS> ValidatorProcessorSpecific<ST, VS> for ValidatorProcessorStdImpl<ST, VS>
    where
        ST: StateNodeAdapter,
        VS: ValidationState,
{
    async fn validate_candidate_by_block_from_bc(
        &mut self,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        self.listener
            .on_block_validated(ValidatedBlock::new(candidate_id, vec![], true))
            .await?;
        Ok(ValidatorTaskResult::Void)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_node::{StateNodeAdapterStdImpl, StateNodeEventListener};
    use crate::utils::async_queued_dispatcher::STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE;
    use everscale_crypto::ed25519::KeyPair;
    use rand::prelude::ThreadRng;
    use tokio::sync::Mutex;
    use tokio::test;

    struct TestValidatorEventListener {
        validated_blocks: Mutex<Vec<ValidatedBlock>>,
    }

    #[async_trait]
    impl StateNodeEventListener for TestValidatorEventListener {
        async fn on_mc_block(&self, mc_block_id: BlockId) -> anyhow::Result<()> {
            Ok(())
        }
    }

    impl TestValidatorEventListener {
        fn new() -> Self {
            Self {
                validated_blocks: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl ValidatorEventListener for TestValidatorEventListener {
        async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()> {
            let mut validated_blocks = self.validated_blocks.lock().await;
            validated_blocks.push(validated_block);
            Ok(())
        }
    }

    // #[tokio::test]
    // async fn test_validator_processor() -> Result<()> {
    //     let test_listener = Arc::new(TestValidatorEventListener::new());
    //
    //     let state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();
    //     let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::create(state_node_event_listener));
    //
    //     let (dispatcher, receiver) =
    //         AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
    //     let dispatcher = Arc::new(dispatcher);
    //     let validation_state = ValidationStateStdImpl::new();
    //
    //     // create validation processor and run dispatcher for own tasks queue
    //     let processor: ValidatorProcessorStdImpl<StateNodeAdapterStdImpl, ValidationStateStdImpl> = ValidatorProcessor::new(dispatcher.clone(),  test_listener.clone(), state_node_adapter, validation_state);
    //     AsyncQueuedDispatcher::run(processor, receiver);
    //
    //     // let (dispatcher, receiver) = AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
    //     // let dispatcher = Arc::new(dispatcher);
    //
    //     // let processor = Arc::new(ValidatorProcessorStdImpl::new(dispatcher.clone(), test_listener.clone(), state_node_adapter, validation_state));
    //     // AsyncQueuedDispatcher::run(processor.clone(), receiver);
    //
    //     let block = BlockId {
    //         shard: Default::default(),
    //         seqno: 0,
    //         root_hash: Default::default(),
    //         file_hash: Default::default(),
    //     };
    //
    //     let validator_1 = ValidatorDescription {
    //         public_key: KeyPair::generate(&mut ThreadRng::default()).public_key.to_bytes().into(),
    //         weight: 0,
    //         adnl_addr: None,
    //         mc_seqno_since: 0,
    //         prev_total_weight: 0,
    //     };
    //     let validators = ValidatorSubsetInfo { validators: vec![validator_1], short_hash: 0 };
    //
    //     let keypair = KeyPair::generate(&mut ThreadRng::default());
    //
    //     let collator_session_info = CollationSessionInfo::new(0, validators, Some(keypair));
    //
    //     let _ = processor.start_candidate_validation(block, Arc::new(collator_session_info)).await?;
    //
    //     tokio::time::sleep(Duration::from_millis(500)).await;
    //
    //     // Check if the on_block_validated event was called
    //     let validated_blocks = test_listener.validated_blocks.lock().await;
    //     assert!(!validated_blocks.is_empty(), "No blocks were validated.");
    //
    //     Ok(())
    // }
}
