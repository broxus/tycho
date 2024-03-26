use std::collections::HashMap;
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Duration;
use std::{future::Future, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::boc::BocRepr;
use everscale_types::cell::HashBytes;

use everscale_types::models::{BlockId, ShardIdent, Signature, ValidatorDescription};
use futures_util::future::join_all;
use tracing::{debug, trace};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{Network, OverlayId, PeerId, PrivateOverlay, Request};

use crate::types::{BlockCandidate, ValidatorNetwork};
use crate::validator::network::network_service::{NetworkService, SignaturesQuery};
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::types::{
    BlockValidationCandidate, OverlayNumber, ValidationSessionInfo, ValidatorInfo,
    ValidatorInfoError,
};
use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    types::{BlockStuff, CollationSessionInfo, ValidatedBlock},
    utils::async_queued_dispatcher::AsyncQueuedDispatcher,
};

use super::{ValidatorEventEmitter, ValidatorEventListener};

// ADAPTER PROCESSOR

#[derive(PartialEq, Debug)]
pub enum ValidatorTaskResult {
    Void,
    Signatures(Option<HashMap<HashBytes, Signature>>),
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
            // self.get_validation_state().get_session()
            let overlay_id = OverlayNumber {
                session_seqno: session.seqno,
            };
            let overlay_id = OverlayId(tl_proto::hash(overlay_id));
            let network_service = NetworkService::new(self.get_dispatcher().clone());
            let private_overlay = PrivateOverlay::builder(overlay_id).build(network_service);
            // let overlay_writer = private_overlay.write_entries();
            for validator in session.validators.values() {
                private_overlay
                    .write_entries()
                    .insert(&PeerId(validator.public_key.to_bytes()));
            }
            let overlay_added = self
                .get_network()
                .overlay_service
                .try_add_private_overlay(&private_overlay);
            debug!("OVERLAY ADDED {:?} {:?}", overlay_added,  overlay_id);
            self.get_validation_state()
                .add_session(session.clone(), private_overlay);
            debug!("SESSION ADDED");
            debug!("NETWORK CLOSED {:?}", self.get_network().network.is_closed());
            debug!("KNOWS PEERS {:?}", self.get_network().network.known_peers().0.len());
            // self.get_network().network.known_peers().0.len()

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
            debug!("No validators found, skip candidate validation");
            return Ok(ValidatorTaskResult::Void);
        }

        let validation_session = Arc::new(ValidationSessionInfo {
            seqno: session_info.seqno(),
            validators,
            current_validator_keypair,
        });

        let our_signature_for_block = sign_block(&current_validator_keypair, &candidate_id)?;

        self.get_validation_state().add_signature(
            session_info.seqno(),
            candidate_id,
            HashBytes(current_validator_keypair.public_key.to_bytes()),
            our_signature_for_block,
        );

        self.validate_candidate(candidate_id, validation_session)
            .await
    }

    async fn get_block_signatures(
        &mut self,
        session_seqno: u32,
        block_validation_candidate: BlockValidationCandidate,
    ) -> Result<ValidatorTaskResult> {
        let signatures = self
            .get_validation_state()
            .get_signatures(session_seqno, block_validation_candidate)
            .cloned();
        Ok(ValidatorTaskResult::Signatures(signatures))
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

    /// Send signature request to each neighbor passing callback closure
    /// that queue signatures responses processing
    // async fn request_candidate_signatures(
    //     // dispatcher: Arc<AsyncQueuedDispatcher<Self, ValidatorTaskResult>>,
    //     candidate_id: BlockId,
    //     // own_signature: Signature,
    //     session_info: Arc<ValidationSessionInfo>,
    // ) -> Result<ValidatorTaskResult> {
    //     let validators = self.get_validation_state().validators_without_signatures(session_info.seqno, candidate_id);
    //
    //     /// create tasks for each peer directly (without enqueue)
    //     for validator in validators {
    //
    //             // Self::request_candidate_signature_from_neighbor(
    //             //     &validator,
    //             //     candidate_id
    //             // );
    //     }
    //
    //
    //
    //     Ok(ValidatorTaskResult::Void)
    // }

    async fn process_candidate_signature_response(
        &mut self,
        session: Arc<ValidationSessionInfo>,
        signatures: SignaturesQuery,
        candidate_id: BlockId,
    ) -> Result<ValidatorTaskResult> {
        debug!("PROCESS RESPONSE SIGNATURE");
        // skip signature if candidate already validated (does not matter if it valid or not)
        let candidate = BlockValidationCandidate::from(candidate_id);
        if self.get_validation_state().is_validated(session.seqno, &candidate) {
            return Ok(ValidatorTaskResult::Void);
        }

        if signatures.block_validation_candidate != candidate {
            panic!("Invalid block validation candidate");
        }

        debug!("SIGNATURES LENGTH = {:?}", signatures.signatures.len());
        debug!("MY SESSION PUBKEY = {:?}", session.current_validator_keypair.public_key);
        for signature in signatures.signatures {
            let validator_id = HashBytes(signature.0);
            let signature = Signature(signature.1);
            let validator = session.validators.get(&validator_id).expect("validator not found");
            debug!("SIGNATURE VALIDATOR PUBKEY = {:?}", validator.public_key);

            let is_valid = validator.public_key.verify(candidate.to_bytes(), &signature.0);
            debug!("SIGNATURE IS VALID {:?}", is_valid);
            self.get_validation_state().add_signature(
                session.seqno,
                candidate_id,
                validator_id,
                signature,
            );
        }
        if self.get_validation_state().is_validated(session.seqno, &candidate) {
            debug!("BLOCK VALIDATED SUCCESSFULLY");
            return Ok(ValidatorTaskResult::Void);
        }



        // get neighbor from local list
        // let neighbor = match self.find_neighbor(&validator_info) {
        //     Some(n) => n,
        //     None => {
        //         // skip signature if collator is unknown
        //         return Ok(ValidatorTaskResult::Void);
        //     }
        // };

        // check signature and update candidate score
        // let signature_is_valid = Self::check_signature(&candidate_id, &his_signature, neighbor)?;
        // // self.update_candidate_score(
        // //     candidate_id,
        // //     signature_is_valid,
        // //     his_signature,
        // //     neighbor.clone(),
        // // )
        // // .await?;

        Ok(ValidatorTaskResult::Void)
    }

    async fn update_candidate_score(
        &mut self,
        candidate_id: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: &ValidatorInfo,
    ) -> Result<()> {
        if let Some(validated_block) = self.append_candidate_signature_and_return_if_validated(
            candidate_id,
            signature_is_valid,
            his_signature,
            neighbor,
        ) {
            self.on_block_validated_event(validated_block).await?;
        }

        Ok(())
    }

    async fn validate_candidate(
        &self,
        candidate_id: BlockId,
        session_info: Arc<ValidationSessionInfo>,
    ) -> Result<ValidatorTaskResult> {
        let receiver = self
            .get_state_node_adapter()
            .request_block(candidate_id)
            .await?;
        let validation_state = self.get_validation_state_ref();

        let current_validator_pubkey = session_info.current_validator_keypair.public_key;
        let validators =
            validation_state.validators_without_signatures(session_info.seqno, candidate_id);
        let validation_session_info = validation_state
            .get_session(session_info.seqno)
            .unwrap()
            .get_validation_session_info();
        debug!("VALIDATORS WITHOUT SIGNATURES = {:?}", validators.len());
        debug!(
            "VALIDATORS TOTAL = {:?}",
            validation_session_info.validators.len()
        );
        let private_overlay = validation_state
            .get_session(session_info.seqno)
            .unwrap()
            .get_overlay()
            .clone();
        let current_signatures = validation_state
            .get_signatures(session_info.seqno, candidate_id.into())
            .cloned();

        let dispatcher = self.get_dispatcher();
        // let validators = self.get_validation_state_ref().validators_without_signatures(session_info.seqno, candidate_id);
        let network = self.get_network().clone();
        let validation_session_info_cloned = validation_session_info.clone();
        let session_info_cloned = session_info.clone();

        // tokio::spawn(async move {
            if let Ok(Some(block_from_bc)) = receiver.try_recv().await {
                debug!("Block some");

                // if state node contains required block then schedule validation using it
                dispatcher
                    .clone()
                    .enqueue_task(method_to_async_task_closure!(
                        validate_candidate_by_block_from_bc,
                        candidate_id
                    ))
                    .await;
            } else {
                debug!("start validation by network");


                let req = SignaturesQuery::create(session_info_cloned.clone().seqno, candidate_id.into(), current_signatures.as_ref());

                for validator in validators {
                    if validator.public_key != current_validator_pubkey.clone() {
                        debug!("SEND REQUEST TO {:?} FROM {:?}", validator.public_key, current_validator_pubkey.clone() );
                        let peer_available = network.network.known_peers().contains(&PeerId(validator.public_key.to_bytes()));
                        if !peer_available {
                            debug!("ERROR: PEER NOT AVAILABLE");
                            continue;
                        }

                        let peer_present = private_overlay.read_entries().contains(&PeerId(validator.public_key.to_bytes()));
                        if !peer_present {
                            debug!("ERROR: PEER NOT PRESENT IN OVERLAY");
                            continue;
                        }

                        debug!("network is closed {:?}", network.network.is_closed());
                        let result = private_overlay
                            .query(&network.network, &PeerId(validator.public_key.to_bytes()), Request::from_tl(req.clone()))
                            .await;
                            // .unwrap()
                            // .parse_tl::<SignaturesQuery>().unwrap();

                        debug!("network is closed2 {:?}", network.network.is_closed());


                        // print respose error
                        if let Err(e) = result {
                            println!("ERROR: {:?}", e);
                            continue;
                        }
                        // println!("GET RESPONSE FROM {:?} {:?}", validator.public_key, result);

                    //     let s = session_info_cloned.clone();
                    //     dispatcher.enqueue_task(method_to_async_task_closure!(
                    //     process_candidate_signature_response,
                    //         s,
                    //         result,
                    //         candidate_id
                    // )).await.unwrap();
                    //
                    }
                }

                //TODO: need to add a block waiting timeout and proceed to the signature request after it expires
            }
        // });

        // tokio::time::sleep(Duration::from_millis(1000)).await;
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

    /// Request signature from neighbor collator and run callback when receive response.
    /// Send own signature so neighbor can use it to validate his own candidate
    async fn request_candidate_signature_from_neighbor<Fut>(
        validator_info: &ValidatorInfo,
        shard_id: ShardIdent,
        seq_no: u32,
        own_signature: Signature,
        callback: impl FnOnce(ValidatorInfo, Signature) -> Fut + Send + 'static,
    ) -> Result<()>
    where
        Fut: Future<Output = Result<()>> + Send;

    fn check_signature(
        candidate_id: &BlockId,
        his_signature: &Signature,
        neighbor: &ValidatorInfo,
    ) -> Result<bool>;

    fn is_candidate_validated(&self, block_id: &BlockId) -> bool;

    fn append_candidate_signature_and_return_if_validated(
        &mut self,
        candidate_id: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: &ValidatorInfo,
    ) -> Option<ValidatedBlock>;
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

    async fn request_candidate_signature_from_neighbor<Fut>(
        collator_descr: &ValidatorInfo,
        shard_id: ShardIdent,
        seq_no: u32,
        own_signature: Signature,
        callback: impl FnOnce(ValidatorInfo, Signature) -> Fut + Send + 'static,
    ) -> Result<()>
    where
        Fut: Future<Output = Result<()>> + Send,
    {
        todo!()
    }
    //
    // fn find_neighbor(&self, neighbor: &ValidatorInfo) -> Option<&ValidatorInfo> {
    //     todo!()
    // }

    fn check_signature(
        candidate_id: &BlockId,
        his_signature: &Signature,
        neighbor: &ValidatorInfo,
    ) -> Result<bool> {
        todo!()
    }

    fn is_candidate_validated(&self, block_id: &BlockId) -> bool {
        todo!()
    }

    fn append_candidate_signature_and_return_if_validated(
        &mut self,
        candidateid: BlockId,
        signature_is_valid: bool,
        his_signature: Signature,
        neighbor: &ValidatorInfo,
    ) -> Option<ValidatedBlock> {
        todo!()
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
    use tycho_block_util::block::ValidatorSubsetInfo;

    // Test-specific listener that records validated blocks
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
