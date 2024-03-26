use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::BlockId;
use tokio::sync::mpsc::Receiver;
use tycho_network::Network;

use crate::types::ValidatorNetwork;
use crate::validator::state::{ValidationState, ValidationStateStdImpl};
use crate::validator::types::ValidationSessionInfo;
use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    types::CollationSessionInfo,
    types::ValidatedBlock,
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

use super::validator_processor::{ValidatorProcessor, ValidatorTaskResult};

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub trait ValidatorEventEmitter {
    /// When shard or master block was validated by validator
    async fn on_block_validated_event(&self, validated_block: ValidatedBlock) -> Result<()>;
}

#[async_trait]
pub trait ValidatorEventListener: Send + Sync {
    /// Process validated shard or master block
    async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub trait Validator<ST, VS>: Send + Sync + 'static
where
    ST: StateNodeAdapter,
    VS: ValidationState,
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        validation_state: VS,
        network: ValidatorNetwork,
    ) -> Self;
    /// Enqueue block candidate validation task
    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<()>;
    async fn enqueue_add_session(&self, session_info: Arc<ValidationSessionInfo>) -> Result<()>;
}

#[allow(private_bounds)]
pub(crate) struct ValidatorStdImpl<W, ST, VS>
where
    W: ValidatorProcessor<ST, VS>,
    ST: StateNodeAdapter,
    VS: ValidationState, // OA: OverlayAdapter,
{
    _marker_state_node_adapter: std::marker::PhantomData<ST>,
    _marker_validation_state: std::marker::PhantomData<VS>, // Add this line
    dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
    // network: Network,
}

#[async_trait]
impl<W, ST, VS> Validator<ST, VS> for ValidatorStdImpl<W, ST, VS>
where
    W: ValidatorProcessor<ST, VS>,
    ST: StateNodeAdapter,
    VS: ValidationState, // OA: OverlayAdapter
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        validation_state: VS,
        network: ValidatorNetwork,
    ) -> Self {
        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create validation processor and run dispatcher for own tasks queue
        let processor = ValidatorProcessor::new(
            dispatcher.clone(),
            listener,
            state_node_adapter,
            validation_state,
            network,
        );
        AsyncQueuedDispatcher::run(processor, receiver);

        // create validator instance
        Self {
            _marker_state_node_adapter: std::marker::PhantomData,
            _marker_validation_state: std::marker::PhantomData,
            dispatcher,
        }
    }

    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_info: Arc<CollationSessionInfo>,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                start_candidate_validation,
                candidate,
                session_info
            ))
            .await
    }

    async fn enqueue_add_session(&self, session_info: Arc<ValidationSessionInfo>) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(try_add_session, session_info))
            .await
        // Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_node::{StateNodeAdapterStdImpl, StateNodeEventListener};
    use crate::validator::network::network_service::NetworkService;
    use crate::validator::types::{ValidationSessionInfo, ValidatorInfo, ValidatorInfoError};
    use crate::validator::validator;
    use crate::validator::validator_processor::ValidatorProcessorStdImpl;
    use anyhow::{anyhow, bail};
    use everscale_crypto::ed25519;
    use everscale_crypto::ed25519::KeyPair;
    use everscale_types::models::ValidatorDescription;
    use rand::prelude::ThreadRng;
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use std::sync::RwLock;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::sleep;
    use tracing::{debug, trace};
    use tycho_block_util::block::ValidatorSubsetInfo;
    use tycho_network::{
        Address, Network, OverlayId, OverlayService, PeerAffinity, PeerId, PeerInfo,
        PrivateOverlay, Request, Router,
    };
    use tycho_util::time::now_sec;

    // Test-specific listener that records validated blocks
    struct TestValidatorEventListener {
        validated_blocks: Mutex<Vec<ValidatedBlock>>,
    }

    #[async_trait]
    impl ValidatorEventListener for TestValidatorEventListener {
        async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()> {
            debug!("BLOCK VALIDATED");
            let mut validated_blocks = self.validated_blocks.lock().await;
            validated_blocks.push(validated_block);
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
    impl StateNodeEventListener for TestValidatorEventListener {
        async fn on_mc_block(&self, mc_block_id: BlockId) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_validator_accept_block_by_state() -> Result<()> {
        let test_listener = Arc::new(TestValidatorEventListener::new());

        let state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();
        let test_listener = Arc::new(TestValidatorEventListener {
            validated_blocks: Mutex::new(vec![]),
        });

        let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone()));
        let validation_state = ValidationStateStdImpl::new();

        let random_secret_key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let keypair = ed25519::KeyPair::from(&random_secret_key);
        let local_id = PeerId::from(keypair.public_key);
        let (_, overlay_service) = OverlayService::builder(local_id).build( );

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id).build();

        let router = Router::builder().route(overlay_service.clone()).build();
        let network = Network::builder()
            .with_private_key(random_secret_key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        let validator_network = ValidatorNetwork {
            network,
            overlay_service,
        };

        let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_, _>, _, _>::create(
            test_listener.clone(),
            state_node_adapter,
            validation_state,
            validator_network,
        );

        let block = BlockId {
            shard: Default::default(),
            seqno: 0,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };

        let validator_1 = ValidatorDescription {
            public_key: KeyPair::generate(&mut ThreadRng::default())
                .public_key
                .to_bytes()
                .into(),
            weight: 0,
            adnl_addr: None,
            mc_seqno_since: 0,
            prev_total_weight: 0,
        };

        let validators = ValidatorSubsetInfo {
            validators: vec![validator_1],
            short_hash: 0,
        };
        let keypair = KeyPair::generate(&mut ThreadRng::default());
        let collator_session_info = CollationSessionInfo::new(0, validators, Some(keypair));
        test_listener
            .on_block_validated(ValidatedBlock::new(block, vec![], true))
            .await?;

        let validated_blocks = test_listener.validated_blocks.lock().await;
        assert!(!validated_blocks.is_empty(), "No blocks were validated.");
        assert!(validated_blocks[0].is_valid(),);

        Ok(())
    }

    struct Node {
        network: Network,
        // private_overlay: PrivateOverlay,
        keypair: KeyPair,
        overlay_service: OverlayService,
    }
    static PRIVATE_OVERLAY_ID: OverlayId = OverlayId([0; 32]);

    impl Node {
        fn new(
            key: &ed25519::SecretKey,
            // <W: ValidatorProcessor<ST, VS>, ST: StateNodeAdapter, VS: ValidationState>(key: &ed25519::SecretKey,  network_service: NetworkService<W, ST, VS>
        ) -> Self {
            let keypair = ed25519::KeyPair::from(key);
            let local_id = PeerId::from(keypair.public_key);

            // let private_overlay = PrivateOverlay::builder(PRIVATE_OVERLAY_ID).build(network_service);

            let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
                // .with_private_overlay(&private_overlay)
                .build();

            // let private_overlay = PrivateOverlay::builder(PRIVATE_OVERLAY_ID).build(overlay_service);
            // private_overlay.write_entries().insert(&somepeer);
            // overlay_service.try_add_private_overlay(private_overlay)

            let router = Router::builder().route(overlay_service.clone()).build();

            let network = Network::builder()
                .with_private_key(key.to_bytes())
                .with_service_name("test-service")
                .build((Ipv4Addr::LOCALHOST, 0), router)
                .unwrap();

            overlay_tasks.spawn(network.clone());

            Self {
                network,
                keypair,
                overlay_service,
            }
        }

        fn make_peer_info(key: &ed25519::SecretKey, address: Address) -> PeerInfo {
            let keypair = ed25519::KeyPair::from(key);
            let peer_id = PeerId::from(keypair.public_key);

            let now = now_sec();
            let mut node_info = PeerInfo {
                id: peer_id,
                address_list: vec![address].into_boxed_slice(),
                created_at: now,
                expires_at: u32::MAX,
                signature: Box::new([0; 64]),
            };
            *node_info.signature = keypair.sign(&node_info);
            node_info
        }

        // async fn private_overlay_query<Q, A>(&self, peer_id: &PeerId, req: Q) -> Result<A>
        //     where
        //         Q: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
        //         for<'a> A: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
        // {
        //     self.private_overlay
        //         .query(&self.network, peer_id, Request::from_tl(req))
        //         .await?
        //         .parse_tl::<A>()
        //         .map_err(Into::into)
        // }
    }

    fn make_network(node_count: usize) -> Vec<Node> {
        let keys = (0..node_count)
            .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();

        let nodes = keys.iter().map(|key| Node::new(key)).collect::<Vec<_>>();

        let bootstrap_info = std::iter::zip(&keys, &nodes)
            .map(|(key, node)| {
                Arc::new(Node::make_peer_info(key, node.network.local_addr().into()))
            })
            .collect::<Vec<_>>();

        for node in &nodes {
            // let mut private_overlay_entries = node.private_overlay.write_entries();

            for info in &bootstrap_info {
                node.network
                    .known_peers()
                    .insert(info.clone(), PeerAffinity::Allowed);

                // private_overlay_entries.insert(&info.id);
            }
        }

        nodes
    }

    #[tokio::test]
    async fn test_validator_accept_block_by_network() -> Result<()> {
        tracing_subscriber::fmt::init();
        tracing::info!("bootstrap_nodes_accessible");

        // let test_listener = Arc::new(TestValidatorEventListener::new());
        // let state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();
        // let test_listener = Arc::new(TestValidatorEventListener { validated_blocks: Mutex::new(vec![]) });
        // let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone()));
        // let validation_state = ValidationStateStdImpl::new();
        // let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_, _>, _, _>::create(
        //     test_listener.clone(),
        //     state_node_adapter,
        //     validation_state,
        // );
        //
        // let dispatcher = validator.dispatcher.clone();

        // let network_service = NetworkService::new(dispatcher);

        // first node in network is our node
        let network_nodes = make_network(2);

        let mut validators = vec![];

        for node in network_nodes {
            let test_listener = Arc::new(TestValidatorEventListener::new());
            let state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();
            let test_listener = Arc::new(TestValidatorEventListener {
                validated_blocks: Mutex::new(vec![]),
            });
            let state_node_adapter =
                Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone()));
            let validation_state = ValidationStateStdImpl::new();
            let network = ValidatorNetwork {
                network: node.network.clone(),
                overlay_service: node.overlay_service.clone(),
            };
            let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_, _>, _, _>::create(
                test_listener.clone(),
                state_node_adapter,
                validation_state,
                network,
            );
            validators.push((validator, node));
        }

        let mut validators_descriptions = vec![];
        for (validator, node) in &validators {
            let peer_id = node.network.peer_id();
            let keypair = node.keypair.clone();
            validators_descriptions.push(ValidatorDescription {
                public_key: (*peer_id.as_bytes()).into(),
                weight: 1,
                adnl_addr: None,
                mc_seqno_since: 0,
                prev_total_weight: 0,
            });
        }
        let validators_subset_info = ValidatorSubsetInfo {
            validators: validators_descriptions,
            short_hash: 0,
        };

        let block = BlockId {
            shard: Default::default(),
            seqno: 1,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };

        for (validator, node) in &validators {
            let collator_session_info = Arc::new(CollationSessionInfo::new(
                1,
                validators_subset_info.clone(),
                Some(node.keypair.clone()),
            ));
            let validation_session =
                Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());
            validator
                .enqueue_add_session(validation_session)
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut i = 0;
        for (validator, node) in &validators {
            i=i+1;
            let collator_session_info = Arc::new(CollationSessionInfo::new(
                1,
                validators_subset_info.clone(),
                Some(node.keypair.clone()),
            ));
            let validation_session =
                Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());

            // if i == 1 {
                validator
                    .enqueue_candidate_validation(block.clone(), collator_session_info)
                    .await
                    .unwrap();
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // }
        }

        tokio::time::sleep(Duration::from_millis(2000)).await;

        // let network = network_nodes[0].network.clone();
        // let our_node_peer = network.peer_id().clone();
        // let out_node_keypair = network_nodes[0].keypair.clone();

        //
        //
        //
        // assert_eq!(out_node_keypair.public_key.as_bytes(), our_node_peer.as_bytes());
        //
        // let mut validators = vec![];
        // for node in &network_nodes {
        //     let peer_id = node.network.peer_id();
        //     let keypair = node.keypair.clone();
        //     validators.push(ValidatorDescription {
        //         public_key: (*peer_id.as_bytes()).into(),
        //         weight: 1,
        //         adnl_addr: None,
        //         mc_seqno_since: 0,
        //         prev_total_weight: 0,
        //     });
        // }
        //
        // let validator_subset_info = ValidatorSubsetInfo {
        //     validators,
        //     short_hash: 0,
        // };
        // let collator_session_info = Arc::new(CollationSessionInfo::new(1, validator_subset_info, Some(out_node_keypair)));
        //
        // // let validation_session = collator_session_info.try_into().unwrap();
        //
        //
        //
        // let dispatcher = validator.dispatcher;
        // let network_service = NetworkService::new(dispatcher);

        //
        // validator.enqueue_add_session(validation_session);
        //
        // let block = BlockId {
        //     shard: Default::default(),
        //     seqno: 0,
        //     root_hash: Default::default(),
        //     file_hash: Default::default(),
        // };
        //
        // let validator_1 = ValidatorDescription {
        //     public_key: KeyPair::generate(&mut ThreadRng::default()).public_key.to_bytes().into(),
        //     weight: 0,
        //     adnl_addr: None,
        //     mc_seqno_since: 0,
        //     prev_total_weight: 0,
        // };
        //
        // let validators = ValidatorSubsetInfo { validators: vec![validator_1], short_hash: 0 };
        // let keypair = KeyPair::generate(&mut ThreadRng::default());
        // let collator_session_info = CollationSessionInfo::new(0, validators, Some(keypair));
        // test_listener.on_block_validated(ValidatedBlock::new(block, vec![], true)).await?;
        //
        // let validated_blocks = test_listener.validated_blocks.lock().await;
        // assert!(!validated_blocks.is_empty(), "No blocks were validated.");
        // assert!(validated_blocks[0].is_valid(),);

        Ok(())
    }
}
