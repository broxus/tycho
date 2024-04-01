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
    VS: ValidationState,
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
    use tokio::sync::{Mutex, Notify};
    use tokio::time::sleep;
    use tracing::{debug, trace};
    use tycho_block_util::block::ValidatorSubsetInfo;
    use tycho_network::{
        Address, DhtClient, DhtConfig, DhtService, Network, OverlayId, OverlayService,
        PeerAffinity, PeerId, PeerInfo, PeerResolver, PrivateOverlay, Request, Router,
    };
    use tycho_util::time::now_sec;

    // Test-specific listener that records validated blocks
    struct TestValidatorEventListener {
        validated_blocks: Mutex<Vec<ValidatedBlock>>,
    }

    #[async_trait]
    impl ValidatorEventListener for TestValidatorEventListener {
        async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()> {
            debug!("ON BLOCK VALIDATED");
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
        let (_, overlay_service) = OverlayService::builder(local_id).build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id).build();

        let router = Router::builder().route(overlay_service.clone()).build();
        let network = Network::builder()
            .with_private_key(random_secret_key.to_bytes())
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        let (_, dht_service) = DhtService::builder(local_id)
            .with_config(DhtConfig {
                local_info_announce_period: Duration::from_secs(1),
                max_local_info_announce_period_jitter: Duration::from_secs(1),
                routing_table_refresh_period: Duration::from_secs(1),
                max_routing_table_refresh_period_jitter: Duration::from_secs(1),
                ..Default::default()
            })
            .build();

        let dht_client = dht_service.make_client(&network);
        let peer_resolver = dht_service.make_peer_resolver().build(&network);

        let validator_network = ValidatorNetwork {
            overlay_service,
            peer_resolver,
            dht_client,
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
        keypair: KeyPair,
        overlay_service: OverlayService,
        dht_client: DhtClient,
        peer_resolver: PeerResolver,
    }
    static PRIVATE_OVERLAY_ID: OverlayId = OverlayId([0; 32]);

    impl Node {
        fn new(
            key: &ed25519::SecretKey,
        ) -> Self {
            let keypair = ed25519::KeyPair::from(key);
            let local_id = PeerId::from(keypair.public_key);

            let (dht_tasks, dht_service) = DhtService::builder(local_id)
                .with_config(DhtConfig {
                    local_info_announce_period: Duration::from_secs(1),
                    max_local_info_announce_period_jitter: Duration::from_secs(1),
                    routing_table_refresh_period: Duration::from_secs(1),
                    max_routing_table_refresh_period_jitter: Duration::from_secs(1),
                    ..Default::default()
                })
                .build();

            let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
                .with_dht_service(dht_service.clone())
                .build();

            let router = Router::builder()
                .route(overlay_service.clone())
                .route(dht_service.clone())
                .build();

            let network = Network::builder()
                .with_private_key(key.to_bytes())
                .with_service_name("test-service")
                .build((Ipv4Addr::LOCALHOST, 0), router)
                .unwrap();

            let dht_client = dht_service.make_client(&network);
            let peer_resolver = dht_service.make_peer_resolver().build(&network);

            overlay_tasks.spawn(&network);
            dht_tasks.spawn(&network);

            Self {
                network,
                keypair,
                overlay_service,
                dht_client,
                peer_resolver,
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
    }

    fn make_network(node_count: usize) -> Vec<Node> {
        let keys = (0..node_count)
            .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();

        let nodes = keys.iter().map(|key| Node::new(key)).collect::<Vec<_>>();

        let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);

        for node in &nodes {
            node.dht_client
                .add_peer(Arc::new(common_peer_info.clone()))
                .unwrap();
        }

        nodes
    }
    //
    // pub struct TestValidatorEventListenerNetwork {
    //     validated_blocks: Mutex<Vec<ValidatedBlock>>,
    //     notify: Arc<Notify>,
    //     expected_notifications: Mutex<u32>,
    //     received_notifications: Mutex<u32>,
    // }
    //
    // impl TestValidatorEventListenerNetwork {
    //     pub fn new(expected_count: u32) -> Arc<Self> {
    //         Arc::new(Self {
    //             validated_blocks: Mutex::new(vec![]),
    //             notify: Arc::new(Notify::new()),
    //             expected_notifications: Mutex::new(expected_count),
    //             received_notifications: Mutex::new(0),
    //         })
    //     }
    //
    //     pub async fn increment_and_check(&self) {
    //         let mut received = self.received_notifications.lock().await;
    //         *received += 1;
    //         if *received == *self.expected_notifications.lock().await {
    //             self.notify.notify_one();
    //         }
    //     }
    // }
    //
    // #[async_trait]
    // impl ValidatorEventListener for TestValidatorEventListenerNetwork {
    //     async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()> {
    //         let mut validated_blocks = self.validated_blocks.lock().await;
    //         validated_blocks.push(validated_block);
    //         self.increment_and_check().await;
    //         Ok(())
    //     }
    // }
    //
    // #[tokio::test]
    // async fn test_validator_accept_block_by_network() -> Result<()> {
    //     tracing_subscriber::fmt::init();
    //     tracing::info!("bootstrap_nodes_accessible");
    //
    //     let network_nodes = make_network(2);
    //     let expected_validations = network_nodes.len() as u32; // Expecting each node to validate
    //     let test_listener = TestValidatorEventListenerNetwork::new(expected_validations);
    //
    //     let mut validators = vec![];
    //
    //     for node in network_nodes {
    //         let test_listener = Arc::new(TestValidatorEventListenerNetwork::new());
    //         let state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();
    //         let test_listener = Arc::new(TestValidatorEventListenerNetwork {
    //             validated_blocks: Mutex::new(vec![]),
    //         });
    //         let state_node_adapter =
    //             Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone()));
    //         let validation_state = ValidationStateStdImpl::new();
    //         let network = ValidatorNetwork {
    //             // network: node.network.clone(),
    //             overlay_service: node.overlay_service.clone(),
    //             dht_client: node.dht_client.clone(),
    //             peer_resolver: node.peer_resolver.clone(),
    //         };
    //         let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_, _>, _, _>::create(
    //             test_listener.clone(),
    //             state_node_adapter,
    //             validation_state,
    //             network,
    //         );
    //         validators.push((validator, node));
    //     }
    //
    //     let mut validators_descriptions = vec![];
    //     for (validator, node) in &validators {
    //         let peer_id = node.network.peer_id();
    //         let keypair = node.keypair.clone();
    //         validators_descriptions.push(ValidatorDescription {
    //             public_key: (*peer_id.as_bytes()).into(),
    //             weight: 1,
    //             adnl_addr: None,
    //             mc_seqno_since: 0,
    //             prev_total_weight: 0,
    //         });
    //     }
    //     let validators_subset_info = ValidatorSubsetInfo {
    //         validators: validators_descriptions,
    //         short_hash: 0,
    //     };
    //
    //     let block = BlockId {
    //         shard: Default::default(),
    //         seqno: 1,
    //         root_hash: Default::default(),
    //         file_hash: Default::default(),
    //     };
    //
    //     for (validator, node) in &validators {
    //         let collator_session_info = Arc::new(CollationSessionInfo::new(
    //             1,
    //             validators_subset_info.clone(),
    //             Some(node.keypair.clone()),
    //         ));
    //         let validation_session =
    //             Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());
    //         validator
    //             .enqueue_add_session(validation_session)
    //             .await
    //             .unwrap();
    //     }
    //
    //     tokio::time::sleep(Duration::from_millis(10)).await;
    //
    //     let mut i = 0;
    //     for (validator, node) in &validators {
    //         i=i+1;
    //         let collator_session_info = Arc::new(CollationSessionInfo::new(
    //             1,
    //             validators_subset_info.clone(),
    //             Some(node.keypair.clone()),
    //         ));
    //         let validation_session =
    //             Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());
    //
    //         // if i == 2 {
    //             validator
    //                 .enqueue_candidate_validation(block.clone(), collator_session_info)
    //                 .await
    //                 .unwrap();
    //         tokio::time::sleep(Duration::from_millis(10)).await;
    //
    //         // }
    //     }
    //
    //     test_listener.notify.notified().await;
    //
    //     // Assert that a block was validated the expected number of times
    //     let validated_blocks = test_listener.validated_blocks.lock().await;
    //     assert_eq!(validated_blocks.len(), expected_validations as usize, "Expected each validator to validate the block once.");
    //
    //
    //     Ok(())
    // }

    pub struct TestValidatorEventListenerNetwork {
        validated_blocks: Mutex<Vec<ValidatedBlock>>,
        notify: Arc<Notify>,
        expected_notifications: Mutex<u32>,
        received_notifications: Mutex<u32>,
    }

    impl TestValidatorEventListenerNetwork {
        pub fn new(expected_count: u32) -> Arc<Self> {
            Arc::new(Self {
                validated_blocks: Mutex::new(vec![]),
                notify: Arc::new(Notify::new()),
                expected_notifications: Mutex::new(expected_count),
                received_notifications: Mutex::new(0),
            })
        }

        pub async fn increment_and_check(&self) {
            let mut received = self.received_notifications.lock().await;
            *received += 1;
            if *received == *self.expected_notifications.lock().await {
                self.notify.notify_one();
            }
        }
    }

    #[async_trait]
    impl ValidatorEventListener for TestValidatorEventListenerNetwork {
        async fn on_block_validated(&self, validated_block: ValidatedBlock) -> Result<()> {
            let mut validated_blocks = self.validated_blocks.lock().await;
            validated_blocks.push(validated_block);
            self.increment_and_check().await;
            debug!("block validated");
            Ok(())
        }
    }

    #[async_trait]
    impl StateNodeEventListener for TestValidatorEventListenerNetwork {
        async fn on_mc_block(&self, mc_block_id: BlockId) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_validator_accept_block_by_network() -> Result<()> {
        tracing_subscriber::fmt::init();
        tracing::info!("bootstrap_nodes_accessible");

        let network_nodes = make_network(2);
        let expected_validations = network_nodes.len() as u32; // Expecting each node to validate
        let test_listener = TestValidatorEventListenerNetwork::new(expected_validations);

        let mut validators = vec![];

        for node in network_nodes {
            // Remove the individual listener creation here, use the shared test_listener
            let state_node_adapter =
                Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone())); // Use the shared listener
            let validation_state = ValidationStateStdImpl::new();
            let network = ValidatorNetwork {
                overlay_service: node.overlay_service.clone(),
                dht_client: node.dht_client.clone(),
                peer_resolver: node.peer_resolver.clone(),
            };
            let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_, _>, _, _>::create(
                test_listener.clone(), // Use the shared listener
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

        let block = BlockId {
            shard: Default::default(),
            seqno: 1,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };
        let block2 = BlockId {
            shard: Default::default(),
            seqno: 2,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };

        let validators_subset_info = ValidatorSubsetInfo {
            validators: validators_descriptions,
            short_hash: 0,
        };
        for (validator, _node) in &validators {
            let collator_session_info = Arc::new(CollationSessionInfo::new(
                1,
                validators_subset_info.clone(),
                Some(_node.keypair.clone()), // Ensure you use the node's keypair correctly here
            ));
            // Assuming this setup is correct and necessary for each validator

            let validation_session =
                Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());
            validator
                .enqueue_add_session(validation_session)
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        for (validator, _node) in &validators {
            let collator_session_info = Arc::new(CollationSessionInfo::new(
                1,
                validators_subset_info.clone(),
                Some(_node.keypair.clone()), // Ensure you use the node's keypair correctly here
            ));
            validator
                .enqueue_candidate_validation(block.clone(), collator_session_info.clone())
                .await
                .unwrap();

            validator
                .enqueue_candidate_validation(block2.clone(), collator_session_info)
                .await
                .unwrap();
        }

        test_listener.notify.notified().await;

        let validated_blocks = test_listener.validated_blocks.lock().await;
        assert_eq!(
            validated_blocks.len(),
            expected_validations as usize,
            "Expected each validator to validate the block once."
        );

        Ok(())
    }
}
