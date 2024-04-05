use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_crypto::ed25519::PublicKey;
use everscale_types::models::BlockId;

use crate::types::{OnValidatedBlockEvent, ValidatorNetwork};
use crate::validator::types::ValidationSessionInfo;
use crate::{
    method_to_async_task_closure,
    state_node::StateNodeAdapter,
    tracing_targets,
    types::ValidatedBlock,
    utils::async_queued_dispatcher::{
        AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE,
    },
};

use super::validator_processor::{ValidatorProcessor, ValidatorTaskResult};

#[async_trait]
pub(crate) trait ValidatorEventEmitter {
    /// When shard or master block was validated by validator
    async fn on_block_validated_event(&self, block_id: BlockId, event: OnValidatedBlockEvent) -> Result<()>;
}

#[async_trait]
pub(crate) trait ValidatorEventListener: Send + Sync {
    /// Process validated shard or master block
    async fn on_block_validated(&self, block_id: BlockId, event: OnValidatedBlockEvent) -> Result<()>;
}

#[async_trait]
pub(crate) trait Validator<ST>: Send + Sync + 'static
where
    ST: StateNodeAdapter,
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
    ) -> Self;

    /// Enqueue block candidate validation task
    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<()>;
    async fn enqueue_stop_candidate_validation(&self, candidate: BlockId) -> Result<()>;

    async fn enqueue_add_session(&self, session_info: Arc<ValidationSessionInfo>) -> Result<()>;
}

#[allow(private_bounds)]
pub(crate) struct ValidatorStdImpl<W, ST>
where
    W: ValidatorProcessor<ST>,
    ST: StateNodeAdapter,
{
    _marker_state_node_adapter: std::marker::PhantomData<ST>,
    dispatcher: Arc<AsyncQueuedDispatcher<W, ValidatorTaskResult>>,
}

#[async_trait]
impl<W, ST> Validator<ST> for ValidatorStdImpl<W, ST>
where
    W: ValidatorProcessor<ST>,
    ST: StateNodeAdapter,
{
    fn create(
        listener: Arc<dyn ValidatorEventListener>,
        state_node_adapter: Arc<ST>,
        network: ValidatorNetwork,
    ) -> Self {
        tracing::info!(target: tracing_targets::VALIDATOR, "Creating validator...");

        // create dispatcher for own async tasks queue
        let (dispatcher, receiver) =
            AsyncQueuedDispatcher::new(STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        let dispatcher = Arc::new(dispatcher);

        // create validation processor and run dispatcher for own tasks queue
        let processor =
            ValidatorProcessor::new(dispatcher.clone(), listener, state_node_adapter, network);
        AsyncQueuedDispatcher::run(processor, receiver);
        tracing::trace!(target: tracing_targets::VALIDATOR, "Tasks queue dispatcher started");

        tracing::info!(target: tracing_targets::VALIDATOR, "Validator created");

        // create validator instance
        Self {
            _marker_state_node_adapter: std::marker::PhantomData,
            dispatcher,
        }
    }

    async fn enqueue_candidate_validation(
        &self,
        candidate: BlockId,
        session_seqno: u32,
        current_validator_keypair: KeyPair,
    ) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                start_candidate_validation,
                candidate,
                session_seqno,
                current_validator_keypair
            ))
            .await
    }

    async fn enqueue_stop_candidate_validation(&self, candidate: BlockId) -> Result<()> {
        self.dispatcher
            .enqueue_task(method_to_async_task_closure!(
                stop_candidate_validation,
                candidate
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
    use std::collections::HashMap;
    use std::net::Ipv4Addr;

    use std::time::Duration;

    use everscale_crypto::ed25519;
    use everscale_crypto::ed25519::KeyPair;
    use everscale_types::models::ValidatorDescription;
    use rand::prelude::ThreadRng;
    use tokio::sync::{Mutex, Notify};

    use tracing::debug;

    use tycho_block_util::block::ValidatorSubsetInfo;
    use tycho_network::{
        DhtClient, DhtConfig, DhtService, Network, OverlayService, PeerId, PeerResolver, Router,
    };

    use crate::state_node::{StateNodeAdapterStdImpl, StateNodeEventListener};
    use crate::test_utils::try_init_test_tracing;
    use crate::types::{BlockSignatures, CollationSessionInfo};

    use crate::validator::state::{ValidationState, ValidationStateStdImpl};
    use crate::validator::types::ValidationSessionInfo;

    use crate::validator::validator_processor::ValidatorProcessorStdImpl;

    use super::*;

    pub struct TestValidatorEventListener {
        validated_blocks: Mutex<Vec<BlockId>>,
        notify: Arc<Notify>,
        expected_notifications: Mutex<u32>,
        received_notifications: Mutex<u32>,
    }

    impl TestValidatorEventListener {
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
    impl ValidatorEventListener for TestValidatorEventListener {
        async fn on_block_validated(&self, block_id: BlockId, event: OnValidatedBlockEvent) -> Result<()> {
            let mut validated_blocks = self.validated_blocks.lock().await;
            validated_blocks.push(block_id);
            self.increment_and_check().await;
            debug!("block validated event");
            Ok(())
        }
    }

    #[async_trait]
    impl StateNodeEventListener for TestValidatorEventListener {
        async fn on_mc_block(&self, _mc_block_id: BlockId) -> Result<()> {
            unimplemented!("Not implemented");
        }
    }

    struct Node {
        network: Network,
        keypair: KeyPair,
        overlay_service: OverlayService,
        dht_client: DhtClient,
        peer_resolver: PeerResolver,
    }

    impl Node {
        fn new(key: &ed25519::SecretKey) -> Self {
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
    }

    fn make_network(node_count: usize) -> Vec<Node> {
        let keys = (0..node_count)
            .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();
        let nodes = keys.iter().map(Node::new).collect::<Vec<_>>();
        let common_peer_info = nodes.first().unwrap().network.sign_peer_info(0, u32::MAX);
        for node in &nodes {
            node.dht_client
                .add_peer(Arc::new(common_peer_info.clone()))
                .unwrap();
        }
        nodes
    }

    #[tokio::test]
    async fn test_validator_accept_block_by_state() -> Result<()> {
        let test_listener = TestValidatorEventListener::new(1);
        let _state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();

        let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone()));
        let _validation_state = ValidationStateStdImpl::new();

        let random_secret_key = ed25519::SecretKey::generate(&mut rand::thread_rng());
        let keypair = ed25519::KeyPair::from(&random_secret_key);
        let local_id = PeerId::from(keypair.public_key);
        let (_, _overlay_service) = OverlayService::builder(local_id).build();

        let (_overlay_tasks, overlay_service) = OverlayService::builder(local_id).build();

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

        let _validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_>, _>::create(
            test_listener.clone(),
            state_node_adapter,
            validator_network,
        );

        let block = BlockId {
            shard: Default::default(),
            seqno: 0,
            root_hash: Default::default(),
            file_hash: Default::default(),
        };

        let validator_description = ValidatorDescription {
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
            validators: vec![validator_description],
            short_hash: 0,
        };
        let keypair = KeyPair::generate(&mut ThreadRng::default());
        let _collator_session_info = CollationSessionInfo::new(0, validators, Some(keypair));
        test_listener
            .on_block_validated(block, OnValidatedBlockEvent::ValidByState)
            .await?;

        let validated_blocks = test_listener.validated_blocks.lock().await;
        assert!(!validated_blocks.is_empty(), "No blocks were validated.");

        Ok(())
    }

    #[tokio::test]
    async fn test_validator_accept_block_by_network() -> Result<()> {
        try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);

        let network_nodes = make_network(3);
        let blocks_amount = 1; // Assuming you expect 3 validation per node.

        let expected_validations = network_nodes.len() as u32; // Expecting each node to validate
        let _test_listener = TestValidatorEventListener::new(expected_validations);

        let mut validators = vec![];
        let mut listeners = vec![]; // Track listeners for later validati

        for node in network_nodes {
            // Create a unique listener for each validator
            let test_listener = TestValidatorEventListener::new(blocks_amount);
            listeners.push(test_listener.clone());

            let state_node_adapter =
                Arc::new(StateNodeAdapterStdImpl::create(test_listener.clone()));
            let _validation_state = ValidationStateStdImpl::new();
            let network = ValidatorNetwork {
                overlay_service: node.overlay_service.clone(),
                dht_client: node.dht_client.clone(),
                peer_resolver: node.peer_resolver.clone(),
            };
            let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_>, _>::create(
                test_listener.clone(),
                state_node_adapter,
                network,
            );
            validators.push((validator, node));
        }

        let mut validators_descriptions = vec![];
        for (_validator, node) in &validators {
            let peer_id = node.network.peer_id();
            let _keypair = node.keypair;
            validators_descriptions.push(ValidatorDescription {
                public_key: (*peer_id.as_bytes()).into(),
                weight: 1,
                adnl_addr: None,
                mc_seqno_since: 0,
                prev_total_weight: 0,
            });
        }

        let blocks = create_blocks(blocks_amount);

        let validators_subset_info = ValidatorSubsetInfo {
            validators: validators_descriptions,
            short_hash: 0,
        };
        for (validator, _node) in &validators {
            let collator_session_info = Arc::new(CollationSessionInfo::new(
                1,
                validators_subset_info.clone(),
                Some(_node.keypair), // Ensure you use the node's keypair correctly here
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
                Some(_node.keypair), // Ensure you use the node's keypair correctly here
            ));

            for block in blocks.iter() {
                validator
                    .enqueue_candidate_validation(
                        *block,
                        collator_session_info.seqno(),
                        *collator_session_info.current_collator_keypair().unwrap(),
                    )
                    .await
                    .unwrap();
            }
        }

        for listener in listeners {
            listener.notify.notified().await;
            let validated_blocks = listener.validated_blocks.lock().await;
            assert_eq!(
                validated_blocks.len(),
                blocks_amount as usize,
                "Expected each validator to validate the block once."
            );
        }
        Ok(())
    }

    fn create_blocks(amount: u32) -> Vec<BlockId> {
        let mut blocks = vec![];
        for i in 0..amount {
            blocks.push(BlockId {
                shard: Default::default(),
                seqno: i,
                root_hash: Default::default(),
                file_hash: Default::default(),
            });
        }
        blocks
    }
}
