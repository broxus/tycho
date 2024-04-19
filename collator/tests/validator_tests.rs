use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytesize::ByteSize;
use std::time::Duration;

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{BlockId, ValidatorDescription};
use rand::prelude::ThreadRng;
use tokio::sync::{Mutex, Notify};

use tracing::debug;

use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_block_util::state::ShardStateStuff;
use tycho_collator::state_node::{
    StateNodeAdapterBuilder, StateNodeAdapterBuilderStdImpl, StateNodeEventListener,
};
use tycho_collator::test_utils::try_init_test_tracing;
use tycho_collator::types::{CollationSessionInfo, OnValidatedBlockEvent, ValidatorNetwork};
use tycho_collator::validator::state::{ValidationState, ValidationStateStdImpl};
use tycho_collator::validator::types::ValidationSessionInfo;
use tycho_collator::validator::validator::{Validator, ValidatorEventListener, ValidatorStdImpl};
use tycho_collator::validator::validator_processor::ValidatorProcessorStdImpl;
use tycho_core::block_strider::prepare_state_apply;
use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayService, PeerId, PeerResolver, Router,
};
use tycho_storage::{build_tmp_storage, Db, DbOptions, Storage};

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
    async fn on_block_validated(
        &self,
        block_id: BlockId,
        _event: OnValidatedBlockEvent,
    ) -> anyhow::Result<()> {
        let mut validated_blocks = self.validated_blocks.lock().await;
        validated_blocks.push(block_id);
        self.increment_and_check().await;
        println!("block validated event");
        Ok(())
    }
}

#[async_trait]
impl StateNodeEventListener for TestValidatorEventListener {
    async fn on_block_accepted(&self, block_id: &BlockId) -> Result<()> {
        unimplemented!("Not implemented");
    }

    async fn on_block_accepted_external(
        &self,
        block_id: &BlockId,
        state: Option<Arc<ShardStateStuff>>,
    ) -> Result<()> {
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
                local_info_announce_period_max_jitter: Duration::from_secs(1),
                routing_table_refresh_period: Duration::from_secs(1),
                routing_table_refresh_period_max_jitter: Duration::from_secs(1),
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
async fn test_validator_accept_block_by_state() -> anyhow::Result<()> {
    let test_listener = TestValidatorEventListener::new(1);
    let _state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();

    let (_, storage) = prepare_state_apply().await?;
    let state_node_adapter =
        Arc::new(StateNodeAdapterBuilderStdImpl::new(storage.clone()).build(test_listener.clone()));
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
            local_info_announce_period_max_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            routing_table_refresh_period_max_jitter: Duration::from_secs(1),
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

    let validator = ValidatorStdImpl::<ValidatorProcessorStdImpl<_>, _>::create(
        test_listener.clone(),
        state_node_adapter,
        validator_network,
    );

    let v_keypair = KeyPair::generate(&mut ThreadRng::default());

    let validator_description = ValidatorDescription {
        public_key: v_keypair.public_key.to_bytes().into(),
        weight: 1,
        adnl_addr: None,
        mc_seqno_since: 0,
        prev_total_weight: 0,
    };

    let validator_description2 = ValidatorDescription {
        public_key: KeyPair::generate(&mut ThreadRng::default())
            .public_key
            .to_bytes()
            .into(),
        weight: 3,
        adnl_addr: None,
        mc_seqno_since: 0,
        prev_total_weight: 0,
    };

    let block_id = BlockId::from_str("-1:8000000000000000:0:58ffca1a178daff705de54216e5433c9bd2e7d850070d334d38997847ab9e845:d270b87b2952b5ba7daa70aaf0a8c361befcf4d8d2db92f9640d5443070838e4")?;

    let block_handle = storage.block_handle_storage().load_handle(&block_id)?;
    assert!(block_handle.is_some(), "Block handle not found in storage.");

    let validators = ValidatorSubsetInfo {
        validators: vec![validator_description, validator_description2],
        short_hash: 0,
    };
    let keypair = KeyPair::generate(&mut ThreadRng::default());
    let collator_session_info = Arc::new(CollationSessionInfo::new(0, validators, Some(keypair)));

    let validation_session =
        Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());

    validator
        .enqueue_add_session(validation_session)
        .await
        .unwrap();

    validator
        .enqueue_candidate_validation(block_id, collator_session_info.seqno(), v_keypair)
        .await
        .unwrap();

    test_listener.notify.notified().await;

    let validated_blocks = test_listener.validated_blocks.lock().await;
    assert!(!validated_blocks.is_empty(), "No blocks were validated.");
    Ok(())
}

#[tokio::test]
async fn test_validator_accept_block_by_network() -> anyhow::Result<()> {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);

    let network_nodes = make_network(3);
    let blocks_amount = 1; // Assuming you expect 3 validation per node.

    let expected_validations = network_nodes.len() as u32; // Expecting each node to validate
    let _test_listener = TestValidatorEventListener::new(expected_validations);

    let mut validators = vec![];
    let mut listeners = vec![]; // Track listeners for later validation

    for node in network_nodes {
        // Create a unique listener for each validator
        let test_listener = TestValidatorEventListener::new(blocks_amount);
        listeners.push(test_listener.clone());

        let state_node_adapter = Arc::new(
            StateNodeAdapterBuilderStdImpl::new(build_tmp_storage()?).build(test_listener.clone()),
        );
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
