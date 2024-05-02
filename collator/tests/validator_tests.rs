use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{BlockId, ValidatorDescription};
use rand::prelude::ThreadRng;
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;
use tracing::debug;

use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_collator::state_node::{StateNodeAdapterStdImpl, StateNodeEventListener};
use tycho_collator::test_utils::{prepare_test_storage, try_init_test_tracing};
use tycho_collator::types::{CollationSessionInfo, OnValidatedBlockEvent, ValidatorNetwork};
use tycho_collator::validator::config::ValidatorConfig;
use tycho_collator::validator::state::{ValidationState, ValidationStateStdImpl};
use tycho_collator::validator::types::ValidationSessionInfo;
use tycho_collator::validator::validator::{Validator, ValidatorEventListener, ValidatorStdImpl};
use tycho_core::block_strider::{BlockStrider, PersistentBlockStriderState, PrintSubscriber};
use tycho_network::{
    DhtClient, DhtConfig, DhtService, Network, OverlayService, PeerId, PeerResolver, Router,
};
use tycho_storage::Storage;

pub struct TestValidatorEventListener {
    validated_blocks: Mutex<Vec<BlockId>>,
    notify: Arc<Notify>,
    expected_notifications: Mutex<u32>,
    received_notifications: Mutex<u32>,
    global_validated_blocks: Arc<AtomicUsize>,
}

impl TestValidatorEventListener {
    pub fn new(expected_count: u32, global_validated_blocks: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            validated_blocks: Mutex::new(vec![]),
            notify: Arc::new(Notify::new()),
            expected_notifications: Mutex::new(expected_count),
            received_notifications: Mutex::new(0),
            global_validated_blocks,
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
    ) -> Result<()> {
        let mut validated_blocks = self.validated_blocks.lock().await;
        if validated_blocks.contains(&block_id) {
            return Ok(());
        } else {
            validated_blocks.push(block_id);
        }

        self.global_validated_blocks.fetch_add(1, Ordering::SeqCst);

        self.increment_and_check().await;
        Ok(())
    }
}

#[async_trait]
impl StateNodeEventListener for TestValidatorEventListener {
    async fn on_block_accepted(&self, _block_id: &BlockId) -> Result<()> {
        unimplemented!("Not implemented");
    }

    async fn on_block_accepted_external(&self, _state: &ShardStateStuff) -> Result<()> {
        unimplemented!("Not implemented");
    }
}

struct Node {
    network: Network,
    keypair: Arc<KeyPair>,
    overlay_service: OverlayService,
    dht_client: DhtClient,
    peer_resolver: PeerResolver,
}

impl Node {
    fn new(key: &ed25519::SecretKey) -> Self {
        let keypair = Arc::new(ed25519::KeyPair::from(key));
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
    let global_validated_blocks = Arc::new(AtomicUsize::new(0));

    let test_listener = TestValidatorEventListener::new(1, global_validated_blocks);
    let _state_node_event_listener: Arc<dyn StateNodeEventListener> = test_listener.clone();

    let (provider, storage) = prepare_test_storage().await.unwrap();

    let zerostate_id = BlockId::default();

    let block_strider = BlockStrider::builder()
        .with_provider(provider)
        .with_state(PersistentBlockStriderState::new(
            zerostate_id,
            storage.clone(),
        ))
        .with_state_subscriber(
            MinRefMcStateTracker::default(),
            storage.clone(),
            PrintSubscriber,
        )
        .build();

    block_strider.run().await.unwrap();

    let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::new(
        test_listener.clone(),
        storage.clone(),
    ));
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

    let validator = ValidatorStdImpl::new(
        vec![test_listener.clone()],
        state_node_adapter,
        validator_network,
        Arc::new(KeyPair::generate(&mut ThreadRng::default())),
        ValidatorConfig {
            base_loop_delay: Duration::from_millis(50),
            max_loop_delay: Duration::from_secs(10),
        },
    );

    let validator_description = ValidatorDescription {
        public_key: validator.get_keypair().public_key.to_bytes().into(),
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

    let block_id = storage.node_state().load_last_mc_block_id().unwrap();

    let block_handle = storage.block_handle_storage().load_handle(&block_id);
    assert!(block_handle.is_some(), "Block handle not found in storage.");

    let validators = ValidatorSubsetInfo {
        validators: vec![validator_description, validator_description2],
        short_hash: 0,
    };
    let keypair = Arc::new(KeyPair::generate(&mut ThreadRng::default()));
    let collator_session_info = Arc::new(CollationSessionInfo::new(0, validators, Some(keypair)));

    let validation_session =
        Arc::new(ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap());

    validator.add_session(validation_session).await.unwrap();

    validator
        .validate(block_id, collator_session_info.seqno())
        .await
        .unwrap();

    test_listener.notify.notified().await;
    let validated_blocks = test_listener.validated_blocks.lock().await;
    assert_eq!(
        validated_blocks.len() as u32,
        1,
        "Expected each validator to validate the block once."
    );
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_validator_accept_block_by_network() -> Result<()> {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_validator_accept_block_by_network");

    let mut tmp_dirs = Vec::new();

    let node_count = 13u32;
    let network_nodes = make_network(node_count as usize);
    let blocks_amount = 100u32;
    let sessions = 1u32;
    let max_concurrent_blocks = 1; // Limit to processing ten blocks at a time
    let required_validations = blocks_amount * node_count; // Total required validations for all validators together
    let global_validated_blocks = Arc::new(AtomicUsize::new(0));

    let mut tasks = vec![];

    let mut validators_descriptions = Vec::new();
    for node in &network_nodes {
        let peer_id = node.network.peer_id();
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

    for node in network_nodes {
        let (storage, tmp_dir) = Storage::new_temp()?;
        tmp_dirs.push(tmp_dir);

        let test_listener = TestValidatorEventListener::new(
            blocks_amount * sessions,
            global_validated_blocks.clone(),
        );
        let state_node_adapter =
            Arc::new(StateNodeAdapterStdImpl::new(test_listener.clone(), storage));

        let network = ValidatorNetwork {
            overlay_service: node.overlay_service.clone(),
            dht_client: node.dht_client.clone(),
            peer_resolver: node.peer_resolver.clone(),
        };
        let validator_config = ValidatorConfig {
            base_loop_delay: Duration::from_millis(50),
            max_loop_delay: Duration::from_secs(10),
        };

        let validator = Arc::new(ValidatorStdImpl::new(
            vec![test_listener.clone()],
            state_node_adapter,
            network,
            node.keypair.clone(),
            validator_config,
        ));

        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_blocks));
        let task = tokio::spawn(handle_validator(
            validator,
            semaphore.clone(),
            test_listener,
            blocks_amount,
            sessions,
            validators_subset_info.clone(),
            required_validations,
            global_validated_blocks.clone(),
        ));
        tasks.push(task);
    }

    // Await all validator tasks to complete
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    // Assert that all validations are completed as expected
    assert_eq!(
        global_validated_blocks.load(Ordering::SeqCst),
        required_validations as usize,
        "Not all required validations were completed"
    );

    Ok(())
}

async fn handle_validator(
    validator: Arc<ValidatorStdImpl>,
    semaphore: Arc<tokio::sync::Semaphore>,
    listener: Arc<TestValidatorEventListener>,
    blocks_amount: u32,
    sessions: u32,
    validators_subset_info: ValidatorSubsetInfo,
    required_validations: u32,
    global_validated_blocks: Arc<AtomicUsize>,
) -> Result<()> {
    for session in 1..=sessions {
        let blocks = create_blocks(blocks_amount);
        let collator_session_info = Arc::new(CollationSessionInfo::new(
            session,
            validators_subset_info.clone(),
            Some(validator.get_keypair()), // Assuming you have access to node's keypair here
        ));

        validator
            .add_session(Arc::new(
                ValidationSessionInfo::try_from(collator_session_info.clone()).unwrap(),
            ))
            .await?;

        for block in blocks {
            let block_clone = block.clone();
            let collator_info_clone = collator_session_info.clone();
            let v = validator.clone();

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            tokio::spawn(async move {
                v.validate(block_clone, collator_info_clone.seqno())
                    .await
                    .unwrap();
                drop(permit);
            });
        }
    }

    while global_validated_blocks.load(Ordering::SeqCst) < required_validations as usize {
        debug!(
            "Validator wait: {:?}",
            global_validated_blocks.load(Ordering::SeqCst)
        );
        sleep(Duration::from_millis(100)).await;
    }

    listener.notify.notified().await;
    Ok(())
}
