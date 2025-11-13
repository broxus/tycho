use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_crypto::ed25519;
use tycho_network::{
    DhtClient, DhtService, Network, OverlayService, PeerInfo, PeerResolver, PublicOverlay, Router,
};
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::models::{BlockId, ValidatorSet};

pub use self::config::NodeBaseConfig;
pub use self::keys::NodeKeys;
use crate::block_strider::{
    ArchiveBlockProvider, BlockProvider, BlockStrider, BlockSubscriber, BlockchainBlockProvider,
    ColdBootType, FileZerostateProvider, PersistentBlockStriderState, QueueStateHandler, Starter,
    StorageBlockProvider,
};
use crate::blockchain_rpc::{
    BlockchainRpcClient, BlockchainRpcService, BroadcastListener, SelfBroadcastListener,
};
use crate::global_config::{GlobalConfig, ZerostateId};
use crate::overlay_client::{PublicOverlayClient, ValidatorsResolver};
#[cfg(feature = "s3")]
use crate::s3::S3Client;
use crate::storage::{CoreStorage, CoreStorageConfig};

mod config;
mod keys;

pub struct NodeBase {
    pub base_config: NodeBaseConfig,
    pub global_config: GlobalConfig,

    pub keypair: Arc<ed25519::KeyPair>,
    pub network: Network,
    pub dht_client: DhtClient,
    pub peer_resolver: PeerResolver,
    pub overlay_service: OverlayService,

    pub storage_context: StorageContext,
    pub core_storage: CoreStorage,

    pub blockchain_rpc_client: BlockchainRpcClient,

    #[cfg(feature = "s3")]
    pub s3_client: S3Client,
}

impl NodeBase {
    pub fn builder<'a>(
        base_config: &'a NodeBaseConfig,
        global_config: &'a GlobalConfig,
    ) -> NodeBaseBuilder<'a, ()> {
        NodeBaseBuilder::new(base_config, global_config)
    }

    /// Wait for some peers and boot the node.
    pub async fn init(
        &self,
        boot_type: ColdBootType,
        import_zerostate: Option<Vec<PathBuf>>,
        queue_state_handler: Option<Box<dyn QueueStateHandler>>,
    ) -> Result<BlockId> {
        self.wait_for_neighbours(3).await;

        let init_block_id = self
            .boot(boot_type, import_zerostate, queue_state_handler)
            .await
            .context("failed to init node")?;

        tracing::info!(%init_block_id, "node initialized");

        Ok(init_block_id)
    }

    /// Wait for at least `count` public overlay peers to resolve.
    pub async fn wait_for_neighbours(&self, count: usize) {
        // Ensure that there are some neighbours
        tracing::info!("waiting for initial neighbours");
        self.blockchain_rpc_client
            .overlay_client()
            .neighbours()
            .wait_for_peers(count)
            .await;
        tracing::info!("found initial neighbours");
    }

    /// Initialize the node and return the init block id.
    pub async fn boot(
        &self,
        boot_type: ColdBootType,
        zerostates: Option<Vec<PathBuf>>,
        queue_state_handler: Option<Box<dyn QueueStateHandler>>,
    ) -> Result<BlockId> {
        let node_state = self.core_storage.node_state();

        let last_mc_block_id = match node_state.load_last_mc_block_id() {
            Some(block_id) => block_id,
            None => {
                let mut starter = Starter::builder()
                    .with_storage(self.core_storage.clone())
                    .with_blockchain_rpc_client(self.blockchain_rpc_client.clone())
                    .with_zerostate_id(self.global_config.zerostate)
                    .with_config(self.base_config.starter.clone());

                #[cfg(feature = "s3")]
                {
                    starter = starter.with_s3(self.s3_client.clone());
                }

                if let Some(handler) = queue_state_handler {
                    starter = starter.with_queue_state_handler(handler);
                }

                starter
                    .build()
                    .cold_boot(boot_type, zerostates.map(FileZerostateProvider))
                    .await?
            }
        };

        tracing::info!(
            %last_mc_block_id,
            "boot finished"
        );

        Ok(last_mc_block_id)
    }

    pub fn validator_resolver(&self) -> &ValidatorsResolver {
        self.blockchain_rpc_client
            .overlay_client()
            .validators_resolver()
    }

    /// Update current validator targets with the specified set.
    pub fn update_validator_set(&self, vset: &ValidatorSet) {
        self.validator_resolver().update_validator_set(vset);
    }

    /// Update current validator targets using the validator set from the provider
    pub async fn update_validator_set_from_shard_state(&self, block_id: &BlockId) -> Result<()> {
        // notify subscriber with an initial validators list
        let mc_state = self
            .core_storage
            .shard_state_storage()
            .load_state(block_id.seqno, block_id)
            .await?;

        let config = mc_state.config_params()?;
        let current_vset = config.get_current_validator_set()?;
        self.update_validator_set(&current_vset);
        Ok(())
    }

    pub fn build_archive_block_provider(&self) -> ArchiveBlockProvider {
        ArchiveBlockProvider::new(
            Arc::new(self.blockchain_rpc_client.clone()),
            self.core_storage.clone(),
            self.base_config.archive_block_provider.clone(),
        )
    }

    pub fn build_blockchain_block_provider(&self) -> BlockchainBlockProvider {
        BlockchainBlockProvider::new(
            self.blockchain_rpc_client.clone(),
            self.core_storage.clone(),
            self.base_config.blockchain_block_provider.clone(),
        )
    }

    pub fn build_storage_block_provider(&self) -> StorageBlockProvider {
        StorageBlockProvider::new(self.core_storage.clone())
    }

    /// Creates a new [`BlockStrider`] using options from the base config.
    pub fn build_strider<P, S>(
        &self,
        provider: P,
        subscriber: S,
    ) -> BlockStrider<PersistentBlockStriderState, P, S>
    where
        P: BlockProvider,
        S: BlockSubscriber,
    {
        let state = PersistentBlockStriderState::new(
            self.global_config.zerostate.as_block_id(),
            self.core_storage.clone(),
        );

        BlockStrider::builder()
            .with_state(state)
            .with_provider(provider)
            .with_block_subscriber(subscriber)
            .build()
    }
}

pub struct NodeBaseBuilder<'a, Step = ()> {
    base_config: &'a NodeBaseConfig,
    global_config: &'a GlobalConfig,
    step: Step,
}

impl<'a> NodeBaseBuilder<'a, ()> {
    pub fn new(base_config: &'a NodeBaseConfig, global_config: &'a GlobalConfig) -> Self {
        Self {
            base_config,
            global_config,
            step: (),
        }
    }

    pub fn init_network(
        self,
        public_addr: SocketAddr,
        secret_key: &ed25519::SecretKey,
    ) -> Result<NodeBaseBuilder<'a, init::Step0>> {
        let net = ConfiguredNetwork::new(
            public_addr,
            secret_key,
            self.base_config,
            &self.global_config.bootstrap_peers,
        )?;

        Ok(NodeBaseBuilder {
            base_config: self.base_config,
            global_config: self.global_config,
            step: init::Step0 { net },
        })
    }
}

impl<'a> NodeBaseBuilder<'a, init::Step0> {
    // TODO: Add some options here if needed.
    pub async fn init_storage(self) -> Result<NodeBaseBuilder<'a, init::Step1>> {
        let store =
            ConfiguredStorage::new(&self.base_config.storage, &self.base_config.core_storage)
                .await?;

        Ok(NodeBaseBuilder {
            base_config: self.base_config,
            global_config: self.global_config,
            step: init::Step1 {
                prev_step: self.step,
                store,
            },
        })
    }
}

impl<'a> NodeBaseBuilder<'a, init::Step1> {
    pub fn init_blockchain_rpc<RL, SL>(
        self,
        remote_broadcast_listener: RL,
        self_broadcast_listener: SL,
    ) -> Result<NodeBaseBuilder<'a, init::Step2>>
    where
        RL: BroadcastListener,
        SL: SelfBroadcastListener,
    {
        let (_, blockchain_rpc_client) = self.step.prev_step.net.add_blockchain_rpc(
            &self.global_config.zerostate,
            self.step.store.core_storage.clone(),
            remote_broadcast_listener,
            self_broadcast_listener,
            self.base_config,
        );

        Ok(NodeBaseBuilder {
            base_config: self.base_config,
            global_config: self.global_config,
            step: init::Step2 {
                prev_step: self.step,
                blockchain_rpc_client,
            },
        })
    }
}

#[cfg(not(feature = "s3"))]
impl<'a> NodeBaseBuilder<'a, init::Final> {
    pub fn build(self) -> Result<NodeBase> {
        let net = self.step.prev_step.prev_step.net;
        let store = self.step.prev_step.store;
        let blockchain_rpc_client = self.step.blockchain_rpc_client;

        Ok(NodeBase {
            base_config: self.base_config.clone(),
            global_config: self.global_config.clone(),
            keypair: net.keypair,
            network: net.network,
            dht_client: net.dht_client,
            peer_resolver: net.peer_resolver,
            overlay_service: net.overlay_service,
            storage_context: store.context,
            core_storage: store.core_storage,
            blockchain_rpc_client,
        })
    }
}

#[cfg(feature = "s3")]
impl<'a> NodeBaseBuilder<'a, init::Step2> {
    pub fn init_s3(self) -> Result<NodeBaseBuilder<'a, init::Step3>> {
        let s3_client = S3Client::new(&self.base_config.s3_client)?;

        Ok(NodeBaseBuilder {
            base_config: self.base_config,
            global_config: self.global_config,
            step: init::Step3 {
                prev_step: self.step,
                s3_client,
            },
        })
    }
}

#[cfg(feature = "s3")]
impl NodeBaseBuilder<'_, init::Final> {
    pub fn build(self) -> Result<NodeBase> {
        let net = self.step.prev_step.prev_step.prev_step.net;
        let store = self.step.prev_step.prev_step.store;
        let blockchain_rpc_client = self.step.prev_step.blockchain_rpc_client;

        #[cfg(feature = "s3")]
        let s3_client = self.step.s3_client;

        Ok(NodeBase {
            base_config: self.base_config.clone(),
            global_config: self.global_config.clone(),
            keypair: net.keypair,
            network: net.network,
            dht_client: net.dht_client,
            peer_resolver: net.peer_resolver,
            overlay_service: net.overlay_service,
            storage_context: store.context,
            core_storage: store.core_storage,
            blockchain_rpc_client,

            #[cfg(feature = "s3")]
            s3_client,
        })
    }
}

impl<'a, Step> NodeBaseBuilder<'a, Step> {
    pub fn base_config(&self) -> &'a NodeBaseConfig {
        self.base_config
    }

    pub fn global_config(&self) -> &'a GlobalConfig {
        self.global_config
    }
}

impl<Step: AsRef<init::Step0>> NodeBaseBuilder<'_, Step> {
    pub fn keypair(&self) -> &Arc<ed25519::KeyPair> {
        &self.step.as_ref().net.keypair
    }

    pub fn network(&self) -> &Network {
        &self.step.as_ref().net.network
    }

    pub fn dht_client(&self) -> &DhtClient {
        &self.step.as_ref().net.dht_client
    }

    pub fn peer_resolver(&self) -> &PeerResolver {
        &self.step.as_ref().net.peer_resolver
    }

    pub fn overlay_service(&self) -> &OverlayService {
        &self.step.as_ref().net.overlay_service
    }
}

impl<Step: AsRef<init::Step1>> NodeBaseBuilder<'_, Step> {
    pub fn storage_context(&self) -> &StorageContext {
        &self.step.as_ref().store.context
    }

    pub fn core_storage(&self) -> &CoreStorage {
        &self.step.as_ref().store.core_storage
    }
}

impl<Step: AsRef<init::Step2>> NodeBaseBuilder<'_, Step> {
    pub fn blockchain_rpc_client(&self) -> &BlockchainRpcClient {
        &self.step.as_ref().blockchain_rpc_client
    }
}

pub mod init {
    use super::*;

    #[cfg(feature = "s3")]
    pub type Final = Step3;

    #[cfg(not(feature = "s3"))]
    pub type Final = Step2;

    /// Node with network.
    pub struct Step0 {
        pub(super) net: ConfiguredNetwork,
    }

    impl AsRef<Step0> for Step0 {
        #[inline]
        fn as_ref(&self) -> &Step0 {
            self
        }
    }

    /// Node with network and storage.
    pub struct Step1 {
        pub(super) prev_step: Step0,
        pub(super) store: ConfiguredStorage,
    }

    impl AsRef<Step0> for Step1 {
        #[inline]
        fn as_ref(&self) -> &Step0 {
            &self.prev_step
        }
    }

    impl AsRef<Step1> for Step1 {
        #[inline]
        fn as_ref(&self) -> &Step1 {
            self
        }
    }

    /// Node with network, storage and public overlay.
    pub struct Step2 {
        pub(super) prev_step: Step1,
        pub(super) blockchain_rpc_client: BlockchainRpcClient,
    }

    impl AsRef<Step0> for Step2 {
        #[inline]
        fn as_ref(&self) -> &Step0 {
            &self.prev_step.prev_step
        }
    }

    impl AsRef<Step1> for Step2 {
        #[inline]
        fn as_ref(&self) -> &Step1 {
            &self.prev_step
        }
    }

    impl AsRef<Step2> for Step2 {
        #[inline]
        fn as_ref(&self) -> &Step2 {
            self
        }
    }

    /// Node with network, storage, public overlay and S3 client.
    #[cfg(feature = "s3")]
    pub struct Step3 {
        pub(super) prev_step: Step2,
        pub(super) s3_client: S3Client,
    }

    #[cfg(feature = "s3")]
    impl AsRef<Step0> for Step3 {
        #[inline]
        fn as_ref(&self) -> &Step0 {
            &self.prev_step.prev_step.prev_step
        }
    }

    #[cfg(feature = "s3")]
    impl AsRef<Step1> for Step3 {
        #[inline]
        fn as_ref(&self) -> &Step1 {
            &self.prev_step.prev_step
        }
    }

    #[cfg(feature = "s3")]
    impl AsRef<Step2> for Step3 {
        #[inline]
        fn as_ref(&self) -> &Step2 {
            &self.prev_step
        }
    }

    #[cfg(feature = "s3")]
    impl AsRef<Step3> for Step3 {
        #[inline]
        fn as_ref(&self) -> &Step3 {
            self
        }
    }
}

pub struct ConfiguredNetwork {
    pub keypair: Arc<ed25519::KeyPair>,
    pub network: Network,
    pub dht_client: DhtClient,
    pub peer_resolver: PeerResolver,
    pub overlay_service: OverlayService,
}

impl ConfiguredNetwork {
    pub fn new(
        public_addr: SocketAddr,
        secret_key: &ed25519::SecretKey,
        base_config: &NodeBaseConfig,
        bootstrap_peers: &[PeerInfo],
    ) -> Result<Self> {
        // Setup network
        let keypair = Arc::new(ed25519::KeyPair::from(secret_key));
        let local_id = keypair.public_key.into();

        let (dht_tasks, dht_service) = DhtService::builder(local_id)
            .with_config(base_config.dht.clone())
            .build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(local_id)
            .with_config(base_config.overlay.clone())
            .with_dht_service(dht_service.clone())
            .build();

        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();

        let local_addr = SocketAddr::from((base_config.local_ip, base_config.port));

        let network = Network::builder()
            .with_config(base_config.network.clone())
            .with_private_key(secret_key.to_bytes())
            .with_remote_addr(public_addr)
            .build(local_addr, router)
            .context("failed to build node network")?;

        dht_tasks.spawn(&network);
        overlay_tasks.spawn(&network);

        let dht_client = dht_service.make_client(&network);
        let peer_resolver = dht_service
            .make_peer_resolver()
            .with_config(base_config.peer_resolver.clone())
            .build(&network);

        let mut bootstrap_peer_count = 0usize;
        for peer in bootstrap_peers {
            let is_new = dht_client.add_peer(Arc::new(peer.clone()))?;
            bootstrap_peer_count += is_new as usize;
        }

        tracing::info!(
            %local_id,
            %local_addr,
            %public_addr,
            bootstrap_peers = bootstrap_peer_count,
            "initialized network"
        );

        Ok(Self {
            keypair,
            network,
            dht_client,
            peer_resolver,
            overlay_service,
        })
    }

    pub fn add_blockchain_rpc<BL, SL>(
        &self,
        zerostate: &ZerostateId,
        storage: CoreStorage,
        remote_broadcast_listener: BL,
        self_broadcast_listener: SL,
        base_config: &NodeBaseConfig,
    ) -> (BlockchainRpcService<BL>, BlockchainRpcClient)
    where
        BL: BroadcastListener,
        SL: SelfBroadcastListener,
    {
        let blockchain_rpc_service = BlockchainRpcService::builder()
            .with_config(base_config.blockchain_rpc_service.clone())
            .with_storage(storage)
            .with_broadcast_listener(remote_broadcast_listener)
            .build();

        let public_overlay = PublicOverlay::builder(zerostate.compute_public_overlay_id())
            .named("blockchain_rpc")
            .with_peer_resolver(self.peer_resolver.clone())
            .build(blockchain_rpc_service.clone());
        self.overlay_service.add_public_overlay(&public_overlay);

        let blockchain_rpc_client = BlockchainRpcClient::builder()
            .with_config(base_config.blockchain_rpc_client.clone())
            .with_public_overlay_client(PublicOverlayClient::new(
                self.network.clone(),
                public_overlay,
                base_config.public_overlay_client.clone(),
            ))
            .with_self_broadcast_listener(self_broadcast_listener)
            .build();

        tracing::info!(
            overlay_id = %blockchain_rpc_client.overlay().overlay_id(),
            "initialized blockchain rpc"
        );

        (blockchain_rpc_service, blockchain_rpc_client)
    }
}

pub struct ConfiguredStorage {
    pub context: StorageContext,
    pub core_storage: CoreStorage,
}

impl ConfiguredStorage {
    pub async fn new(
        storage_config: &StorageConfig,
        core_storage_config: &CoreStorageConfig,
    ) -> Result<Self> {
        let context = StorageContext::new(storage_config.clone())
            .await
            .context("failed to create storage context")?;
        let core_storage = CoreStorage::open(context.clone(), core_storage_config.clone())
            .await
            .context("failed to create storage")?;
        tracing::info!(
            root_dir = %core_storage.context().root_dir().path().display(),
            "initialized storage"
        );

        Ok(Self {
            context,
            core_storage,
        })
    }
}
