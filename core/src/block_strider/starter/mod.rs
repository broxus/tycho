use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_types::boc::Boc;
use tycho_types::models::{BlockId, OutMsgQueueUpdates, ShardStateUnsplit};
use tycho_util::serde_helpers;

use crate::blockchain_rpc::BlockchainRpcClient;
use crate::global_config::ZerostateId;
use crate::storage::{CoreStorage, ShardStateStorage};

mod cold_boot;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StarterConfig {
    /// Choose persistent state which is at least this old.
    ///
    /// Default: None
    #[serde(with = "serde_helpers::humantime")]
    pub custom_boot_offset: Option<Duration>,
}

pub struct StarterBuilder<
    MandatoryFields = (CoreStorage, BlockchainRpcClient, ZerostateId, StarterConfig),
> {
    mandatory_fields: MandatoryFields,
    optional_fields: BuilderFields,
}

impl Default for StarterBuilder<((), (), (), ())> {
    #[inline]
    fn default() -> Self {
        Self {
            mandatory_fields: Default::default(),
            optional_fields: Default::default(),
        }
    }
}

impl StarterBuilder {
    pub fn build(self) -> Starter {
        let (storage, blockchain_rpc_client, zerostate, config) = self.mandatory_fields;
        let BuilderFields {
            queue_state_handler,
        } = self.optional_fields;

        Starter {
            inner: Arc::new(StarterInner {
                storage,
                blockchain_rpc_client,
                zerostate,
                config,
                queue_state_handler,
            }),
        }
    }
}

impl<T2, T3, T4> StarterBuilder<((), T2, T3, T4)> {
    // TODO: Use `CoreStorage`.
    pub fn with_storage(self, storage: CoreStorage) -> StarterBuilder<(CoreStorage, T2, T3, T4)> {
        let ((), client, id, config) = self.mandatory_fields;
        StarterBuilder {
            mandatory_fields: (storage, client, id, config),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1, T3, T4> StarterBuilder<(T1, (), T3, T4)> {
    pub fn with_blockchain_rpc_client(
        self,
        client: BlockchainRpcClient,
    ) -> StarterBuilder<(T1, BlockchainRpcClient, T3, T4)> {
        let (storage, (), id, config) = self.mandatory_fields;
        StarterBuilder {
            mandatory_fields: (storage, client, id, config),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1, T2, T4> StarterBuilder<(T1, T2, (), T4)> {
    pub fn with_zerostate_id(
        self,
        zerostate_id: ZerostateId,
    ) -> StarterBuilder<(T1, T2, ZerostateId, T4)> {
        let (storage, client, (), config) = self.mandatory_fields;
        StarterBuilder {
            mandatory_fields: (storage, client, zerostate_id, config),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1, T2, T3> StarterBuilder<(T1, T2, T3, ())> {
    pub fn with_config(self, config: StarterConfig) -> StarterBuilder<(T1, T2, T3, StarterConfig)> {
        let (storage, client, id, ()) = self.mandatory_fields;
        StarterBuilder {
            mandatory_fields: (storage, client, id, config),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T> StarterBuilder<T> {
    pub fn with_queue_state_handler<H: QueueStateHandler>(mut self, handler: H) -> Self {
        self.optional_fields.queue_state_handler = Some(castaway::match_type!(handler, {
            Box<dyn QueueStateHandler> as handler => handler,
            handler => Box::new(handler),
        }));
        self
    }
}

#[derive(Default)]
struct BuilderFields {
    queue_state_handler: Option<Box<dyn QueueStateHandler>>,
}

/// Bootstrapping utils.
// TODO: Use it as a block provider?
#[derive(Clone)]
#[repr(transparent)]
pub struct Starter {
    inner: Arc<StarterInner>,
}

impl Starter {
    pub fn builder() -> StarterBuilder<((), (), (), ())> {
        StarterBuilder::default()
    }

    pub fn config(&self) -> &StarterConfig {
        &self.inner.config
    }

    pub fn queue_state_handler(&self) -> Option<&dyn QueueStateHandler> {
        self.inner.queue_state_handler.as_deref()
    }

    /// Boot type when the node has not yet started syncing
    ///
    /// Returns the last masterchain key block id.
    pub async fn cold_boot<P>(
        &self,
        boot_type: ColdBootType,
        zerostate_provider: Option<P>,
    ) -> Result<BlockId>
    where
        P: ZerostateProvider,
    {
        self.inner.cold_boot(boot_type, zerostate_provider).await
    }

    pub async fn init_allowed_workchains(
        shard_state_storage: &ShardStateStorage,
        last_mc_block_id: &BlockId,
    ) -> Result<()> {
        StarterInner::init_allowed_workchains(shard_state_storage, last_mc_block_id).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum ColdBootType {
    Genesis,
    LatestPersistent,
}

struct StarterInner {
    storage: CoreStorage,
    blockchain_rpc_client: BlockchainRpcClient,
    zerostate: ZerostateId,
    config: StarterConfig,
    queue_state_handler: Option<Box<dyn QueueStateHandler>>,
}

pub trait ZerostateProvider {
    fn load_zerostates(
        &self,
        tracker: &MinRefMcStateTracker,
    ) -> impl Iterator<Item = Result<ShardStateStuff>>;
}

impl ZerostateProvider for () {
    fn load_zerostates(
        &self,
        _: &MinRefMcStateTracker,
    ) -> impl Iterator<Item = Result<ShardStateStuff>> {
        std::iter::empty()
    }
}

pub struct FileZerostateProvider(pub Vec<PathBuf>);

impl ZerostateProvider for FileZerostateProvider {
    fn load_zerostates(
        &self,
        tracker: &MinRefMcStateTracker,
    ) -> impl Iterator<Item = Result<ShardStateStuff>> {
        self.0.iter().map(move |path| load_zerostate(tracker, path))
    }
}

fn load_zerostate(tracker: &MinRefMcStateTracker, path: &PathBuf) -> Result<ShardStateStuff> {
    let data = std::fs::read(path).context("failed to read file")?;
    let file_hash = Boc::file_hash_blake(&data);

    let root = Boc::decode(data).context("failed to decode BOC")?;
    let root_hash = *root.repr_hash();

    let state = root
        .parse::<ShardStateUnsplit>()
        .context("failed to parse state")?;

    anyhow::ensure!(state.seqno == 0, "not a zerostate");

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    ShardStateStuff::from_root(&block_id, root, tracker.insert_untracked())
}

#[async_trait::async_trait]
pub trait QueueStateHandler: Send + Sync + 'static {
    async fn import_from_file(
        &self,
        top_update: &OutMsgQueueUpdates,
        file: File,
        block_id: &BlockId,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl<T: QueueStateHandler + ?Sized> QueueStateHandler for Arc<T> {
    async fn import_from_file(
        &self,
        top_update: &OutMsgQueueUpdates,
        file: File,
        block_id: &BlockId,
    ) -> Result<()> {
        T::import_from_file(self, top_update, file, block_id).await
    }
}

#[async_trait::async_trait]
impl<T: QueueStateHandler + ?Sized> QueueStateHandler for Box<T> {
    async fn import_from_file(
        &self,
        top_update: &OutMsgQueueUpdates,
        file: File,
        block_id: &BlockId,
    ) -> Result<()> {
        T::import_from_file(self, top_update, file, block_id).await
    }
}
