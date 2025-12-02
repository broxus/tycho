use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_types::boc::Boc;
use tycho_types::models::{
    BlockId, IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardStateUnsplit,
};
use tycho_util::config::PartialConfig;
use tycho_util::fs::MappedFile;
use tycho_util::serde_helpers;

use self::starter_client::StarterClient;
use crate::blockchain_rpc::BlockchainRpcClient;
use crate::global_config::ZerostateId;
#[cfg(feature = "s3")]
use crate::s3::S3Client;
use crate::storage::{CoreStorage, QueueStateReader};

mod cold_boot;
mod starter_client;

#[derive(Default, Debug, Clone, PartialConfig, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StarterConfig {
    /// Choose persistent state which is at least this old.
    ///
    /// Default: None
    #[serde(with = "serde_helpers::humantime")]
    pub custom_boot_offset: Option<Duration>,

    /// Choose the nearest persistent state strictly before this seqno.
    ///
    /// Default: None
    #[important]
    pub start_from: Option<u32>,
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
            ignore_states,
            queue_state_handler,
            #[cfg(feature = "s3")]
            s3_client,
        } = self.optional_fields;

        #[allow(unused_labels)]
        let starter_client: Arc<dyn StarterClient> = 'client: {
            #[cfg(feature = "s3")]
            if let Some(s3_client) = s3_client {
                use self::starter_client::{HybridStarterClient, S3StarterClient};

                break 'client Arc::new(HybridStarterClient::new(
                    blockchain_rpc_client.clone(),
                    S3StarterClient::new(s3_client, storage.clone()),
                ));
            }

            Arc::new(blockchain_rpc_client.clone())
        };

        Starter {
            inner: Arc::new(StarterInner {
                ignore_states,
                storage,
                starter_client,
                blockchain_rpc_client,
                zerostate,
                config,
                queue_state_handler: queue_state_handler
                    .unwrap_or_else(|| Box::new(ValidateQueueState)),
            }),
        }
    }
}

impl<T2, T3, T4> StarterBuilder<((), T2, T3, T4)> {
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
    /// Whether to skip downloading persistent states.
    pub fn ignore_states(mut self, ignore_states: bool) -> Self {
        self.optional_fields.ignore_states = ignore_states;
        self
    }

    pub fn with_queue_state_handler<H: QueueStateHandler>(mut self, handler: H) -> Self {
        self.optional_fields.queue_state_handler = Some(castaway::match_type!(handler, {
            Box<dyn QueueStateHandler> as handler => handler,
            handler => Box::new(handler),
        }));
        self
    }

    #[cfg(feature = "s3")]
    pub fn with_s3_client(mut self, client: S3Client) -> Self {
        self.optional_fields.s3_client = Some(client);
        self
    }
}

#[derive(Default)]
struct BuilderFields {
    ignore_states: bool,
    queue_state_handler: Option<Box<dyn QueueStateHandler>>,

    #[cfg(feature = "s3")]
    s3_client: Option<S3Client>,
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

    pub fn queue_state_handler(&self) -> &dyn QueueStateHandler {
        self.inner.queue_state_handler.as_ref()
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum ColdBootType {
    Genesis,
    LatestPersistent,
}

struct StarterInner {
    ignore_states: bool,
    storage: CoreStorage,
    starter_client: Arc<dyn StarterClient>,
    // TODO: Access blockchain only though the starter client.
    blockchain_rpc_client: BlockchainRpcClient,
    zerostate: ZerostateId,
    config: StarterConfig,
    queue_state_handler: Box<dyn QueueStateHandler>,
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

/// Does some basic validation of the provided queue state.
#[derive(Debug, Clone, Copy)]
pub struct ValidateQueueState;

#[async_trait::async_trait]
impl QueueStateHandler for ValidateQueueState {
    async fn import_from_file(
        &self,
        top_update: &OutMsgQueueUpdates,
        file: File,
        block_id: &BlockId,
    ) -> Result<()> {
        tracing::info!(%block_id, "validating internal queue state from file");

        let top_update = top_update.clone();

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let mapped = MappedFile::from_existing_file(file)?;

            let mut reader = QueueStateReader::begin_from_mapped(mapped.as_slice(), &top_update)?;

            while let Some(mut part) = reader.read_next_queue_diff()? {
                while let Some(cell) = part.read_next_message()? {
                    let msg_hash = cell.repr_hash();
                    let msg = cell.parse::<Message<'_>>()?;
                    let MsgInfo::Int(int_msg_info) = &msg.info else {
                        anyhow::bail!("non-internal message in the queue in msg {msg_hash}");
                    };

                    let IntAddr::Std(_dest) = &int_msg_info.dst else {
                        anyhow::bail!("non-std destination address in msg {msg_hash}");
                    };

                    let IntAddr::Std(_src) = &int_msg_info.src else {
                        anyhow::bail!("non-std destination address in msg {msg_hash}");
                    };
                }
            }

            reader.finish()
        })
        .await?
    }
}
