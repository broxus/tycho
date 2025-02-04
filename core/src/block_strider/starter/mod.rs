use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use serde::{Deserialize, Serialize};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::Storage;
use tycho_util::serde_helpers;

use crate::blockchain_rpc::BlockchainRpcClient;
use crate::global_config::ZerostateId;

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

/// Bootstrapping utils.
// TODO: Use it as a block provider?
#[derive(Clone)]
#[repr(transparent)]
pub struct Starter {
    inner: Arc<StarterInner>,
}

impl Starter {
    pub fn new(
        storage: Storage,
        blockchain_rpc_client: BlockchainRpcClient,
        zerostate: ZerostateId,
        config: StarterConfig,
    ) -> Self {
        Self {
            inner: Arc::new(StarterInner {
                storage,
                blockchain_rpc_client,
                zerostate,
                config,
            }),
        }
    }

    pub fn config(&self) -> &StarterConfig {
        &self.inner.config
    }

    /// Boot type when the node has not yet started syncing
    ///
    /// Returns the last masterchain key block id.
    pub async fn cold_boot<P>(
        &self,
        zerostate_provider: Option<P>,
        sync_from_genesis: bool,
    ) -> Result<BlockId>
    where
        P: ZerostateProvider,
    {
        self.inner
            .cold_boot(zerostate_provider, sync_from_genesis)
            .await
    }
}

struct StarterInner {
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,
    zerostate: ZerostateId,
    config: StarterConfig,
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

    ShardStateStuff::from_root(&block_id, root, tracker)
}
