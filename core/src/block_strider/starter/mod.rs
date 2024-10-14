use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::Storage;

use crate::blockchain_rpc::BlockchainRpcClient;
use crate::global_config::ZerostateId;

mod cold_boot;

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
    ) -> Self {
        Self {
            inner: Arc::new(StarterInner {
                storage,
                blockchain_rpc_client,
                zerostate,
            }),
        }
    }

    /// Boot type when the node has not yet started syncing
    ///
    /// Returns the last masterchain key block id.
    pub async fn cold_boot<P>(&self, zerostate_provider: Option<P>) -> Result<BlockId>
    where
        P: ZerostateProvider,
    {
        self.inner.cold_boot(zerostate_provider).await
    }
}

struct StarterInner {
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,
    zerostate: ZerostateId,
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
