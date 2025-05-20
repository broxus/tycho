use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_types::boc::de::ProcessedCells;
use everscale_types::boc::{de, Boc};
use everscale_types::cell::Cell;
use everscale_types::models::{BlockId, ShardAccounts, ShardStateUnsplit};
use everscale_types::prelude::CellFamily;
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
        boot_type: ColdBootType,
        zerostate_provider: Option<P>,
    ) -> Result<BlockId>
    where
        P: ZerostateProvider,
    {
        self.inner.cold_boot(boot_type, zerostate_provider).await
    }
}

pub enum ColdBootType {
    Genesis,
    LatestPersistent,
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
    tracing::info!("loading zerostate {}", path.display());

    let data = std::fs::read(path).context("failed to read file")?;
    let file_hash = Boc::file_hash_blake(&data);

    let boc = de::BocHeader::decode(&data, &de::Options {
        min_roots: None,
        // NOTE: We must specify the max number of roots to avoid the default
        //       limit (which is quite low since it is rarely used in practice).
        max_roots: Some(33),
    })?;

    let mut roots = boc.roots().to_vec();
    roots.reverse();

    let cells = boc.finalize(Cell::empty_context())?;

    let mut parsed = ParsedBocHeader { roots, cells };

    let root_state = parsed.next().unwrap();
    let root_hash = *root_state.repr_hash();

    tracing::info!("loading state");

    let state = root_state
        .parse::<ShardStateUnsplit>()
        .context("failed to parse state")?;

    anyhow::ensure!(state.seqno == 0, "not a zerostate");

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    tracing::info!("state loaded");

    let mut shard_accounts = BTreeMap::new();
    while let Some(root_prefix) = parsed.next() {
        let shard_prefix = root_prefix
            .parse::<u64>()
            .context("failed to parse shard prefix")?;

        let root_shard_accounts = parsed.next().unwrap();
        let accounts = root_shard_accounts
            .parse::<ShardAccounts>()
            .context("failed to parse shard accounts")?;

        shard_accounts.insert(shard_prefix, accounts);
    }

    ShardStateStuff::from_root_and_accounts(&block_id, root_state, shard_accounts, tracker)
}

struct ParsedBocHeader {
    roots: Vec<u32>,
    cells: ProcessedCells,
}

impl Iterator for ParsedBocHeader {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.roots.pop()?;
        self.cells.get(index)
    }
}
