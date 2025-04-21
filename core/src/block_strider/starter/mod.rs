use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_types::boc::Boc;
use everscale_types::cell::Cell;
use everscale_types::models::{BlockId, ShardAccounts, ShardStateUnsplit};
use serde::{Deserialize, Serialize};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateDataId, ShardStateStuff};
use tycho_storage::Storage;
use tycho_util::{serde_helpers, FastHashMap};

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
        self.0.iter().map(move |path| {
            load_zerostate(tracker, path, &PathBuf::from("/var/node/data/accounts.boc"))
        })
    }
}

fn load_zerostate(
    tracker: &MinRefMcStateTracker,
    path: &PathBuf,
    accounts_path: &PathBuf,
) -> Result<ShardStateStuff> {
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

    let mut data_roots: FastHashMap<u8, ShardAccounts> = FastHashMap::default();

    for shard_data_id in ShardStateDataId::iter() {
        let accs = ShardAccounts::new();
        data_roots.insert(shard_data_id.0, accs);
    }

    {
        let data = std::fs::read(accounts_path).context("failed to read file")?;
        let root = Boc::decode(data).context("failed to decode BOC")?;

        let accounts = root
            .parse::<ShardAccounts>()
            .context("failed to parse accounts")?;

        for item in accounts.iter() {
            let account = item?;
            let data_shard_id = ShardStateDataId::from_account(account.0.as_array()).0;

            match data_roots.entry(data_shard_id) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().set(account.0, account.1, account.2)?;
                }
                Entry::Vacant(entry) => {
                    let mut accs = ShardAccounts::new();
                    accs.set(account.0, account.1, account.2)?;
                    entry.insert(accs);
                }
            }
        }
    };

    ShardStateStuff::from_root_and_accounts(&block_id, root, data_roots, tracker)
}
