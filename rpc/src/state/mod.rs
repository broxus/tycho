use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;
use futures_util::future::BoxFuture;
use parking_lot::RwLock;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_core::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_storage::{CodeHashesIter, Storage};
use tycho_util::FastHashMap;

use crate::config::RpcConfig;
use crate::models::GenTimings;

pub struct RpcStateBuilder<MandatoryFields = (Storage, BlockchainRpcClient)> {
    config: RpcConfig,
    mandatory_fields: MandatoryFields,
}

impl RpcStateBuilder {
    pub fn build(self) -> RpcState {
        let (storage, blockchain_rpc_client) = self.mandatory_fields;

        RpcState {
            inner: Arc::new(Inner {
                config: self.config,
                storage,
                blockchain_rpc_client,
                mc_accounts: Default::default(),
                sc_accounts: Default::default(),
            }),
        }
    }
}

impl<T2> RpcStateBuilder<((), T2)> {
    pub fn with_storage(self, storage: Storage) -> RpcStateBuilder<(Storage, T2)> {
        let (_, bc_rpc_client) = self.mandatory_fields;

        RpcStateBuilder {
            config: self.config,
            mandatory_fields: (storage, bc_rpc_client),
        }
    }
}

impl<T1> RpcStateBuilder<(T1, ())> {
    pub fn with_blockchain_rpc_client(
        self,
        client: BlockchainRpcClient,
    ) -> RpcStateBuilder<(T1, BlockchainRpcClient)> {
        let (storage, _) = self.mandatory_fields;

        RpcStateBuilder {
            config: self.config,
            mandatory_fields: (storage, client),
        }
    }
}

impl<T1, T2> RpcStateBuilder<(T1, T2)> {
    pub fn with_config(self, config: RpcConfig) -> RpcStateBuilder<(T1, T2)> {
        RpcStateBuilder { config, ..self }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct RpcState {
    inner: Arc<Inner>,
}

impl RpcState {
    pub fn builder() -> RpcStateBuilder<()> {
        RpcStateBuilder {
            config: RpcConfig::default(),
            mandatory_fields: (),
        }
    }

    pub async fn broadcast_external_message(&self, message: &[u8]) {
        self.inner
            .blockchain_rpc_client
            .broadcast_external_message(message)
            .await;
    }

    pub fn get_account_state(&self, address: &StdAddr) -> Result<LoadedAccountState> {
        self.inner.get_account_state(address)
    }

    pub fn get_accounts_by_code_hash(
        &self,
        code_hash: &HashBytes,
        continuation: Option<&StdAddr>,
    ) -> Result<CodeHashesIter<'_>> {
        let Some(storage) = &self.inner.storage.rpc_storage() else {
            anyhow::bail!("not supported");
        };
        storage.get_accounts_by_code_hash(code_hash, continuation)
    }
}

impl StateSubscriber for RpcState {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(self.inner.update(&cx.block, Some(&cx.state)))
    }
}

impl BlockSubscriber for RpcState {
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::HandleBlockFut<'a> {
        Box::pin(self.inner.update(&cx.block, None))
    }
}

struct Inner {
    config: RpcConfig,
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,
    mc_accounts: RwLock<Option<CachedAccounts>>,
    sc_accounts: RwLock<FastHashMap<ShardIdent, CachedAccounts>>,
}

impl Inner {
    fn get_account_state(&self, address: &StdAddr) -> Result<LoadedAccountState> {
        let is_masterchain = address.is_masterchain();

        if is_masterchain {
            // Search in masterchain cache
            match &*self.mc_accounts.read() {
                None => Ok(LoadedAccountState::NotReady),
                Some(cache) => cache.get(&address.address),
            }
        } else {
            let cache = self.sc_accounts.read();
            let mut state = Ok(LoadedAccountState::NotReady);

            // Search in all shard caches
            let mut gen_utime = 0;
            for (shard, cache) in &*cache {
                if !shard.contains_account(&address.address) {
                    continue;
                }

                gen_utime = cache.gen_utime;
                state = cache.get(&address.address);
            }

            // Handle case when account is not found in any shard
            if matches!(&state, Ok(LoadedAccountState::NotReady) if gen_utime > 0) {
                state = Ok(LoadedAccountState::NotFound {
                    timings: GenTimings {
                        gen_lt: 0,
                        gen_utime,
                    },
                });
            }

            // Done
            state
        }
    }

    async fn update(&self, block: &BlockStuff, state: Option<&ShardStateStuff>) -> Result<()> {
        let accounts = if let Some(state) = state {
            Some(self.update_accounts_cache(block, state)?)
        } else {
            None
        };

        if let Some(rpc_storage) = self.storage.rpc_storage() {
            rpc_storage.update(block.clone(), accounts).await?;
        }
        Ok(())
    }

    fn update_accounts_cache(
        &self,
        block: &BlockStuff,
        state: &ShardStateStuff,
    ) -> Result<ShardAccountsDict> {
        let shard = block.id().shard;

        // TODO: Get `gen_utime` from somewhere else.
        let block_info = block.load_info()?;

        let accounts = state.state().load_accounts()?.dict().clone();

        let cached = CachedAccounts {
            accounts: accounts.clone(),
            mc_ref_hanlde: state.ref_mc_state_handle().clone(),
            gen_utime: block_info.gen_utime,
        };

        if shard.is_masterchain() {
            *self.mc_accounts.write() = Some(cached);
        } else {
            let mut cache = self.sc_accounts.write();

            cache.insert(shard, cached);
            if block_info.after_merge || block_info.after_split {
                tracing::debug!("clearing shard states cache after shards merge/split");

                match block_info.load_prev_ref()? {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    PrevBlockRef::Single(..) => {
                        // Compute parent shard of the B' or B"
                        let parent = shard
                            .merge()
                            .ok_or(everscale_types::error::Error::InvalidData)?;

                        let opposite = shard.opposite().expect("after split");

                        // Remove parent shard state
                        if cache.contains_key(&shard) && cache.contains_key(&opposite) {
                            cache.remove(&parent);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    PrevBlockRef::AfterMerge { .. } => {
                        // Compute parent shard of the B' or B"
                        let (left, right) = shard
                            .split()
                            .ok_or(everscale_types::error::Error::InvalidData)?;

                        // Find and remove all parent shards
                        cache.remove(&left);
                        cache.remove(&right);
                    }
                }
            }
        }

        Ok(accounts)
    }
}

pub enum LoadedAccountState {
    NotReady,
    NotFound {
        timings: GenTimings,
    },
    Found {
        state: ShardAccount,
        mc_ref_handle: RefMcStateHandle,
        gen_utime: u32,
    },
}

struct CachedAccounts {
    accounts: ShardAccountsDict,
    mc_ref_hanlde: RefMcStateHandle,
    gen_utime: u32,
}

impl CachedAccounts {
    fn get(&self, account: &HashBytes) -> Result<LoadedAccountState> {
        let Some((_, state)) = self.accounts.get(account)? else {
            return Ok(LoadedAccountState::NotFound {
                timings: GenTimings {
                    gen_lt: 0,
                    gen_utime: self.gen_utime,
                },
            });
        };

        Ok(LoadedAccountState::Found {
            state,
            mc_ref_handle: self.mc_ref_hanlde.clone(),
            gen_utime: self.gen_utime,
        })
    }
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;
