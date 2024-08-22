use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwapAny;
use everscale_types::models::BlockId;
use futures_util::future::{self, BoxFuture};
use tycho_block_util::block::{
    check_with_master_state, check_with_prev_key_block_proof, BlockProofStuff, BlockProofStuffAug,
    BlockStuff, BlockStuffAug,
};
use tycho_block_util::queue::QueueDiffStuffAug;
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::{MaybeExistingHandle, NewBlockMeta, Storage};
use tycho_util::metrics::HistogramGuard;

pub use self::archive_provider::{ArchiveBlockProvider, ArchiveBlockProviderConfig};
pub use self::blockchain_provider::{BlockchainBlockProvider, BlockchainBlockProviderConfig};
pub use self::storage_provider::StorageBlockProvider;

mod archive_provider;
mod blockchain_provider;
mod storage_provider;

pub type OptionalBlockStuff = Option<Result<BlockStuffAug>>;

/// Block provider *MUST* validate the block before returning it.
pub trait BlockProvider: Send + Sync + 'static {
    type GetNextBlockFut<'a>: Future<Output = OptionalBlockStuff> + Send + 'a;
    type GetBlockFut<'a>: Future<Output = OptionalBlockStuff> + Send + 'a;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a>;
    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a>;
}

impl<T: BlockProvider> BlockProvider for Box<T> {
    type GetNextBlockFut<'a> = T::GetNextBlockFut<'a>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        <T as BlockProvider>::get_block(self, block_id)
    }
}

impl<T: BlockProvider> BlockProvider for Arc<T> {
    type GetNextBlockFut<'a> = T::GetNextBlockFut<'a>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        <T as BlockProvider>::get_block(self, block_id)
    }
}

pub trait BlockProviderExt: Sized {
    fn chain<T: BlockProvider>(self, other: T) -> ChainBlockProvider<Self, T>;
}

impl<B: BlockProvider> BlockProviderExt for B {
    fn chain<T: BlockProvider>(self, other: T) -> ChainBlockProvider<Self, T> {
        ChainBlockProvider {
            left: self,
            right: other,
            is_right: AtomicBool::new(false),
        }
    }
}

// === Provider combinators ===
#[derive(Debug, Clone, Copy)]
pub struct EmptyBlockProvider;

impl BlockProvider for EmptyBlockProvider {
    type GetNextBlockFut<'a> = future::Ready<OptionalBlockStuff>;
    type GetBlockFut<'a> = future::Ready<OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, _prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        future::ready(None)
    }

    fn get_block<'a>(&'a self, _block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        future::ready(None)
    }
}

pub struct ChainBlockProvider<T1, T2> {
    left: T1,
    right: T2,
    is_right: AtomicBool,
}

impl<T1: BlockProvider, T2: BlockProvider> BlockProvider for ChainBlockProvider<T1, T2> {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async move {
            if !self.is_right.load(Ordering::Acquire) {
                let res = self.left.get_next_block(prev_block_id).await;
                if res.is_some() {
                    return res;
                }
                self.is_right.store(true, Ordering::Release);
            }
            self.right.get_next_block(prev_block_id).await
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'_> {
        Box::pin(async {
            if self.is_right.load(Ordering::Acquire) {
                self.right.get_block(block_id).await
            } else {
                self.left.get_block(block_id).await
            }
        })
    }
}

impl<T1: BlockProvider, T2: BlockProvider> BlockProvider for (T1, T2) {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        let left = self.0.get_next_block(prev_block_id);
        let right = self.1.get_next_block(prev_block_id);

        Box::pin(async move {
            match future::select(pin!(left), pin!(right)).await {
                future::Either::Left((res, right)) => match res {
                    Some(res) => Some(res),
                    None => right.await,
                },
                future::Either::Right((res, left)) => match res {
                    Some(res) => Some(res),
                    None => left.await,
                },
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        let left = self.0.get_block(block_id);
        let right = self.1.get_block(block_id);

        Box::pin(async move {
            match future::select(pin!(left), pin!(right)).await {
                future::Either::Left((res, right)) => match res {
                    Some(res) => Some(res),
                    None => right.await,
                },
                future::Either::Right((res, left)) => match res {
                    Some(res) => Some(res),
                    None => left.await,
                },
            }
        })
    }
}

// TODO: Rename to something better since it checks proofs queue diffs now,
//       and I don't want to parse block info twice to check queue diff separately.
pub struct ProofChecker {
    storage: Storage,
    cached_zerostate: ArcSwapAny<Option<ShardStateStuff>>,
    cached_prev_key_block_proof: ArcSwapAny<Option<BlockProofStuff>>,
}

impl ProofChecker {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            cached_zerostate: Default::default(),
            cached_prev_key_block_proof: Default::default(),
        }
    }

    pub async fn check_proof(
        &self,
        block: &BlockStuff,
        proof: &BlockProofStuffAug,
        queue_diff: &QueueDiffStuffAug,
        store_proof_on_success: bool,
    ) -> Result<NewBlockMeta> {
        // TODO: Add labels with shard?
        let _histogram = HistogramGuard::begin("tycho_core_check_block_proof_time");

        anyhow::ensure!(
            block.id() == &proof.proof().proof_for,
            "proof_for and block id mismatch: proof_for={}, block_id={}",
            proof.proof().proof_for,
            block.id(),
        );

        let is_masterchain = block.id().is_masterchain();
        anyhow::ensure!(is_masterchain ^ proof.is_link(), "unexpected proof type");

        let (virt_block, virt_block_info) = proof.pre_check_block_proof()?;
        let meta = NewBlockMeta {
            is_key_block: virt_block_info.key_block,
            gen_utime: virt_block_info.gen_utime,
            mc_ref_seqno: is_masterchain.then(|| block.id().seqno),
        };

        let block_storage = self.storage.block_storage();

        // TODO: Check `queue_diff` using data from `virt_block` or `virt_block_info`

        if is_masterchain {
            let block_handles = self.storage.block_handle_storage();
            let handle = block_handles
                .load_key_block_handle(virt_block_info.prev_key_block_seqno)
                .context("failed to load prev key block handle")?;

            if handle.id().seqno == 0 {
                let zerostate = 'zerostate: {
                    if let Some(zerostate) = self.cached_zerostate.load_full() {
                        break 'zerostate zerostate;
                    }

                    let shard_states = self.storage.shard_state_storage();
                    let zerostate = shard_states
                        .load_state(handle.id())
                        .await
                        .context("failed to load mc zerostate")?;

                    self.cached_zerostate.store(Some(zerostate.clone()));

                    zerostate
                };

                check_with_master_state(proof, &zerostate, &virt_block, &virt_block_info)?;
            } else {
                let prev_key_block_proof = 'prev_proof: {
                    if let Some(prev_proof) = self.cached_prev_key_block_proof.load_full() {
                        if &prev_proof.as_ref().proof_for == handle.id() {
                            break 'prev_proof prev_proof;
                        }
                    }

                    let prev_key_block_proof = block_storage
                        .load_block_proof(&handle)
                        .await
                        .context("failed to load prev key block proof")?;

                    // NOTE: Assume that there is only one masterchain block using this cache.
                    // Otherwise, it will be overwritten every time. Maybe use `rcu`.
                    self.cached_prev_key_block_proof
                        .store(Some(prev_key_block_proof.clone()));

                    prev_key_block_proof
                };

                check_with_prev_key_block_proof(
                    proof,
                    &prev_key_block_proof,
                    &virt_block,
                    &virt_block_info,
                )?;
            }
        }

        if store_proof_on_success {
            // Store proof
            let res = block_storage
                .store_block_proof(proof, MaybeExistingHandle::New(meta))
                .await?;

            // Store queue diff
            block_storage
                .store_queue_diff(queue_diff, res.handle.into())
                .await?;
        }

        Ok(meta)
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use everscale_types::boc::Boc;
    use everscale_types::models::Block;
    use tycho_block_util::block::BlockStuff;

    use super::*;

    struct MockBlockProvider {
        // let's give it some state, pretending it's useful
        has_block: AtomicBool,
    }

    impl BlockProvider for MockBlockProvider {
        type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
        type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

        fn get_next_block(&self, _prev_block_id: &BlockId) -> Self::GetNextBlockFut<'_> {
            Box::pin(async {
                if self.has_block.load(Ordering::Acquire) {
                    Some(Ok(get_empty_block()))
                } else {
                    None
                }
            })
        }

        fn get_block(&self, _block_id: &BlockId) -> Self::GetBlockFut<'_> {
            Box::pin(async {
                if self.has_block.load(Ordering::Acquire) {
                    Some(Ok(get_empty_block()))
                } else {
                    None
                }
            })
        }
    }

    #[tokio::test]
    async fn chain_block_provider_switches_providers_correctly() {
        let left_provider = Arc::new(MockBlockProvider {
            has_block: AtomicBool::new(true),
        });
        let right_provider = Arc::new(MockBlockProvider {
            has_block: AtomicBool::new(false),
        });

        let chain_provider = ChainBlockProvider {
            left: Arc::clone(&left_provider),
            right: Arc::clone(&right_provider),
            is_right: AtomicBool::new(false),
        };

        chain_provider
            .get_next_block(&get_default_block_id())
            .await
            .unwrap()
            .unwrap();

        // Now let's pretend the left provider ran out of blocks.
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(true, Ordering::Release);

        chain_provider
            .get_next_block(&get_default_block_id())
            .await
            .unwrap()
            .unwrap();

        // End of blocks stream for both providers
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(false, Ordering::Release);

        assert!(chain_provider
            .get_next_block(&get_default_block_id())
            .await
            .is_none());
    }

    fn get_empty_block() -> BlockStuffAug {
        let block_data = include_bytes!("../../../tests/data/empty_block.bin");
        let root = Boc::decode(block_data).unwrap();
        let block = root.parse::<Block>().unwrap();

        let block_id = BlockId {
            root_hash: *root.repr_hash(),
            ..Default::default()
        };

        BlockStuff::from_block_and_root(&block_id, block, root)
            .with_archive_data(block_data.as_slice())
    }

    fn get_default_block_id() -> BlockId {
        BlockId::default()
    }
}
