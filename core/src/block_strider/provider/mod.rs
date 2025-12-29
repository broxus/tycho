use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::{ArcSwapAny, ArcSwapOption};
use futures_util::future::{self, BoxFuture};
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{
    BlockIdRelation, BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug,
    check_with_master_state, check_with_prev_key_block_proof,
};
use tycho_block_util::queue::QueueDiffStuffAug;
use tycho_block_util::state::ShardStateStuff;
use tycho_types::models::BlockId;
use tycho_util::metrics::HistogramGuard;
use tycho_util::serde_helpers;

pub use self::archive_provider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, ArchiveClient, ArchiveDownloadContext,
    ArchiveResponse, FoundArchive, HybridArchiveClient, IntoArchiveClient,
};
pub use self::blockchain_provider::{BlockchainBlockProvider, BlockchainBlockProviderConfig};
pub use self::box_provider::BoxBlockProvider;
use self::futures::SelectNonEmptyFut;
pub use self::storage_provider::StorageBlockProvider;
use crate::global_config::ZerostateId;
use crate::storage::{CoreStorage, MaybeExistingHandle, NewBlockMeta};

mod archive_provider;
mod blockchain_provider;
mod box_provider;
mod futures;
mod storage_provider;

pub type OptionalBlockStuff = Option<Result<BlockStuffAug>>;

/// Block provider *MUST* validate the block before returning it.
pub trait BlockProvider: Send + Sync + 'static {
    type GetNextBlockFut<'a>: Future<Output = OptionalBlockStuff> + Send + 'a;
    type GetBlockFut<'a>: Future<Output = OptionalBlockStuff> + Send + 'a;
    type CleanupFut<'a>: Future<Output = Result<()>> + Send + 'a;

    /// Wait for the next block. Mostly used for masterchain blocks.
    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a>;
    /// Get the exact block. Provider must return the requested block.
    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a>;
    /// Clear resources until (and including) the specified masterchain block seqno.
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_>;
}

impl<T: BlockProvider> BlockProvider for Box<T> {
    type GetNextBlockFut<'a> = T::GetNextBlockFut<'a>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;
    type CleanupFut<'a> = T::CleanupFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        <T as BlockProvider>::get_block(self, block_id_relation)
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        <T as BlockProvider>::cleanup_until(self, mc_seqno)
    }
}

impl<T: BlockProvider> BlockProvider for Arc<T> {
    type GetNextBlockFut<'a> = T::GetNextBlockFut<'a>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;
    type CleanupFut<'a> = T::CleanupFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        <T as BlockProvider>::get_block(self, block_id_relation)
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        <T as BlockProvider>::cleanup_until(self, mc_seqno)
    }
}

pub trait BlockProviderExt: Sized {
    fn boxed(self) -> BoxBlockProvider;

    fn chain<T: BlockProvider>(self, other: T) -> ChainBlockProvider<Self, T>;

    fn cycle<T: BlockProvider>(self, other: T) -> CycleBlockProvider<Self, T>;

    fn retry(self, config: RetryConfig) -> RetryBlockProvider<Self>;
}

impl<B: BlockProvider> BlockProviderExt for B {
    fn boxed(self) -> BoxBlockProvider {
        castaway::match_type!(self, {
            BoxBlockProvider as provider => provider,
            provider => BoxBlockProvider::new(provider),
        })
    }

    fn chain<T: BlockProvider>(self, other: T) -> ChainBlockProvider<Self, T> {
        ChainBlockProvider::new(self, other)
    }

    fn cycle<T: BlockProvider>(self, other: T) -> CycleBlockProvider<Self, T> {
        CycleBlockProvider::new(self, other)
    }

    fn retry(self, config: RetryConfig) -> RetryBlockProvider<Self> {
        RetryBlockProvider {
            inner: self,
            config,
        }
    }
}

// === Provider combinators ===
#[derive(Debug, Clone, Copy)]
pub struct EmptyBlockProvider;

impl BlockProvider for EmptyBlockProvider {
    type GetNextBlockFut<'a> = future::Ready<OptionalBlockStuff>;
    type GetBlockFut<'a> = future::Ready<OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, _prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        future::ready(None)
    }

    fn get_block<'a>(&'a self, _block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        future::ready(None)
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        future::ready(Ok(()))
    }
}

pub struct ChainBlockProvider<T1, T2> {
    left: ArcSwapOption<T1>,
    right: T2,
    cleanup_left_at: AtomicU32,
}

impl<T1, T2> ChainBlockProvider<T1, T2> {
    pub fn new(left: T1, right: T2) -> Self {
        Self {
            left: ArcSwapAny::new(Some(Arc::new(left))),
            right,
            cleanup_left_at: AtomicU32::new(u32::MAX),
        }
    }
}

impl<T1: BlockProvider, T2: BlockProvider> BlockProvider for ChainBlockProvider<T1, T2> {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = BoxFuture<'a, Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        if self.cleanup_left_at.load(Ordering::Acquire) == u32::MAX
            && let Some(left) = self.left.load_full()
        {
            return Box::pin(async move {
                let res = left.get_next_block(prev_block_id).await;
                if res.is_some() {
                    return res;
                }

                // Schedule left provider cleanup for the next block.
                self.cleanup_left_at
                    .store(prev_block_id.seqno.saturating_add(1), Ordering::Release);

                // Fallback to right
                self.right.get_next_block(prev_block_id).await
            });
        }

        Box::pin(self.right.get_next_block(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        if self.cleanup_left_at.load(Ordering::Acquire) == u32::MAX
            && let Some(left) = self.left.load_full()
        {
            return Box::pin(async move { left.get_block(block_id_relation).await });
        }

        Box::pin(self.right.get_block(block_id_relation))
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        Box::pin(async move {
            // Read the cleanup flag:
            // - 0 means that `left` has been reset;
            // - u32::MAX means that `left` is still in use;
            // - other seqno indicates when we need to cleanup `left`.
            let cleanup_left_at = self.cleanup_left_at.load(Ordering::Acquire);

            if cleanup_left_at > 0 && cleanup_left_at <= mc_seqno {
                // Cleanup and reset `left` if target block has been set.
                if let Some(left) = self.left.load_full() {
                    left.cleanup_until(mc_seqno).await?;
                    self.left.store(None);
                    self.cleanup_left_at.store(0, Ordering::Release);
                }
            } else if cleanup_left_at == u32::MAX {
                // Cleanup only `left` until we switch to `right`.
                if let Some(left) = self.left.load_full() {
                    return left.cleanup_until(mc_seqno).await;
                }
            }

            // In all other cases just cleanup `right`.
            self.right.cleanup_until(mc_seqno).await
        })
    }
}

pub struct CycleBlockProvider<T1, T2> {
    left: T1,
    right: T2,
    state: Mutex<CycleBlockProviderState>,
}

struct CycleBlockProviderState {
    // Current used provider.
    current: CycleBlockProviderPart,
    // Next planned switch.
    switch_at: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CycleBlockProviderPart {
    Left,
    Right,
}

impl std::ops::Not for CycleBlockProviderPart {
    type Output = Self;

    #[inline]
    fn not(self) -> Self::Output {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

impl<T1, T2> CycleBlockProvider<T1, T2> {
    pub fn new(left: T1, right: T2) -> Self {
        Self {
            left,
            right,
            state: Mutex::new(CycleBlockProviderState {
                current: CycleBlockProviderPart::Left,
                switch_at: u32::MAX,
            }),
        }
    }

    /// Determine which provider to use based on the current state and scheduled switch.
    ///
    /// This method implements the next logic:
    /// - If a switch is scheduled (`switch_at` != `u32::MAX`) and the seqno has been reached,
    ///   returns the NEW provider even though `is_right` hasn't been updated yet
    /// - Otherwise returns the current provider
    ///
    /// This ensures that:
    /// - Shard blocks are fetched from the same provider as their master block
    /// - Parallel processing of master blocks use the correct provider during the switching transition
    fn choose_provider(&self, mc_seqno: u32) -> CycleBlockProviderPart {
        let state = self.state.lock().unwrap();
        if state.switch_at != u32::MAX && state.switch_at <= mc_seqno {
            // Switch in advance but without changing the state.
            !state.current
        } else {
            state.current
        }
    }

    fn toggle_switch_at(&self, mc_seqno: u32) -> CycleBlockProviderPart {
        let mut state = self.state.lock().unwrap();
        if state.switch_at == u32::MAX {
            state.switch_at = mc_seqno;
            !state.current
        } else {
            state.switch_at = u32::MAX;
            state.current
        }
    }

    fn try_apply_switch(&self, mc_seqno: u32) {
        let mut state = self.state.lock().unwrap();
        if state.switch_at != u32::MAX && state.switch_at <= mc_seqno {
            state.current = !state.current;
            state.switch_at = u32::MAX;
        }
    }
}

impl<T1: BlockProvider, T2: BlockProvider> BlockProvider for CycleBlockProvider<T1, T2> {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = BoxFuture<'a, Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async move {
            // Try using the current provider.
            let mut res = match self.choose_provider(prev_block_id.seqno) {
                CycleBlockProviderPart::Left => self.left.get_next_block(prev_block_id).await,
                CycleBlockProviderPart::Right => self.right.get_next_block(prev_block_id).await,
            };
            if res.is_some() {
                return res;
            }

            loop {
                // Fallback to the next provider.
                res = match self.toggle_switch_at(prev_block_id.seqno.saturating_add(1)) {
                    CycleBlockProviderPart::Left => self.left.get_next_block(prev_block_id).await,
                    CycleBlockProviderPart::Right => self.right.get_next_block(prev_block_id).await,
                };
                if res.is_some() {
                    return res;
                }

                // Allow executor to do some work if all these methods are blocking.
                // FIXME: Add some sleep here in case of complete blocking?
                tokio::task::yield_now().await;
            }
        })
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        match self.choose_provider(block_id_relation.mc_block_id.seqno) {
            CycleBlockProviderPart::Left => Box::pin(self.left.get_block(block_id_relation)),
            CycleBlockProviderPart::Right => Box::pin(self.right.get_block(block_id_relation)),
        }
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        Box::pin(async move {
            self.try_apply_switch(mc_seqno);

            let cleanup_left = self.left.cleanup_until(mc_seqno);
            let cleanup_right = self.right.cleanup_until(mc_seqno);
            match futures_util::future::join(cleanup_left, cleanup_right).await {
                (Err(e), _) | (_, Err(e)) => Err(e),
                (Ok(()), Ok(())) => Ok(()),
            }
        })
    }
}

pub struct RetryBlockProvider<T> {
    inner: T,
    config: RetryConfig,
}

impl<T: BlockProvider> BlockProvider for RetryBlockProvider<T> {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;
    type CleanupFut<'a> = T::CleanupFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async move {
            let mut attempts = 0usize;

            loop {
                let res = self.inner.get_next_block(prev_block_id).await;
                if res.is_some() || attempts >= self.config.attempts {
                    break res;
                }

                attempts += 1;

                // TODO: Backoff?
                tokio::time::sleep(self.config.interval).await;
            }
        })
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        self.inner.get_block(block_id_relation)
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        self.inner.cleanup_until(mc_seqno)
    }
}

macro_rules! impl_provider_tuple {
    ($join_fn:path, |$e:ident| $err_pat:pat$(,)?, {
        $($n:tt: $var:ident = $ty:ident),*$(,)?
    }) => {
        impl<$($ty),*> BlockProvider for ($($ty),*)
        where
            $($ty: BlockProvider),*
        {
            type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
            type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
            type CleanupFut<'a> = BoxFuture<'a, Result<()>>;

            fn get_next_block<'a>(
                &'a self,
                prev_block_id: &'a BlockId,
            ) -> Self::GetNextBlockFut<'a> {
                $(let $var = self.$n.get_next_block(prev_block_id));*;

                Box::pin(async move {
                    $(let $var = pin!($var));*;
                    SelectNonEmptyFut::from(($($var),*)).await
                })
            }

            fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
                $(let $var = self.$n.get_block(block_id_relation));*;

                Box::pin(async move {
                    $(let $var = pin!($var));*;
                    SelectNonEmptyFut::from(($($var),*)).await
                })
            }

            fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
                $(let $var = self.$n.cleanup_until(mc_seqno));*;

                Box::pin(async move {
                    match $join_fn($($var),*).await {
                        $err_pat => Err($e),
                        _ => Ok(())
                    }
                })
            }
        }
    };
}

impl_provider_tuple! {
    futures_util::future::join,
    |e| (Err(e), _) | (_, Err(e)),
    {
        0: a = T0,
        1: b = T1,
    }
}
impl_provider_tuple! {
    futures_util::future::join3,
    |e| (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)),
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
    }
}
impl_provider_tuple! {
    futures_util::future::join4,
    |e| (Err(e), _, _, _) | (_, Err(e), _, _) | (_, _, Err(e), _) | (_, _, _, Err(e)),
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
        3: d = T3,
    }
}
impl_provider_tuple! {
    futures_util::future::join5,
    |e|
        (Err(e), _, _, _, _)
        | (_, Err(e), _, _, _)
        | (_, _, Err(e), _, _)
        | (_, _, _, Err(e), _)
        | (_, _, _, _, Err(e)),
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
        3: d = T3,
        4: e = T4,
    }
}

pub struct CheckProof<'a> {
    pub mc_block_id: &'a BlockId,
    pub block: &'a BlockStuff,
    pub proof: &'a BlockProofStuffAug,
    pub queue_diff: &'a QueueDiffStuffAug,

    /// Whether to store `proof` and `queue_diff` if they are valid.
    pub store_on_success: bool,
}

// TODO: Rename to something better since it checks proofs queue diffs now,
//       and I don't want to parse block info twice to check queue diff separately.
pub struct ProofChecker {
    storage: CoreStorage,
    cached_zerostate: ArcSwapAny<Option<ShardStateStuff>>,
    cached_prev_key_block_proof: ArcSwapAny<Option<BlockProofStuff>>,
    zerostate_id: ZerostateId,
}

impl ProofChecker {
    pub fn new(zerostate_id: ZerostateId, storage: CoreStorage) -> Self {
        Self {
            storage,
            cached_zerostate: Default::default(),
            cached_prev_key_block_proof: Default::default(),
            zerostate_id,
        }
    }

    pub async fn check_proof(&self, ctx: CheckProof<'_>) -> Result<NewBlockMeta> {
        // TODO: Add labels with shard?
        let _histogram = HistogramGuard::begin("tycho_core_check_block_proof_time");

        let CheckProof {
            mc_block_id,
            block,
            proof,
            queue_diff,
            store_on_success,
        } = ctx;

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
            ref_by_mc_seqno: mc_block_id.seqno,
        };

        let block_storage = self.storage.block_storage();

        anyhow::ensure!(
            &virt_block.out_msg_queue_updates.diff_hash == queue_diff.diff_hash(),
            "queue diff mismatch (expected: {}, got: {})",
            virt_block.out_msg_queue_updates.diff_hash,
            queue_diff.diff_hash(),
        );

        if is_masterchain {
            let block_handles = self.storage.block_handle_storage();
            let handle = block_handles
                .load_key_block_handle(virt_block_info.prev_key_block_seqno)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "failed to load prev key block handle by prev_key_block_seqno {}",
                        virt_block_info.prev_key_block_seqno
                    )
                })?;

            if handle.id().seqno == self.zerostate_id.seqno {
                let zerostate = 'zerostate: {
                    if let Some(zerostate) = self.cached_zerostate.load_full() {
                        break 'zerostate zerostate;
                    }

                    let shard_states = self.storage.shard_state_storage();
                    let zerostate = shard_states
                        .load_state(0, handle.id())
                        .await
                        .context("failed to load mc zerostate to check proof")?;

                    self.cached_zerostate.store(Some(zerostate.clone()));

                    zerostate
                };

                check_with_master_state(proof, &zerostate, &virt_block, &virt_block_info)?;
            } else {
                let prev_key_block_proof = 'prev_proof: {
                    if let Some(prev_proof) = self.cached_prev_key_block_proof.load_full()
                        && &prev_proof.as_ref().proof_for == handle.id()
                    {
                        break 'prev_proof prev_proof;
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

        if store_on_success {
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

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    /// Retry limit.
    ///
    /// Default: 1.
    pub attempts: usize,

    /// Polling interval.
    ///
    /// Default: 1 second.
    #[serde(with = "serde_helpers::humantime")]
    pub interval: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: 1,
            interval: Duration::from_secs(1),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tycho_block_util::block::{BlockIdExt, BlockStuff};
    use tycho_types::boc::Boc;
    use tycho_types::models::Block;

    use super::*;

    struct MockBlockProvider {
        // let's give it some state, pretending it's useful
        has_block: AtomicBool,
    }

    impl BlockProvider for MockBlockProvider {
        type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
        type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
        type CleanupFut<'a> = future::Ready<Result<()>>;

        fn get_next_block(&self, _prev_block_id: &BlockId) -> Self::GetNextBlockFut<'_> {
            Box::pin(async {
                if self.has_block.load(Ordering::Acquire) {
                    Some(Ok(get_empty_block()))
                } else {
                    None
                }
            })
        }

        fn get_block(&self, _block_id: &BlockIdRelation) -> Self::GetBlockFut<'_> {
            Box::pin(async {
                if self.has_block.load(Ordering::Acquire) {
                    Some(Ok(get_empty_block()))
                } else {
                    None
                }
            })
        }

        fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
            future::ready(Ok(()))
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

        let chain_provider = ChainBlockProvider::new(left_provider.clone(), right_provider.clone());

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

        assert!(
            chain_provider
                .get_next_block(&get_default_block_id())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn cycle_block_provider_switches_providers_correctly() {
        const LEFT_LIMIT: usize = 10;
        const RIGHT_LIMIT: usize = 1;

        const POLLING_INTERVAL_MS: u64 = 100;

        let left_provider = Arc::new(MockBlockProvider {
            has_block: AtomicBool::new(true),
        });
        let right_provider = Arc::new(MockBlockProvider {
            has_block: AtomicBool::new(false),
        });

        let left_config = RetryConfig {
            attempts: LEFT_LIMIT,
            interval: Duration::from_millis(POLLING_INTERVAL_MS),
        };

        let right_config = RetryConfig {
            attempts: RIGHT_LIMIT,
            interval: Duration::from_millis(POLLING_INTERVAL_MS),
        };

        let left = left_provider.clone().retry(left_config);
        let right = right_provider.clone().retry(right_config);
        let cycle_provider = left.cycle(right);

        assert_eq!(
            cycle_provider.state.lock().unwrap().current,
            CycleBlockProviderPart::Left
        );

        cycle_provider
            .get_next_block(&get_default_block_id())
            .await
            .unwrap()
            .unwrap();

        // Now let's pretend the left provider ran out of blocks.
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(true, Ordering::Release);

        while cycle_provider
            .get_next_block(&get_default_block_id())
            .await
            .is_none()
        {}

        cycle_provider
            .get_next_block(&get_default_block_id())
            .await
            .unwrap()
            .unwrap();

        cycle_provider
            .cleanup_until(get_default_block_id().seqno + 1)
            .await
            .unwrap();

        assert_eq!(
            cycle_provider.state.lock().unwrap().current,
            CycleBlockProviderPart::Right
        );

        // Cycle switch
        left_provider.has_block.store(true, Ordering::Release);
        right_provider.has_block.store(false, Ordering::Release);

        while cycle_provider
            .get_next_block(&get_default_block_id())
            .await
            .is_none()
        {}

        cycle_provider
            .get_next_block(&get_default_block_id())
            .await
            .unwrap()
            .unwrap();

        cycle_provider
            .cleanup_until(get_default_block_id().seqno + 1)
            .await
            .unwrap();

        assert_eq!(
            cycle_provider.state.lock().unwrap().current,
            CycleBlockProviderPart::Left
        );

        cycle_provider
            .get_block(&get_default_block_id().relative_to_self())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            cycle_provider.state.lock().unwrap().current,
            CycleBlockProviderPart::Left
        );

        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(true, Ordering::Release);

        let block = cycle_provider
            .get_block(&get_default_block_id().relative_to_self())
            .await;
        assert!(block.is_none());
    }

    fn get_empty_block() -> BlockStuffAug {
        let block_data = include_bytes!("../../../tests/data/empty_block.bin");
        let root = Boc::decode(block_data).unwrap();
        let block = root.parse::<Block>().unwrap();

        let block_id = BlockId {
            root_hash: *root.repr_hash(),
            ..Default::default()
        };

        BlockStuff::from_block_and_root(&block_id, block, root, block_data.len())
            .with_archive_data(block_data.as_slice())
    }

    fn get_default_block_id() -> BlockId {
        BlockId::default()
    }
}
