use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
pub use self::archive_provider::{ArchiveBlockProvider, ArchiveBlockProviderConfig};
pub use self::blockchain_provider::{
    BlockchainBlockProvider, BlockchainBlockProviderConfig,
};
pub use self::box_provider::BoxBlockProvider;
use self::futures::SelectNonEmptyFut;
pub use self::storage_provider::StorageBlockProvider;
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
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a>;
    /// Get the exact block. Provider must return the requested block.
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a>;
    /// Clear resources until (and including) the specified masterchain block seqno.
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_>;
}
impl<T: BlockProvider> BlockProvider for Box<T> {
    type GetNextBlockFut<'a> = T::GetNextBlockFut<'a>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;
    type CleanupFut<'a> = T::CleanupFut<'a>;
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
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
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
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
        castaway::match_type!(
            self, { BoxBlockProvider as provider => provider, provider =>
            BoxBlockProvider::new(provider), }
        )
    }
    fn chain<T: BlockProvider>(self, other: T) -> ChainBlockProvider<Self, T> {
        ChainBlockProvider::new(self, other)
    }
    fn cycle<T: BlockProvider>(self, other: T) -> CycleBlockProvider<Self, T> {
        CycleBlockProvider {
            left: self,
            right: other,
            is_right: AtomicBool::new(false),
        }
    }
    fn retry(self, config: RetryConfig) -> RetryBlockProvider<Self> {
        RetryBlockProvider {
            inner: self,
            config,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct EmptyBlockProvider;
impl BlockProvider for EmptyBlockProvider {
    type GetNextBlockFut<'a> = future::Ready<OptionalBlockStuff>;
    type GetBlockFut<'a> = future::Ready<OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;
    fn get_next_block<'a>(
        &'a self,
        _prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        future::ready(None)
    }
    fn get_block<'a>(
        &'a self,
        _block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
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
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        if self.cleanup_left_at.load(Ordering::Acquire) == u32::MAX
            && let Some(left) = self.left.load_full()
        {
            return Box::pin(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    171u32,
                );
                let res = {
                    __guard.end_section(172u32);
                    let __result = left.get_next_block(prev_block_id).await;
                    __guard.start_section(172u32);
                    __result
                };
                if res.is_some() {
                    {
                        __guard.end_section(174u32);
                        return res;
                    };
                }
                self.cleanup_left_at
                    .store(prev_block_id.seqno.saturating_add(1), Ordering::Release);
                {
                    __guard.end_section(182u32);
                    let __result = self.right.get_next_block(prev_block_id).await;
                    __guard.start_section(182u32);
                    __result
                }
            });
        }
        Box::pin(self.right.get_next_block(prev_block_id))
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
        if self.cleanup_left_at.load(Ordering::Acquire) == u32::MAX
            && let Some(left) = self.left.load_full()
        {
            return Box::pin(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    193u32,
                );
                {
                    __guard.end_section(193u32);
                    let __result = left.get_block(block_id_relation).await;
                    __guard.start_section(193u32);
                    __result
                }
            });
        }
        Box::pin(self.right.get_block(block_id_relation))
    }
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        Box::pin(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                200u32,
            );
            let cleanup_left_at = self.cleanup_left_at.load(Ordering::Acquire);
            if cleanup_left_at > 0 && cleanup_left_at <= mc_seqno {
                if let Some(left) = self.left.load_full() {
                    {
                        __guard.end_section(210u32);
                        let __result = left.cleanup_until(mc_seqno).await;
                        __guard.start_section(210u32);
                        __result
                    }?;
                    self.left.store(None);
                    self.cleanup_left_at.store(0, Ordering::Release);
                }
            } else if cleanup_left_at == u32::MAX {
                if let Some(left) = self.left.load_full() {
                    {
                        __guard.end_section(217u32);
                        return {
                            __guard.end_section(217u32);
                            let __result = left.cleanup_until(mc_seqno).await;
                            __guard.start_section(217u32);
                            __result
                        };
                    };
                }
            }
            {
                __guard.end_section(222u32);
                let __result = self.right.cleanup_until(mc_seqno).await;
                __guard.start_section(222u32);
                __result
            }
        })
    }
}
pub struct CycleBlockProvider<T1, T2> {
    left: T1,
    right: T2,
    is_right: AtomicBool,
}
impl<T1: BlockProvider, T2: BlockProvider> BlockProvider for CycleBlockProvider<T1, T2> {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = BoxFuture<'a, Result<()>>;
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        Box::pin(async {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                239u32,
            );
            let is_right = self.is_right.load(Ordering::Acquire);
            let res = if !is_right {
                {
                    __guard.end_section(243u32);
                    let __result = self.left.get_next_block(prev_block_id).await;
                    __guard.start_section(243u32);
                    __result
                }
            } else {
                {
                    __guard.end_section(245u32);
                    let __result = self.right.get_next_block(prev_block_id).await;
                    __guard.start_section(245u32);
                    __result
                }
            };
            if res.is_some() {
                {
                    __guard.end_section(249u32);
                    return res;
                };
            }
            let is_right = !is_right;
            self.is_right.store(is_right, Ordering::Release);
            if !is_right {
                {
                    __guard.end_section(256u32);
                    let __result = self.left.get_next_block(prev_block_id).await;
                    __guard.start_section(256u32);
                    __result
                }
            } else {
                {
                    __guard.end_section(258u32);
                    let __result = self.right.get_next_block(prev_block_id).await;
                    __guard.start_section(258u32);
                    __result
                }
            }
        })
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
        if self.is_right.load(Ordering::Acquire) {
            Box::pin(self.right.get_block(block_id_relation))
        } else {
            Box::pin(self.left.get_block(block_id_relation))
        }
    }
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        Box::pin(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                272u32,
            );
            let cleanup_left = self.left.cleanup_until(mc_seqno);
            let cleanup_right = self.right.cleanup_until(mc_seqno);
            match {
                __guard.end_section(275u32);
                let __result = futures_util::future::join(cleanup_left, cleanup_right)
                    .await;
                __guard.start_section(275u32);
                __result
            } {
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
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        Box::pin(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                294u32,
            );
            let mut attempts = 0usize;
            loop {
                __guard.checkpoint(297u32);
                let res = {
                    __guard.end_section(298u32);
                    let __result = self.inner.get_next_block(prev_block_id).await;
                    __guard.start_section(298u32);
                    __result
                };
                if res.is_some() || attempts >= self.config.attempts {
                    {
                        __guard.end_section(300u32);
                        __guard.start_section(300u32);
                        break res;
                    };
                }
                attempts += 1;
                {
                    __guard.end_section(306u32);
                    let __result = tokio::time::sleep(self.config.interval).await;
                    __guard.start_section(306u32);
                    __result
                };
            }
        })
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
        self.inner.get_block(block_id_relation)
    }
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        self.inner.cleanup_until(mc_seqno)
    }
}
macro_rules! impl_provider_tuple {
    (
        $join_fn:path, |$e:ident | $err_pat:pat $(,)?, { $($n:tt : $var:ident =
        $ty:ident),*$(,) ? }
    ) => {
        impl <$($ty),*> BlockProvider for ($($ty),*) where $($ty : BlockProvider),* {
        type GetNextBlockFut <'a > = BoxFuture <'a, OptionalBlockStuff >; type
        GetBlockFut <'a > = BoxFuture <'a, OptionalBlockStuff >; type CleanupFut <'a > =
        BoxFuture <'a, Result < () >>; fn get_next_block <'a > (&'a self, prev_block_id :
        &'a BlockId,) -> Self::GetNextBlockFut <'a > { $(let $var = self.$n
        .get_next_block(prev_block_id));*; Box::pin(async move { $(let $var =
        pin!($var));*; SelectNonEmptyFut::from(($($var),*)). await }) } fn get_block <'a
        > (&'a self, block_id_relation : &'a BlockIdRelation) -> Self::GetBlockFut <'a >
        { $(let $var = self.$n .get_block(block_id_relation));*; Box::pin(async move {
        $(let $var = pin!($var));*; SelectNonEmptyFut::from(($($var),*)). await }) } fn
        cleanup_until(& self, mc_seqno : u32) -> Self::CleanupFut <'_ > { $(let $var =
        self.$n .cleanup_until(mc_seqno));*; Box::pin(async move { match $join_fn
        ($($var),*). await { $err_pat => Err($e), _ => Ok(()) } }) } }
    };
}
impl_provider_tuple! {
    futures_util::future::join, | e | (Err(e), _) | (_, Err(e)), { 0 : a = T0, 1 : b =
    T1, }
}
impl_provider_tuple! {
    futures_util::future::join3, | e | (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)),
    { 0 : a = T0, 1 : b = T1, 2 : c = T2, }
}
impl_provider_tuple! {
    futures_util::future::join4, | e | (Err(e), _, _, _) | (_, Err(e), _, _) | (_, _,
    Err(e), _) | (_, _, _, Err(e)), { 0 : a = T0, 1 : b = T1, 2 : c = T2, 3 : d = T3, }
}
impl_provider_tuple! {
    futures_util::future::join5, | e | (Err(e), _, _, _, _) | (_, Err(e), _, _, _) | (_,
    _, Err(e), _, _) | (_, _, _, Err(e), _) | (_, _, _, _, Err(e)), { 0 : a = T0, 1 : b =
    T1, 2 : c = T2, 3 : d = T3, 4 : e = T4, }
}
pub struct CheckProof<'a> {
    pub mc_block_id: &'a BlockId,
    pub block: &'a BlockStuff,
    pub proof: &'a BlockProofStuffAug,
    pub queue_diff: &'a QueueDiffStuffAug,
    /// Whether to store `proof` and `queue_diff` if they are valid.
    pub store_on_success: bool,
}
pub struct ProofChecker {
    storage: CoreStorage,
    cached_zerostate: ArcSwapAny<Option<ShardStateStuff>>,
    cached_prev_key_block_proof: ArcSwapAny<Option<BlockProofStuff>>,
}
impl ProofChecker {
    pub fn new(storage: CoreStorage) -> Self {
        Self {
            storage,
            cached_zerostate: Default::default(),
            cached_prev_key_block_proof: Default::default(),
        }
    }
    pub async fn check_proof(&self, ctx: CheckProof<'_>) -> Result<NewBlockMeta> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(check_proof)),
            file!(),
            438u32,
        );
        let ctx = ctx;
        let _histogram = HistogramGuard::begin("tycho_core_check_block_proof_time");
        let CheckProof { mc_block_id, block, proof, queue_diff, store_on_success } = ctx;
        anyhow::ensure!(
            block.id() == & proof.proof().proof_for,
            "proof_for and block id mismatch: proof_for={}, block_id={}", proof.proof()
            .proof_for, block.id(),
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
            & virt_block.out_msg_queue_updates.diff_hash == queue_diff.diff_hash(),
            "queue diff mismatch (expected: {}, got: {})", virt_block
            .out_msg_queue_updates.diff_hash, queue_diff.diff_hash(),
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
            if handle.id().seqno == 0 {
                let zerostate = 'zerostate: {
                    if let Some(zerostate) = self.cached_zerostate.load_full() {
                        {
                            __guard.end_section(490u32);
                            __guard.start_section(490u32);
                            break 'zerostate zerostate;
                        };
                    }
                    let shard_states = self.storage.shard_state_storage();
                    let zerostate = {
                        __guard.end_section(496u32);
                        let __result = shard_states.load_state(0, handle.id()).await;
                        __guard.start_section(496u32);
                        __result
                    }
                        .context("failed to load mc zerostate")?;
                    self.cached_zerostate.store(Some(zerostate.clone()));
                    zerostate
                };
                check_with_master_state(
                    proof,
                    &zerostate,
                    &virt_block,
                    &virt_block_info,
                )?;
            } else {
                let prev_key_block_proof = 'prev_proof: {
                    if let Some(prev_proof) = self
                        .cached_prev_key_block_proof
                        .load_full() && &prev_proof.as_ref().proof_for == handle.id()
                    {
                        {
                            __guard.end_section(510u32);
                            __guard.start_section(510u32);
                            break 'prev_proof prev_proof;
                        };
                    }
                    let prev_key_block_proof = {
                        __guard.end_section(515u32);
                        let __result = block_storage.load_block_proof(&handle).await;
                        __guard.start_section(515u32);
                        __result
                    }
                        .context("failed to load prev key block proof")?;
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
            let res = {
                __guard.end_section(539u32);
                let __result = block_storage
                    .store_block_proof(proof, MaybeExistingHandle::New(meta))
                    .await;
                __guard.start_section(539u32);
                __result
            }?;
            {
                __guard.end_section(544u32);
                let __result = block_storage
                    .store_queue_diff(queue_diff, res.handle.into())
                    .await;
                __guard.start_section(544u32);
                __result
            }?;
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
    /// Polling interval for downloading archive.
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
        has_block: AtomicBool,
    }
    impl BlockProvider for MockBlockProvider {
        type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
        type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
        type CleanupFut<'a> = future::Ready<Result<()>>;
        fn get_next_block(&self, _prev_block_id: &BlockId) -> Self::GetNextBlockFut<'_> {
            Box::pin(async {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    597u32,
                );
                if self.has_block.load(Ordering::Acquire) {
                    Some(Ok(get_empty_block()))
                } else {
                    None
                }
            })
        }
        fn get_block(&self, _block_id: &BlockIdRelation) -> Self::GetBlockFut<'_> {
            Box::pin(async {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    607u32,
                );
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(chain_block_provider_switches_providers_correctly)
            ),
            file!(),
            622u32,
        );
        let left_provider = Arc::new(MockBlockProvider {
            has_block: AtomicBool::new(true),
        });
        let right_provider = Arc::new(MockBlockProvider {
            has_block: AtomicBool::new(false),
        });
        let chain_provider = ChainBlockProvider::new(
            left_provider.clone(),
            right_provider.clone(),
        );
        {
            __guard.end_section(634u32);
            let __result = chain_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(634u32);
            __result
        }
            .unwrap()
            .unwrap();
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(true, Ordering::Release);
        {
            __guard.end_section(644u32);
            let __result = chain_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(644u32);
            __result
        }
            .unwrap()
            .unwrap();
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(false, Ordering::Release);
        assert!(
            chain_provider.get_next_block(& get_default_block_id()). await .is_none()
        );
    }
    #[tokio::test]
    async fn cycle_block_provider_switches_providers_correctly() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(), "::",
                stringify!(cycle_block_provider_switches_providers_correctly)
            ),
            file!(),
            661u32,
        );
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
        assert!(! cycle_provider.is_right.load(Ordering::Acquire));
        {
            __guard.end_section(692u32);
            let __result = cycle_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(692u32);
            __result
        }
            .unwrap()
            .unwrap();
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(true, Ordering::Release);
        while {
            __guard.end_section(702u32);
            let __result = cycle_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(702u32);
            __result
        }
            .is_none()
        {
            __guard.checkpoint(700u32);
        }
        {
            __guard.end_section(708u32);
            let __result = cycle_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(708u32);
            __result
        }
            .unwrap()
            .unwrap();
        assert!(cycle_provider.is_right.load(Ordering::Acquire));
        left_provider.has_block.store(true, Ordering::Release);
        right_provider.has_block.store(false, Ordering::Release);
        while {
            __guard.end_section(720u32);
            let __result = cycle_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(720u32);
            __result
        }
            .is_none()
        {
            __guard.checkpoint(718u32);
        }
        {
            __guard.end_section(726u32);
            let __result = cycle_provider.get_next_block(&get_default_block_id()).await;
            __guard.start_section(726u32);
            __result
        }
            .unwrap()
            .unwrap();
        assert!(! cycle_provider.is_right.load(Ordering::Acquire));
        {
            __guard.end_section(733u32);
            let __result = cycle_provider
                .get_block(&get_default_block_id().relative_to_self())
                .await;
            __guard.start_section(733u32);
            __result
        }
            .unwrap()
            .unwrap();
        assert!(! cycle_provider.is_right.load(Ordering::Acquire));
        left_provider.has_block.store(false, Ordering::Release);
        right_provider.has_block.store(true, Ordering::Release);
        let block = {
            __guard.end_section(743u32);
            let __result = cycle_provider
                .get_block(&get_default_block_id().relative_to_self())
                .await;
            __guard.start_section(743u32);
            __result
        };
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
