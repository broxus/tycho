use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use anyhow::Result;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Either};
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::{BlockIdRelation, BlockProofStuff, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_types::models::*;
use tycho_util::serde_helpers;
use tycho_util::sync::rayon_run;
use crate::block_strider::BlockProvider;
use crate::block_strider::provider::{
    BoxBlockProvider, CheckProof, OptionalBlockStuff, ProofChecker,
};
use crate::blockchain_rpc::{BlockDataFull, BlockchainRpcClient, DataRequirement};
use crate::overlay_client::{Neighbour, PunishReason};
use crate::storage::CoreStorage;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct BlockchainBlockProviderConfig {
    /// Polling interval for `get_next_block` method.
    ///
    /// Default: 1 second.
    #[serde(with = "serde_helpers::humantime")]
    pub get_next_block_polling_interval: Duration,
    /// Polling interval for `get_block` method.
    ///
    /// Default: 1 second.
    #[serde(with = "serde_helpers::humantime")]
    pub get_block_polling_interval: Duration,
    /// Timeout of `get_next_block` for the primary logic (get full block request).
    /// Ignored if no fallback.
    ///
    /// Default: 120 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub get_next_block_timeout: Duration,
    /// Timeout of `get_block` for the primary logic (get full block request).
    /// Ignored if no fallback.
    ///
    /// Default: 60 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub get_block_timeout: Duration,
}
impl Default for BlockchainBlockProviderConfig {
    fn default() -> Self {
        Self {
            get_next_block_polling_interval: Duration::from_secs(1),
            get_block_polling_interval: Duration::from_secs(1),
            get_next_block_timeout: Duration::from_secs(120),
            get_block_timeout: Duration::from_secs(60),
        }
    }
}
pub struct BlockchainBlockProvider {
    client: BlockchainRpcClient,
    config: BlockchainBlockProviderConfig,
    proof_checker: ProofChecker,
    fallback: Option<BoxBlockProvider>,
    use_fallback: AtomicBool,
    cleanup_fallback_at: AtomicU32,
}
impl BlockchainBlockProvider {
    pub fn new(
        client: BlockchainRpcClient,
        storage: CoreStorage,
        config: BlockchainBlockProviderConfig,
    ) -> Self {
        let proof_checker = ProofChecker::new(storage);
        Self {
            client,
            config,
            proof_checker,
            fallback: None,
            use_fallback: AtomicBool::new(false),
            cleanup_fallback_at: AtomicU32::new(u32::MAX),
        }
    }
    pub fn with_fallback<P: BlockProvider>(mut self, fallback: P) -> Self {
        self.fallback = Some(BoxBlockProvider::new(fallback));
        self
    }
    async fn get_next_block_impl(&self, prev_block_id: &BlockId) -> OptionalBlockStuff {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_next_block_impl)),
            file!(),
            103u32,
        );
        let prev_block_id = prev_block_id;
        fn is_next_for(block_id: &BlockId, prev_block_id: &BlockId) -> bool {
            block_id.shard == prev_block_id.shard
                && block_id.seqno == prev_block_id.seqno + 1
        }
        let primary = || {
            loop_with_timeout(
                self.config.get_next_block_polling_interval,
                self.config.get_next_block_timeout,
                self.fallback.is_some(),
                || {
                    tracing::debug!(% prev_block_id, "get_next_block_full requested");
                    self.client
                        .get_next_block_full(prev_block_id, DataRequirement::Optional)
                },
                |res| async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        118u32,
                    );
                    match res {
                        Ok(res) => {
                            match res.data {
                                Some(
                                    data,
                                ) if !is_next_for(&data.block_id, prev_block_id) => {
                                    res.neighbour.punish(PunishReason::Malicious);
                                    tracing::warn!("got response for an unknown block id");
                                }
                                Some(data) => {
                                    let mc_block_id = data.block_id;
                                    let parsed = {
                                        __guard.end_section(129u32);
                                        let __result = self
                                            .process_received_block(&mc_block_id, data, res.neighbour)
                                            .await;
                                        __guard.start_section(129u32);
                                        __result
                                    };
                                    if parsed.is_some() {
                                        {
                                            __guard.end_section(131u32);
                                            return parsed;
                                        };
                                    }
                                }
                                None => tracing::warn!(? prev_block_id, "block not found"),
                            }
                        }
                        Err(e) => tracing::error!("failed to get next block: {e:?}"),
                    }
                    None
                },
            )
        };
        loop {
            __guard.checkpoint(143u32);
            if !self.use_fallback.load(Ordering::Relaxed)
                && let res @ Some(_) = {
                    __guard.end_section(146u32);
                    let __result = primary().await;
                    __guard.start_section(146u32);
                    __result
                }
            {
                {
                    __guard.end_section(148u32);
                    return res;
                };
            }
            if let Some(fallback) = &self.fallback {
                tracing::debug!(% prev_block_id, "get_next_block_full fallback");
                self.use_fallback.store(true, Ordering::Relaxed);
                if let res @ Some(_) = {
                    __guard.end_section(155u32);
                    let __result = fallback.get_next_block(prev_block_id).await;
                    __guard.start_section(155u32);
                    __result
                } {
                    {
                        __guard.end_section(156u32);
                        return res;
                    };
                }
            }
            self.use_fallback.store(false, Ordering::Relaxed);
            self.cleanup_fallback_at
                .store(prev_block_id.seqno.saturating_add(1), Ordering::Release);
        }
    }
    async fn get_block_impl(
        &self,
        block_id_relation: &BlockIdRelation,
    ) -> OptionalBlockStuff {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_block_impl)),
            file!(),
            169u32,
        );
        let block_id_relation = block_id_relation;
        let BlockIdRelation { mc_block_id, block_id } = block_id_relation;
        let primary = || {
            loop_with_timeout(
                self.config.get_block_polling_interval,
                self.config.get_block_timeout,
                self.fallback.is_some(),
                || {
                    tracing::debug!(% block_id, "get_block_full requested");
                    self.client.get_block_full(block_id, DataRequirement::Optional)
                },
                |res| async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        188u32,
                    );
                    match res {
                        Ok(res) => {
                            match res.data {
                                Some(data) => {
                                    let parsed = {
                                        __guard.end_section(194u32);
                                        let __result = self
                                            .process_received_block(mc_block_id, data, res.neighbour)
                                            .await;
                                        __guard.start_section(194u32);
                                        __result
                                    };
                                    if parsed.is_some() {
                                        {
                                            __guard.end_section(196u32);
                                            return parsed;
                                        };
                                    }
                                }
                                None => tracing::warn!(% block_id, "block not found"),
                            }
                        }
                        Err(e) => tracing::error!("failed to get block: {e:?}"),
                    }
                    None
                },
            )
        };
        loop {
            __guard.checkpoint(208u32);
            if !self.use_fallback.load(Ordering::Relaxed)
                && let res @ Some(_) = {
                    __guard.end_section(211u32);
                    let __result = primary().await;
                    __guard.start_section(211u32);
                    __result
                }
            {
                {
                    __guard.end_section(213u32);
                    return res;
                };
            }
            if let Some(fallback) = &self.fallback {
                tracing::debug!(% block_id, "get_block_full fallback");
                self.use_fallback.store(true, Ordering::Relaxed);
                if let res @ Some(_) = {
                    __guard.end_section(220u32);
                    let __result = fallback.get_block(block_id_relation).await;
                    __guard.start_section(220u32);
                    __result
                } {
                    {
                        __guard.end_section(221u32);
                        return res;
                    };
                }
            }
            self.use_fallback.store(false, Ordering::Relaxed);
        }
    }
    #[tracing::instrument(
        skip(self, mc_block_id, block_full, neighbour),
        fields(mc_block_id = %mc_block_id.as_short_id())
    )]
    async fn process_received_block(
        &self,
        mc_block_id: &BlockId,
        block_full: BlockDataFull,
        neighbour: Neighbour,
    ) -> OptionalBlockStuff {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(process_received_block)),
            file!(),
            241u32,
        );
        let mc_block_id = mc_block_id;
        let block_full = block_full;
        let neighbour = neighbour;
        let block_stuff_fut = pin!(
            rayon_run({ let block_id = block_full.block_id; let block_data = block_full
            .block_data.clone(); move || BlockStuff::deserialize_checked(& block_id, &
            block_data) })
        );
        let other_data_fut = pin!(
            rayon_run({ let block_id = block_full.block_id; let proof_data = block_full
            .proof_data.clone(); let queue_diff_data = block_full.queue_diff_data
            .clone(); move || { (BlockProofStuff::deserialize(& block_id, & proof_data),
            QueueDiffStuff::deserialize(& block_id, & queue_diff_data),) } })
        );
        let (block_stuff, (block_proof, queue_diff)) = {
            __guard.end_section(261u32);
            let __result = futures_util::future::join(block_stuff_fut, other_data_fut)
                .await;
            __guard.start_section(261u32);
            __result
        };
        match (block_stuff, block_proof, queue_diff) {
            (Ok(block), Ok(proof), Ok(diff)) => {
                let proof = WithArchiveData::new(proof, block_full.proof_data);
                let diff = WithArchiveData::new(diff, block_full.queue_diff_data);
                if let Err(e) = {
                    __guard.end_section(276u32);
                    let __result = self
                        .proof_checker
                        .check_proof(CheckProof {
                            mc_block_id,
                            block: &block,
                            proof: &proof,
                            queue_diff: &diff,
                            store_on_success: true,
                        })
                        .await;
                    __guard.start_section(276u32);
                    __result
                } {
                    neighbour.punish(PunishReason::Malicious);
                    tracing::error!("got invalid block proof: {e:?}");
                    {
                        __guard.end_section(280u32);
                        return None;
                    };
                }
                Some(Ok(block.with_archive_data(block_full.block_data)))
            }
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                neighbour.punish(PunishReason::Malicious);
                tracing::error!(
                    "failed to deserialize shard block or block proof: {e:?}"
                );
                None
            }
        }
    }
}
impl BlockProvider for BlockchainBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = BlockchainBlockProviderCleanupFut<'a>;
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id_relation))
    }
    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        match &self.fallback {
            Some(
                fallback,
            ) if self.cleanup_fallback_at.load(Ordering::Acquire) <= mc_seqno => {
                BlockchainBlockProviderCleanupFut::Fallback {
                    fut: fallback.cleanup_until(mc_seqno),
                    cleanup_fallback_at: &self.cleanup_fallback_at,
                    mc_seqno,
                }
            }
            _ => BlockchainBlockProviderCleanupFut::Noop,
        }
    }
}
pub enum BlockchainBlockProviderCleanupFut<'a> {
    Noop,
    Fallback {
        fut: BoxFuture<'a, Result<()>>,
        cleanup_fallback_at: &'a AtomicU32,
        mc_seqno: u32,
    },
}
impl Future for BlockchainBlockProviderCleanupFut<'_> {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Noop => Poll::Ready(Ok(())),
            Self::Fallback { fut, cleanup_fallback_at, mc_seqno } => {
                let res = fut.poll_unpin(cx);
                if matches!(& res, Poll::Ready(r) if r.is_ok()) {
                    cleanup_fallback_at
                        .compare_exchange(
                            *mc_seqno,
                            u32::MAX,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .ok();
                }
                res
            }
        }
    }
}
async fn loop_with_timeout<E, EFut, P, PFut, R, T>(
    interval: Duration,
    timeout: Duration,
    use_timeout: bool,
    request: E,
    process: P,
) -> Option<T>
where
    E: Fn() -> EFut,
    EFut: Future<Output = R>,
    P: Fn(R) -> PFut,
    PFut: Future<Output = Option<T>>,
{
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(loop_with_timeout)),
        file!(),
        368u32,
    );
    let interval = interval;
    let timeout = timeout;
    let use_timeout = use_timeout;
    let request = request;
    let process = process;
    let mut interval = tokio::time::interval(interval);
    let mut timeout = pin!(
        if use_timeout { Either::Left(tokio::time::sleep(timeout)) } else {
        Either::Right(futures_util::future::pending::< () > ()) }
    );
    loop {
        __guard.checkpoint(378u32);
        {
            __guard.end_section(379u32);
            let __result = tokio::select! {
                res = request() => { if let res @ Some(_) = process(res). await { return
                res; } }, _ = & mut timeout => return None,
            };
            __guard.start_section(379u32);
            __result
        }
        {
            __guard.end_section(388u32);
            let __result = tokio::select! {
                _ = interval.tick() => {}, _ = & mut timeout => return None,
            };
            __guard.start_section(388u32);
            __result
        }
    }
}
