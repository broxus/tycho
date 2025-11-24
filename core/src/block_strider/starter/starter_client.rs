use std::fs::File;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use tycho_types::models::BlockId;

#[cfg(feature = "s3")]
pub(crate) use self::s3::{HybridStarterClient, S3StarterClient};
use crate::blockchain_rpc::{BlockDataFull, BlockchainRpcClient, DataRequirement};
use crate::overlay_client::PunishReason;
use crate::storage::PersistentStateKind;

#[async_trait]
pub(crate) trait StarterClient: Send + Sync + 'static {
    async fn find_persistent_state<'a>(
        &'a self,
        block_id: &'a BlockId,
        kind: PersistentStateKind,
    ) -> Result<FoundState<'a>>;

    async fn get_block_full(&self, mc_seqno: u32, block_id: &BlockId)
    -> Result<FoundBlockDataFull>;
}

#[async_trait]
impl StarterClient for BlockchainRpcClient {
    async fn find_persistent_state<'a>(
        &'a self,
        block_id: &'a BlockId,
        kind: PersistentStateKind,
    ) -> Result<FoundState<'a>> {
        let pending_state = self.find_persistent_state(block_id, kind).await?;
        let this = self.clone();

        Ok(FoundState {
            download: Box::new(move |output| {
                Box::pin(async move {
                    let output = this
                        .download_persistent_state(pending_state, output)
                        .await?;
                    Ok(output)
                })
            }),
        })
    }

    async fn get_block_full(
        &self,
        _mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<FoundBlockDataFull> {
        let res = self
            .get_block_full(block_id, DataRequirement::Expected)
            .await?;
        let Some(data) = res.data else {
            return Err(crate::overlay_client::Error::NotFound.into());
        };

        Ok(FoundBlockDataFull {
            data,
            punish: Box::new(move |reason| res.neighbour.punish(reason)),
        })
    }
}

pub struct FoundBlockDataFull {
    pub data: BlockDataFull,
    pub punish: Box<PunishFn>,
}

pub struct FoundState<'a> {
    pub download: Box<DownloadFn<'a>>,
}

type DownloadFn<'a> = dyn FnOnce(File) -> BoxFuture<'a, Result<File>> + Send + 'a;
type PunishFn = dyn FnOnce(PunishReason) + Send + 'static;

#[cfg(feature = "s3")]
mod s3 {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, Ordering};

    use arc_swap::ArcSwapOption;
    use tycho_block_util::archive::Archive;
    use tycho_util::fs::TargetWriter;

    use super::*;
    use crate::s3::S3Client;
    use crate::storage::CoreStorage;

    pub struct S3StarterClient {
        s3_client: S3Client,
        storage: CoreStorage,
        last_archive: ArcSwapOption<Archive>,
    }

    impl S3StarterClient {
        pub fn new(s3_client: S3Client, storage: CoreStorage) -> Self {
            Self {
                s3_client,
                storage,
                last_archive: Default::default(),
            }
        }
    }

    #[async_trait]
    impl StarterClient for S3StarterClient {
        async fn find_persistent_state<'a>(
            &'a self,
            block_id: &'a BlockId,
            kind: PersistentStateKind,
        ) -> Result<FoundState<'a>> {
            let Some(_) = self
                .s3_client
                .get_persistent_state_info(block_id, kind)
                .await?
            else {
                anyhow::bail!("not found");
            };

            Ok(FoundState {
                download: Box::new(move |output| {
                    Box::pin(async move {
                        let output = self
                            .s3_client
                            .download_persistent_state(block_id, kind, output)
                            .await?;
                        Ok(output)
                    })
                }),
            })
        }

        async fn get_block_full(
            &self,
            mc_seqno: u32,
            block_id: &BlockId,
        ) -> Result<FoundBlockDataFull> {
            let storage = &self.storage;

            let archive_id = storage.block_storage().estimate_archive_id(mc_seqno);

            let extract_block_data = |archive: &Archive| {
                archive.blocks.get(block_id).and_then(|entry| {
                    match (&entry.block, &entry.proof, &entry.queue_diff) {
                        (Some(block_data), Some(proof_data), Some(queue_diff_data)) => {
                            Some(BlockDataFull {
                                block_id: *block_id,
                                block_data: block_data.clone(),
                                proof_data: proof_data.clone(),
                                queue_diff_data: queue_diff_data.clone(),
                            })
                        }
                        _ => None,
                    }
                })
            };

            if let Some(archive) = self.last_archive.load_full()
                && let Some(data) = extract_block_data(&archive)
            {
                return Ok(FoundBlockDataFull {
                    data,
                    punish: Box::new(|_| {}),
                });
            }

            let Some(info) = self.s3_client.get_archive_info(archive_id).await? else {
                anyhow::bail!("archive {archive_id} not found")
            };

            let output = TargetWriter::File(std::io::BufWriter::new(
                storage.context().temp_files().unnamed_file().open()?,
            ));

            let output = self
                .s3_client
                .download_archive(info.archive_id, output)
                .await?;

            let span = tracing::Span::current();
            let archive = tokio::task::spawn_blocking({
                move || {
                    let _span = span.enter();
                    let bytes = output.try_freeze()?;
                    let archive = Archive::new(bytes)?;
                    archive.check_mc_blocks_range()?;
                    anyhow::Ok(archive)
                }
            })
            .await??;

            let Some(data) = extract_block_data(&archive) else {
                anyhow::bail!("archive {archive_id} does not contain the requested block");
            };

            self.last_archive.store(Some(Arc::new(archive)));

            Ok(FoundBlockDataFull {
                data,
                punish: Box::new(|_| {}),
            })
        }
    }

    // === Hybrid S3 Client ===

    pub struct HybridStarterClient<T1, T2> {
        primary: T1,
        secondary: T2,
        prefer: HybridStarterClientState,
    }

    impl<T1, T2> HybridStarterClient<T1, T2> {
        pub fn new(primary: T1, secondary: T2) -> Self {
            Self {
                primary,
                secondary,
                prefer: Default::default(),
            }
        }
    }

    #[async_trait]
    impl<T1, T2> StarterClient for HybridStarterClient<T1, T2>
    where
        T1: StarterClient,
        T2: StarterClient,
    {
        async fn find_persistent_state<'a>(
            &'a self,
            block_id: &'a BlockId,
            kind: PersistentStateKind,
        ) -> Result<FoundState<'a>> {
            self.hybrid_call(
                |primary| primary.find_persistent_state(block_id, kind),
                |secondary| secondary.find_persistent_state(block_id, kind),
            )
            .await
        }

        async fn get_block_full(
            &self,
            mc_seqno: u32,
            block_id: &BlockId,
        ) -> Result<FoundBlockDataFull> {
            self.hybrid_call(
                |primary| primary.get_block_full(mc_seqno, block_id),
                |secondary| secondary.get_block_full(mc_seqno, block_id),
            )
            .await
        }
    }

    impl<T1, T2> HybridStarterClient<T1, T2>
    where
        T1: StarterClient,
        T2: StarterClient,
    {
        async fn hybrid_call<'a, F1, F2, Fut1, Fut2, R>(
            &'a self,
            primary_fn: F1,
            secondary_fn: F2,
        ) -> Result<R>
        where
            F1: Fn(&'a T1) -> Fut1,
            F2: Fn(&'a T2) -> Fut2,
            Fut1: Future<Output = Result<R>> + 'a,
            Fut2: Future<Output = Result<R>> + 'a,
        {
            if let Some(prefer) = self.prefer.get() {
                match prefer {
                    HybridStarterClientPart::Primary => {
                        let res = primary_fn(&self.primary).await;
                        if res.is_ok() {
                            return res;
                        }
                    }
                    HybridStarterClientPart::Secondary => {
                        let res = secondary_fn(&self.secondary).await;
                        if res.is_ok() {
                            return res;
                        }
                    }
                }
            }

            self.prefer.set(None);

            let primary = pin!(primary_fn(&self.primary));
            let secondary = pin!(secondary_fn(&self.secondary));

            match futures_util::future::select(primary, secondary).await {
                futures_util::future::Either::Left((result, other)) => {
                    match result {
                        Ok(state) => {
                            self.prefer.set(Some(HybridStarterClientPart::Primary));
                            return Ok(state);
                        }
                        Err(e) => tracing::warn!("primary starter client error: {e:?}"),
                    }
                    other.await.inspect(|_| {
                        self.prefer.set(Some(HybridStarterClientPart::Secondary));
                    })
                }
                futures_util::future::Either::Right((result, other)) => {
                    match result {
                        Ok(state) => {
                            self.prefer.set(Some(HybridStarterClientPart::Secondary));
                            return Ok(state);
                        }
                        Err(e) => tracing::warn!("secondary starter client error: {e:?}"),
                    }
                    other.await.inspect(|_| {
                        self.prefer.set(Some(HybridStarterClientPart::Primary));
                    })
                }
            }
        }
    }

    #[derive(Default)]
    struct HybridStarterClientState(AtomicU8);

    impl HybridStarterClientState {
        fn get(&self) -> Option<HybridStarterClientPart> {
            match self.0.load(Ordering::Acquire) {
                1 => Some(HybridStarterClientPart::Primary),
                2 => Some(HybridStarterClientPart::Secondary),
                _ => None,
            }
        }

        fn set(&self, value: Option<HybridStarterClientPart>) {
            let value = match value {
                None => 0,
                Some(HybridStarterClientPart::Primary) => 1,
                Some(HybridStarterClientPart::Secondary) => 2,
            };
            self.0.store(value, Ordering::Release);
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum HybridStarterClientPart {
        Primary,
        Secondary,
    }

    impl std::ops::Not for HybridStarterClientPart {
        type Output = Self;

        #[inline]
        fn not(self) -> Self::Output {
            match self {
                Self::Primary => Self::Secondary,
                Self::Secondary => Self::Primary,
            }
        }
    }
}
