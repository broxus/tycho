use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_storage::Storage;

use crate::blockchain_client::BlockchainClient;
use crate::proto::overlay::BlockFull;

pub type OptionalBlockStuff = Option<anyhow::Result<BlockStuffAug>>;

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

// === Provider combinators ===
struct ChainBlockProvider<T1, T2> {
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

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let res = self.left.get_block(block_id).await;
            if res.is_some() {
                return res;
            }
            self.right.get_block(block_id).await
        })
    }
}

impl BlockProvider for BlockchainClient {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async {
            let config = self.config();

            loop {
                let res = self.get_next_block_full(prev_block_id).await;

                let block = match res {
                    Ok(res) if matches!(res.data(), BlockFull::Found { .. }) => {
                        let (block_id, data) = match res.data() {
                            BlockFull::Found {
                                block_id, block, ..
                            } => (*block_id, block.clone()),
                            BlockFull::Empty => unreachable!(),
                        };

                        match BlockStuff::deserialize_checked(block_id, &data) {
                            Ok(block) => {
                                res.accept();
                                Some(Ok(BlockStuffAug::new(block, data)))
                            }
                            Err(e) => {
                                tracing::error!("failed to deserialize block: {:?}", e);
                                res.reject();
                                None
                            }
                        }
                    }
                    Ok(_) => None,
                    Err(e) => {
                        tracing::error!("failed to get next block: {:?}", e);
                        None
                    }
                };

                if block.is_some() {
                    break block;
                }

                tokio::time::sleep(config.get_next_block_polling_interval).await;
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let config = self.config();

            loop {
                let res = match self.get_block_full(block_id).await {
                    Ok(res) => res,
                    Err(e) => {
                        tracing::error!("failed to get block: {:?}", e);
                        tokio::time::sleep(config.get_block_polling_interval).await;
                        continue;
                    }
                };

                let block = match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: data,
                        ..
                    } => match BlockStuff::deserialize_checked(*block_id, data) {
                        Ok(block) => Some(Ok(BlockStuffAug::new(block, data.clone()))),
                        Err(e) => {
                            res.accept();
                            tracing::error!("failed to deserialize block: {:?}", e);
                            tokio::time::sleep(config.get_block_polling_interval).await;
                            continue;
                        }
                    },
                    BlockFull::Empty => {
                        tokio::time::sleep(config.get_block_polling_interval).await;
                        continue;
                    }
                };

                break block;
            }
        })
    }
}

impl BlockProvider for Storage {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async {
            let block_storage = self.block_storage();

            let get_next_block = || async {
                let rx = block_storage
                    .subscribe_to_next_block(*prev_block_id)
                    .await?;

                let block = rx.await?;

                Ok::<_, anyhow::Error>(block)
            };

            match get_next_block().await {
                Ok(block) => Some(Ok(block)),
                Err(e) => Some(Err(e)),
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let block_storage = self.block_storage();

            let get_block = || async {
                let rx = block_storage.subscribe_to_block(*block_id).await?;
                let block = rx.await?;

                Ok::<_, anyhow::Error>(block)
            };

            match get_block().await {
                Ok(block) => Some(Ok(block)),
                Err(e) => Some(Err(e)),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tycho_block_util::block::BlockStuff;

    struct MockBlockProvider {
        // let's give it some state, pretending it's useful
        has_block: AtomicBool,
    }

    impl BlockProvider for MockBlockProvider {
        type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
        type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

        fn get_next_block<'a>(&'a self, _prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
            Box::pin(async {
                if self.has_block.load(Ordering::Acquire) {
                    Some(Ok(get_empty_block()))
                } else {
                    None
                }
            })
        }

        fn get_block<'a>(&'a self, _block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
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
        let block_data = include_bytes!("../../tests/data/empty_block.bin");
        let block = everscale_types::boc::BocRepr::decode(block_data).unwrap();
        BlockStuffAug::new(
            BlockStuff::with_block(get_default_block_id(), block),
            block_data.as_slice(),
        )
    }

    fn get_default_block_id() -> BlockId {
        BlockId::default()
    }
}
