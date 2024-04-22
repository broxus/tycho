use crate::blockchain_client::BlockchainClient;
use crate::proto::overlay::BlockFull;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_storage::{BlockConnection, Storage};

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
            let get_block = || async {
                let res = self.get_next_block_full(*prev_block_id).await?;
                let block = match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: data,
                        ..
                    } => {
                        let block = BlockStuff::deserialize_checked(*block_id, data)?;
                        Some(BlockStuffAug::new(block, data.to_vec()))
                    }
                    BlockFull::Empty => None,
                };

                Ok::<_, anyhow::Error>(block)
            };

            match get_block().await {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let get_block = || async {
                let res = self.get_block_full(*block_id).await?;
                let block = match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: data,
                        ..
                    } => {
                        let block = BlockStuff::deserialize_checked(*block_id, data)?;
                        Some(BlockStuffAug::new(block, data.to_vec()))
                    }
                    BlockFull::Empty => None,
                };

                Ok::<_, anyhow::Error>(block)
            };

            match get_block().await {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
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
            let block_handle_storage = self.block_handle_storage();
            let block_connection_storage = self.block_connection_storage();

            let get_next_block = || async {
                let next_block_id = match block_handle_storage.load_handle(prev_block_id)? {
                    Some(handle) if handle.meta().has_next1() => block_connection_storage
                        .load_connection(prev_block_id, BlockConnection::Next1)?,
                    _ => return Ok(None),
                };

                let block = match block_handle_storage.load_handle(&next_block_id)? {
                    Some(handle) if handle.meta().has_data() => {
                        let data = block_storage.load_block_data_raw(&handle).await?;

                        let block = BlockStuff::deserialize_checked(next_block_id, &data)?;
                        Some(BlockStuffAug::new(block, data))
                    }
                    _ => None,
                };

                Ok::<_, anyhow::Error>(block)
            };

            match get_next_block().await {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let block_storage = self.block_storage();
            let block_handle_storage = self.block_handle_storage();

            let get_block = || async {
                let block = match block_handle_storage.load_handle(block_id)? {
                    Some(handle) if handle.meta().has_data() => {
                        let data = block_storage.load_block_data_raw(&handle).await?;

                        let block = BlockStuff::deserialize_checked(*block_id, &data)?;
                        Some(BlockStuffAug::new(block, data))
                    }
                    _ => None,
                };

                Ok::<_, anyhow::Error>(block)
            };

            match get_block().await {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
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
