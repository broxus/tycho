use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_storage::Storage;

use crate::block_strider::provider::OptionalBlockStuff;
use crate::block_strider::BlockProvider;

// TODO: Add an explicit storage provider type

pub struct StorageBlockProvider {
    storage: Storage,
}

impl StorageBlockProvider {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl BlockProvider for StorageBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async {
            let block_storage = self.storage.block_storage();

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
            let block_storage = self.storage.block_storage();

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
