use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
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
            match block_storage.wait_for_next_block(prev_block_id).await {
                Ok(block) => Some(Ok(block)),
                Err(e) => Some(Err(e)),
            }
        })
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let block_storage = self.storage.block_storage();
            match block_storage
                .wait_for_block(&block_id_relation.block_id)
                .await
            {
                Ok(block) => Some(Ok(block)),
                Err(e) => Some(Err(e)),
            }
        })
    }
}
