use anyhow::Result;
use futures_util::future;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
use tycho_types::models::BlockId;
use crate::block_strider::BlockProvider;
use crate::block_strider::provider::OptionalBlockStuff;
use crate::storage::CoreStorage;
pub struct StorageBlockProvider {
    storage: CoreStorage,
}
impl StorageBlockProvider {
    pub fn new(storage: CoreStorage) -> Self {
        Self { storage }
    }
}
impl BlockProvider for StorageBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;
    fn get_next_block<'a>(
        &'a self,
        prev_block_id: &'a BlockId,
    ) -> Self::GetNextBlockFut<'a> {
        Box::pin(async {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                29u32,
            );
            let block_storage = self.storage.block_storage();
            Some({
                __guard.end_section(31u32);
                let __result = block_storage.wait_for_next_block(prev_block_id).await;
                __guard.start_section(31u32);
                __result
            })
        })
    }
    fn get_block<'a>(
        &'a self,
        block_id_relation: &'a BlockIdRelation,
    ) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                36u32,
            );
            Some({
                __guard.end_section(41u32);
                let __result = self
                    .storage
                    .block_storage()
                    .wait_for_block(&block_id_relation.block_id)
                    .await;
                __guard.start_section(41u32);
                __result
            })
        })
    }
    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}
