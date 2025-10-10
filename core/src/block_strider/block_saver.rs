use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_types::models::BlockId;
use crate::block_strider::{BlockSubscriber, BlockSubscriberContext};
use crate::storage::{BlockConnection, BlockHandle, CoreStorage, NewBlockMeta};
#[repr(transparent)]
#[derive(Clone)]
pub struct BlockSaver {
    storage: CoreStorage,
}
impl BlockSaver {
    pub fn new(storage: CoreStorage) -> Self {
        Self { storage }
    }
    pub async fn save_block(&self, cx: &BlockSubscriberContext) -> Result<BlockHandle> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(save_block)),
            file!(),
            21u32,
        );
        let cx = cx;
        let (prev_id, prev_id_alt) = cx
            .block
            .construct_prev_id()
            .context("failed to construct prev id")?;
        let handle = {
            __guard.end_section(31u32);
            let __result = self
                .create_or_load_block_handle(
                    &cx.mc_block_id,
                    &cx.block,
                    &cx.archive_data,
                )
                .await;
            __guard.start_section(31u32);
            __result
        }?;
        let block_handles = self.storage.block_handle_storage();
        let connections = self.storage.block_connection_storage();
        let block_id = cx.block.id();
        let prev_handle = block_handles.load_handle(&prev_id);
        match prev_id_alt {
            None => {
                if let Some(handle) = prev_handle {
                    let direction = if block_id.shard != prev_id.shard
                        && prev_id.shard.split().unwrap().1 == block_id.shard
                    {
                        BlockConnection::Next2
                    } else {
                        BlockConnection::Next1
                    };
                    connections.store_connection(&handle, direction, block_id);
                }
                connections.store_connection(&handle, BlockConnection::Prev1, &prev_id);
            }
            Some(ref prev_id_alt) => {
                if let Some(handle) = prev_handle {
                    connections
                        .store_connection(&handle, BlockConnection::Next1, block_id);
                }
                if let Some(handle) = block_handles.load_handle(prev_id_alt) {
                    connections
                        .store_connection(&handle, BlockConnection::Next1, block_id);
                }
                connections.store_connection(&handle, BlockConnection::Prev1, &prev_id);
                connections
                    .store_connection(&handle, BlockConnection::Prev2, prev_id_alt);
            }
        }
        if self.storage.config().archives_gc.is_some() {
            let storage = self.storage.clone();
            let handle = handle.clone();
            let mc_is_key_block = cx.mc_is_key_block;
            cx.delayed
                .spawn(move || async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        73u32,
                    );
                    tracing::debug!(
                        block_id = % handle.id(), "saving block into archive"
                    );
                    {
                        __guard.end_section(78u32);
                        let __result = storage
                            .block_storage()
                            .move_into_archive(&handle, mc_is_key_block)
                            .await;
                        __guard.start_section(78u32);
                        __result
                    }
                })?;
        }
        Ok(handle)
    }
    async fn create_or_load_block_handle(
        &self,
        mc_block_id: &BlockId,
        block: &BlockStuff,
        archive_data: &ArchiveData,
    ) -> Result<BlockHandle> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(create_or_load_block_handle)),
            file!(),
            90u32,
        );
        let mc_block_id = mc_block_id;
        let block = block;
        let archive_data = archive_data;
        let block_storage = self.storage.block_storage();
        let info = block.load_info()?;
        let res = {
            __guard.end_section(100u32);
            let __result = block_storage
                .store_block_data(
                    block,
                    archive_data,
                    NewBlockMeta {
                        is_key_block: info.key_block,
                        gen_utime: info.gen_utime,
                        ref_by_mc_seqno: mc_block_id.seqno,
                    },
                )
                .await;
            __guard.start_section(100u32);
            __result
        }?;
        Ok(res.handle)
    }
}
impl BlockSubscriber for BlockSaver {
    type Prepared = BlockHandle;
    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    fn prepare_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
    ) -> Self::PrepareBlockFut<'a> {
        Box::pin(self.save_block(cx))
    }
    fn handle_block<'a>(
        &'a self,
        _: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }
}
