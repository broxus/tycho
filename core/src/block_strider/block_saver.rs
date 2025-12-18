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
        // Construct prev ids
        let (prev_id, prev_id_alt) = cx
            .block
            .construct_prev_id()
            .context("failed to construct prev id")?;

        // Store block data and get handle
        let handle = self
            .create_or_load_block_handle(&cx.mc_block_id, &cx.block, &cx.archive_data)
            .await?;

        // Store block connections
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
                        // Special case for the right child after split
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
                    connections.store_connection(&handle, BlockConnection::Next1, block_id);
                }
                if let Some(handle) = block_handles.load_handle(prev_id_alt) {
                    connections.store_connection(&handle, BlockConnection::Next1, block_id);
                }
                connections.store_connection(&handle, BlockConnection::Prev1, &prev_id);
                connections.store_connection(&handle, BlockConnection::Prev2, prev_id_alt);
            }
        }

        // Save block to archive if needed
        if self.storage.config().store_archives {
            let storage = self.storage.clone();
            let handle = handle.clone();
            let mc_is_key_block = cx.mc_is_key_block;
            cx.delayed.spawn(move || async move {
                tracing::debug!(block_id = %handle.id(), "saving block into archive");
                storage
                    .block_storage()
                    .move_into_archive(&handle, mc_is_key_block)
                    .await
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
        let block_storage = self.storage.block_storage();

        let info = block.load_info()?;
        let res = block_storage
            .store_block_data(block, archive_data, NewBlockMeta {
                is_key_block: info.key_block,
                gen_utime: info.gen_utime,
                ref_by_mc_seqno: mc_block_id.seqno,
            })
            .await?;

        Ok(res.handle)
    }
}

impl BlockSubscriber for BlockSaver {
    type Prepared = BlockHandle;

    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
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
