use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use tycho_storage::Storage;

use crate::block_strider::{BlockSubscriber, BlockSubscriberContext};

#[repr(transparent)]
pub struct GcSubscriber {
    inner: Arc<Inner>,
}

impl GcSubscriber {
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: Arc::new(Inner { storage }),
        }
    }
}

struct Inner {
    storage: Storage,
}

impl BlockSubscriber for GcSubscriber {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type AfterBlockHandleFut<'a> = BoxFuture<'a, anyhow::Result<()>>;

    fn prepare_block<'a>(&'a self, _cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        _cx: &'a BlockSubscriberContext,
        _prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn after_block_handle<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
    ) -> Self::AfterBlockHandleFut<'a> {
        let Ok(block_info) = cx.block.block().load_info() else {
            return Box::pin(futures_util::future::ready(Ok(())))
        };

        self.inner
            .storage
            .node_state()
            .store_shards_client_mc_block_id(&cx.mc_block_id);

        let enabled = self
            .inner
            .storage
            .gc_enable_for_sync()
            .load(Ordering::Acquire);

        match (
            self.inner.storage.config().blocks_gc_config,
            enabled,
            block_info.key_block,
        ) {
            (Some(config), true, true) => {
                Box::pin(self.inner.storage.block_storage().remove_outdated_blocks(
                    &cx.mc_block_id,
                    config.max_blocks_per_batch,
                    config.kind,
                ))
            }
            _ => Box::pin(futures_util::future::ready(Ok(()))),
        }
    }
}
