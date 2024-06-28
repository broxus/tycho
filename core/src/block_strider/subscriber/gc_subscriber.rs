use std::sync::Arc;

use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockStuff;
use tycho_storage::Storage;

use crate::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};

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

    fn handle<'a>(
        &'a self,
        block: &'a BlockStuff,
        mc_block_id: &'a BlockId
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        if !block.id().is_masterchain() {
            return Box::pin(futures_util::future::ready(Ok(())));
        }

        let Ok(block_info) = block.block().load_info() else {
            return Box::pin(futures_util::future::ready(Ok(())));
        };

        let enabled = self.inner.storage.gc_enable_for_sync();

        match (
            self.inner.storage.config().blocks_gc_config,
            enabled,
            block_info.key_block,
        ) {
            (Some(config), true, true) => {
                Box::pin(self.inner.storage.block_storage().remove_outdated_blocks(
                    &mc_block_id,
                    config.max_blocks_per_batch,
                    config.kind,
                ))
            }
            _ => Box::pin(futures_util::future::ready(Ok(()))),
        }
    }
}

struct Inner {
    storage: Storage,
}

impl StateSubscriber for GcSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, anyhow::Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.handle(&cx.block, &cx.mc_block_id)
    }
}

impl BlockSubscriber for GcSubscriber {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = BoxFuture<'a, anyhow::Result<()>>;

    fn prepare_block<'a>(&'a self, _cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        self.handle(&cx.block, &cx.mc_block_id)
    }
}
