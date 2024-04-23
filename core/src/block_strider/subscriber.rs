use std::future::Future;
use std::sync::Arc;

use futures_util::future;

use tycho_block_util::block::BlockStuffAug;
use tycho_block_util::state::ShardStateStuff;

pub trait BlockSubscriber: Send + Sync + 'static {
    type HandleBlockFut: Future<Output = anyhow::Result<()>> + Send + 'static;

    fn handle_block(
        &self,
        block: &BlockStuffAug,
        state: Option<&Arc<ShardStateStuff>>,
    ) -> Self::HandleBlockFut;
}

impl<T: BlockSubscriber> BlockSubscriber for Box<T> {
    type HandleBlockFut = T::HandleBlockFut;

    fn handle_block(
        &self,
        block: &BlockStuffAug,
        state: Option<&Arc<ShardStateStuff>>,
    ) -> Self::HandleBlockFut {
        <T as BlockSubscriber>::handle_block(self, block, state)
    }
}

impl<T: BlockSubscriber> BlockSubscriber for Arc<T> {
    type HandleBlockFut = T::HandleBlockFut;

    fn handle_block(
        &self,
        block: &BlockStuffAug,
        state: Option<&Arc<ShardStateStuff>>,
    ) -> Self::HandleBlockFut {
        <T as BlockSubscriber>::handle_block(self, block, state)
    }
}

pub struct FanoutBlockSubscriber<T1, T2> {
    pub left: T1,
    pub right: T2,
}

impl<T1: BlockSubscriber, T2: BlockSubscriber> BlockSubscriber for FanoutBlockSubscriber<T1, T2> {
    type HandleBlockFut = future::BoxFuture<'static, anyhow::Result<()>>;

    fn handle_block(
        &self,
        block: &BlockStuffAug,
        state: Option<&Arc<ShardStateStuff>>,
    ) -> Self::HandleBlockFut {
        let left = self.left.handle_block(block, state);
        let right = self.right.handle_block(block, state);

        Box::pin(async move {
            let (l, r) = future::join(left, right).await;
            l.and(r)
        })
    }
}

#[cfg(any(test, feature = "test"))]
pub mod test {
    use super::*;

    pub struct PrintSubscriber;

    impl BlockSubscriber for PrintSubscriber {
        type HandleBlockFut = future::Ready<anyhow::Result<()>>;

        fn handle_block(
            &self,
            block: &BlockStuffAug,
            _state: Option<&Arc<ShardStateStuff>>,
        ) -> Self::HandleBlockFut {
            tracing::info!("handling block: {:?}", block.id());
            future::ready(Ok(()))
        }
    }
}
