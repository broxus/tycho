use std::future::Future;

use anyhow::Result;
use everscale_types::models::*;
use futures_util::future::{self, BoxFuture};
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;

// === trait BlockSubscriber ===

pub struct BlockSubscriberContext {
    pub mc_block_id: BlockId,
    pub block: BlockStuff,
    pub archive_data: ArchiveData,
}

pub trait BlockSubscriber: Send + Sync + 'static {
    type HandleBlockFut<'a>: Future<Output = Result<()>> + Send + 'a;

    fn handle_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::HandleBlockFut<'a>;
}

impl<T: BlockSubscriber> BlockSubscriber for Box<T> {
    type HandleBlockFut<'a> = T::HandleBlockFut<'a>;

    fn handle_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::HandleBlockFut<'a> {
        <T as BlockSubscriber>::handle_block(self, cx)
    }
}

pub trait BlockSubscriberExt: Sized {
    fn chain<T: BlockSubscriber>(self, other: T) -> ChainSubscriber<Self, T>;
}

impl<B: BlockSubscriber> BlockSubscriberExt for B {
    fn chain<T: BlockSubscriber>(self, other: T) -> ChainSubscriber<Self, T> {
        ChainSubscriber {
            left: self,
            right: other,
        }
    }
}

// === trait StateSubscriber ===

pub struct StateSubscriberContext {
    pub mc_block_id: BlockId,
    pub block: BlockStuff,
    pub archive_data: ArchiveData,
    pub state: ShardStateStuff,
}

pub trait StateSubscriber: Send + Sync + 'static {
    type HandleStateFut<'a>: Future<Output = Result<()>> + Send + 'a;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a>;
}

impl<T: StateSubscriber> StateSubscriber for Box<T> {
    type HandleStateFut<'a> = T::HandleStateFut<'a>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        <T as StateSubscriber>::handle_state(self, cx)
    }
}

pub trait StateSubscriberExt: Sized {
    fn chain<T: StateSubscriber>(self, other: T) -> ChainSubscriber<Self, T>;
}

impl<B: StateSubscriber> StateSubscriberExt for B {
    fn chain<T: StateSubscriber>(self, other: T) -> ChainSubscriber<Self, T> {
        ChainSubscriber {
            left: self,
            right: other,
        }
    }
}

// === NoopSubscriber ===

#[derive(Default, Debug, Clone, Copy)]
pub struct NoopSubscriber;

impl BlockSubscriber for NoopSubscriber {
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_block(&self, _cx: &BlockSubscriberContext) -> Self::HandleBlockFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

impl StateSubscriber for NoopSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state(&self, _cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

// === ChainSubscriber ===

pub struct ChainSubscriber<T1, T2> {
    left: T1,
    right: T2,
}

impl<T1: BlockSubscriber, T2: BlockSubscriber> BlockSubscriber for ChainSubscriber<T1, T2> {
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::HandleBlockFut<'a> {
        let left = self.left.handle_block(cx);
        let right = self.right.handle_block(cx);

        Box::pin(async move {
            left.await?;
            right.await
        })
    }
}

impl<T1: StateSubscriber, T2: StateSubscriber> StateSubscriber for ChainSubscriber<T1, T2> {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        let left = self.left.handle_state(cx);
        let right = self.right.handle_state(cx);

        Box::pin(async move {
            left.await?;
            right.await
        })
    }
}

// === (T1, T2) aka `join` ===

impl<T1: BlockSubscriber, T2: BlockSubscriber> BlockSubscriber for (T1, T2) {
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::HandleBlockFut<'a> {
        let left = self.0.handle_block(cx);
        let right = self.1.handle_block(cx);

        Box::pin(async move {
            let (l, r) = future::join(left, right).await;
            l.and(r)
        })
    }
}

impl<T1: StateSubscriber, T2: StateSubscriber> StateSubscriber for (T1, T2) {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        let left = self.0.handle_state(cx);
        let right = self.1.handle_state(cx);

        Box::pin(async move {
            let (l, r) = future::join(left, right).await;
            l.and(r)
        })
    }
}

#[cfg(any(test, feature = "test"))]
pub mod test {
    use super::*;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct PrintSubscriber;

    impl BlockSubscriber for PrintSubscriber {
        type HandleBlockFut<'a> = future::Ready<Result<()>>;

        fn handle_block(&self, cx: &BlockSubscriberContext) -> Self::HandleBlockFut<'_> {
            tracing::info!(
                block_id = %cx.block.id(),
                mc_block_id = %cx.mc_block_id,
                "handling block"
            );
            future::ready(Ok(()))
        }
    }

    impl StateSubscriber for PrintSubscriber {
        type HandleStateFut<'a> = future::Ready<anyhow::Result<()>>;

        fn handle_state(&self, cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
            tracing::info!(
                block_id = %cx.block.id(),
                mc_block_id = %cx.mc_block_id,
                "handling state"
            );
            future::ready(Ok(()))
        }
    }
}
