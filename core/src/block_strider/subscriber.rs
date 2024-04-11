use futures_util::future;
use std::future::Future;
use tycho_block_util::block::BlockStuff;

pub trait BlockSubscriber: Send + Sync + 'static {
    type HandleBlockFut: Future<Output = anyhow::Result<()>> + Send + 'static;

    fn handle_block(&self, block: &BlockStuff) -> Self::HandleBlockFut;
}

impl<T: BlockSubscriber> BlockSubscriber for Box<T> {
    type HandleBlockFut = T::HandleBlockFut;

    fn handle_block(&self, block: &BlockStuff) -> Self::HandleBlockFut {
        <T as BlockSubscriber>::handle_block(self, block)
    }
}

pub struct FanoutBlockSubscriber<T1, T2> {
    pub left: T1,
    pub right: T2,
}

impl<T1: BlockSubscriber, T2: BlockSubscriber> BlockSubscriber for FanoutBlockSubscriber<T1, T2> {
    type HandleBlockFut = future::BoxFuture<'static, anyhow::Result<()>>;

    fn handle_block(&self, block: &BlockStuff) -> Self::HandleBlockFut {
        let left = self.left.handle_block(block);
        let right = self.right.handle_block(block);

        Box::pin(async move {
            let (l, r) = future::join(left, right).await;
            l.and(r)
        })
    }
}

#[cfg(test)]
pub struct PrintSubscriber;

#[cfg(test)]
impl BlockSubscriber for PrintSubscriber {
    type HandleBlockFut = future::Ready<anyhow::Result<()>>;

    fn handle_block(&self, block: &BlockStuff) -> Self::HandleBlockFut {
        println!("Handling block: {:?}", block.id());
        future::ready(Ok(()))
    }
}
