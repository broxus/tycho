use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use futures_util::future::{self, BoxFuture};
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;

pub use self::futures::{
    DelayedTasks, DelayedTasksJoinHandle, DelayedTasksSpawner, OptionHandleFut, OptionPrepareFut,
};
pub use self::gc_subscriber::{GcSubscriber, ManualGcTrigger};
pub use self::metrics_subscriber::MetricsSubscriber;
pub use self::ps_subscriber::PsSubscriber;
use crate::storage::{BlockHandle, CoreStorage};

mod futures;
mod gc_subscriber;
mod metrics_subscriber;
mod ps_subscriber;

// === trait BlockSubscriber ===

#[derive(Clone)]
pub struct BlockSubscriberContext {
    /// Related masterchain block id.
    /// In case of context for mc block this id is the same as `block.id()`.
    pub mc_block_id: BlockId,
    /// Related masterchain block flag.
    /// In case of context for mc block this flag is the same as `is_key_block`.
    pub mc_is_key_block: bool,
    /// Whether the `block` from this context is a key block.
    pub is_key_block: bool,
    /// Parsed block data.
    pub block: BlockStuff,
    /// Serialized block data.
    pub archive_data: ArchiveData,
    /// Delayed tasks to wait before commit.
    pub delayed: DelayedTasks,
}

pub trait BlockSubscriber: Send + Sync + 'static {
    type Prepared: Send;

    type PrepareBlockFut<'a>: Future<Output = Result<Self::Prepared>> + Send + 'a;
    type HandleBlockFut<'a>: Future<Output = Result<()>> + Send + 'a;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a>;

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a>;
}

impl<T: BlockSubscriber> BlockSubscriber for Option<T> {
    type Prepared = Option<T::Prepared>;

    type PrepareBlockFut<'a> = OptionPrepareFut<T::PrepareBlockFut<'a>>;
    type HandleBlockFut<'a> = OptionHandleFut<T::HandleBlockFut<'a>>;

    #[inline]
    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        OptionPrepareFut::from(self.as_ref().map(|s| s.prepare_block(cx)))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        OptionHandleFut::from(match (self, prepared) {
            (Some(subscriber), Some(prepared)) => Some(subscriber.handle_block(cx, prepared)),
            _ => None,
        })
    }
}

impl<T: BlockSubscriber> BlockSubscriber for Box<T> {
    type Prepared = T::Prepared;

    type PrepareBlockFut<'a> = T::PrepareBlockFut<'a>;
    type HandleBlockFut<'a> = T::HandleBlockFut<'a>;

    #[inline]
    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        <T as BlockSubscriber>::prepare_block(self, cx)
    }

    #[inline]
    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        <T as BlockSubscriber>::handle_block(self, cx, prepared)
    }
}

impl<T: BlockSubscriber> BlockSubscriber for Arc<T> {
    type Prepared = T::Prepared;

    type PrepareBlockFut<'a> = T::PrepareBlockFut<'a>;
    type HandleBlockFut<'a> = T::HandleBlockFut<'a>;

    #[inline]
    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        <T as BlockSubscriber>::prepare_block(self, cx)
    }

    #[inline]
    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        <T as BlockSubscriber>::handle_block(self, cx, prepared)
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
    /// Related masterchain block id.
    /// In case of context for mc block this id is the same as `block.id()`.
    pub mc_block_id: BlockId,
    /// Related masterchain block flag.
    /// In case of context for mc block this flag is the same as `is_key_block`.
    pub mc_is_key_block: bool,
    /// Whether the `block` from this context is a key block.
    pub is_key_block: bool,
    /// Parsed block data.
    pub block: BlockStuff,
    /// Serialized block data.
    pub archive_data: ArchiveData,
    /// Applied shard state.
    pub state: ShardStateStuff,
    /// Delayed tasks to wait before commit.
    pub delayed: DelayedTasks,
}

pub trait StateSubscriber: Send + Sync + 'static {
    type HandleStateFut<'a>: Future<Output = Result<()>> + Send + 'a;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a>;
}

impl<T: StateSubscriber> StateSubscriber for Option<T> {
    type HandleStateFut<'a> = OptionHandleFut<T::HandleStateFut<'a>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        OptionHandleFut::<_>::from(self.as_ref().map(|s| s.handle_state(cx)))
    }
}

impl<T: StateSubscriber> StateSubscriber for Box<T> {
    type HandleStateFut<'a> = T::HandleStateFut<'a>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        <T as StateSubscriber>::handle_state(self, cx)
    }
}

impl<T: StateSubscriber> StateSubscriber for Arc<T> {
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

// === trait ArchiveSubscriber ===

pub struct ArchiveSubscriberContext<'a> {
    pub archive_id: u32,
    pub storage: &'a CoreStorage,
}

pub trait ArchiveSubscriber: Send + Sync + 'static {
    type HandleArchiveFut<'a>: Future<Output = Result<()>> + Send + 'a;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a>;
}

impl<T: ArchiveSubscriber> ArchiveSubscriber for Option<T> {
    type HandleArchiveFut<'a> = OptionHandleFut<T::HandleArchiveFut<'a>>;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a> {
        OptionHandleFut::<_>::from(self.as_ref().map(|s| s.handle_archive(cx)))
    }
}

impl<T: ArchiveSubscriber> ArchiveSubscriber for Box<T> {
    type HandleArchiveFut<'a> = T::HandleArchiveFut<'a>;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a> {
        <T as ArchiveSubscriber>::handle_archive(self, cx)
    }
}

impl<T: ArchiveSubscriber> ArchiveSubscriber for Arc<T> {
    type HandleArchiveFut<'a> = T::HandleArchiveFut<'a>;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a> {
        <T as ArchiveSubscriber>::handle_archive(self, cx)
    }
}

pub trait ArchiveSubscriberExt: Sized {
    fn chain<T: ArchiveSubscriber>(self, other: T) -> ChainSubscriber<Self, T>;
}

impl<B: ArchiveSubscriber> ArchiveSubscriberExt for B {
    fn chain<T: ArchiveSubscriber>(self, other: T) -> ChainSubscriber<Self, T> {
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
    type Prepared = ();

    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

    #[inline]
    fn prepare_block<'a>(&'a self, _cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    #[inline]
    fn handle_block(
        &self,
        _cx: &BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'_> {
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
    type Prepared = (T1::Prepared, T2::Prepared);

    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        let left = self.left.prepare_block(cx);
        let right = self.right.prepare_block(cx);

        Box::pin(async move {
            match future::join(left, right).await {
                (Ok(l), Ok(r)) => Ok((l, r)),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        })
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        (left_prepared, right_prepared): Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        let left = self.left.handle_block(cx, left_prepared);
        let right = self.right.handle_block(cx, right_prepared);

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

// === (T1, ..., Tn) aka `join` ===

macro_rules! impl_subscriber_tuple {
    ($join_fn:path, |$e:ident| $err_pat:pat, { $($n:tt: $var:ident = $ty:ident),*$(,)? }) => {
        impl<$($ty),*> BlockSubscriber for ($($ty),*)
        where
            $($ty: BlockSubscriber),*
        {
            type Prepared = ($($ty::Prepared),*);

            type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
            type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

            fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
                $(let $var = self.$n.prepare_block(cx));*;

                Box::pin(async move {
                    match $join_fn($($var),*).await {
                        ($(Ok($var)),*) => Ok(($($var),*)),
                        $err_pat => Err($e),
                    }
                })
            }

            fn handle_block<'a>(
                &'a self,
                cx: &'a BlockSubscriberContext,
                ($($var),*): Self::Prepared,
            ) -> Self::HandleBlockFut<'a> {
                $(let $var = self.$n.handle_block(cx, $var));*;

                Box::pin(async move {
                    match $join_fn($($var),*).await {
                        $err_pat => Err($e),
                        _ => Ok(()),
                    }
                })
            }
        }

        impl<$($ty),*> StateSubscriber for ($($ty),*)
        where
            $($ty: StateSubscriber),*
        {
            type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

            fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
                $(let $var = self.$n.handle_state(cx));*;

                Box::pin(async move {
                    match $join_fn($($var),*).await {
                        $err_pat => Err($e),
                        _ => Ok(()),
                    }
                })
            }
        }
    };
}

impl_subscriber_tuple! {
    futures_util::future::join,
    |e| (Err(e), _) | (_, Err(e)),
    {
        0: a = T0,
        1: b = T1,
    }
}

impl_subscriber_tuple! {
    futures_util::future::join3,
    |e| (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)),
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
    }
}

impl_subscriber_tuple! {
    futures_util::future::join4,
    |e| (Err(e), _, _, _) | (_, Err(e), _, _) | (_, _, Err(e), _) | (_, _, _, Err(e)),
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
        3: d = T3,
    }
}

impl_subscriber_tuple! {
    futures_util::future::join5,
    |e|
        (Err(e), _, _, _, _)
        | (_, Err(e), _, _, _)
        | (_, _, Err(e), _, _)
        | (_, _, _, Err(e), _)
        | (_, _, _, _, Err(e)),
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
        3: d = T3,
        4: e = T4,
    }
}

#[cfg(any(test, feature = "test"))]
pub mod test {
    use super::*;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct PrintSubscriber;

    impl BlockSubscriber for PrintSubscriber {
        type Prepared = ();

        type PrepareBlockFut<'a> = future::Ready<Result<()>>;
        type HandleBlockFut<'a> = future::Ready<Result<()>>;

        fn prepare_block<'a>(
            &'a self,
            cx: &'a BlockSubscriberContext,
        ) -> Self::PrepareBlockFut<'a> {
            tracing::info!(
                block_id = %cx.block.id(),
                mc_block_id = %cx.mc_block_id,
                "preparing block"
            );
            future::ready(Ok(()))
        }

        fn handle_block(
            &self,
            cx: &BlockSubscriberContext,
            _: Self::Prepared,
        ) -> Self::HandleBlockFut<'_> {
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

pub async fn find_longest_diffs_tail(mc_block: BlockId, storage: &CoreStorage) -> Result<usize> {
    let mc_block_stuff = load_mc_block_stuff(mc_block, storage).await?;

    let shard_block_handles = load_shard_block_handles(&mc_block_stuff, storage).await?;

    let mut max_tail_len = None;

    for block_handle in shard_block_handles {
        let block = storage
            .block_storage()
            .load_block_data(&block_handle)
            .await?;
        let tail_len = block.block().out_msg_queue_updates.tail_len as usize;

        max_tail_len = Some(max_tail_len.map_or(tail_len, |max: usize| max.max(tail_len)));
    }

    let mc_tail_len = mc_block_stuff.block().out_msg_queue_updates.tail_len as usize;
    let result_tail_len = max_tail_len.map_or(mc_tail_len, |max: usize| mc_tail_len.max(max));

    Ok(result_tail_len)
}

async fn load_mc_block_stuff(mc_seqno: BlockId, storage: &CoreStorage) -> Result<BlockStuff> {
    let mc_handle = storage.block_handle_storage().load_handle(&mc_seqno);
    if let Some(mc_handle) = mc_handle {
        let mc_block_stuff = storage.block_storage().load_block_data(&mc_handle).await?;
        Ok(mc_block_stuff)
    } else {
        anyhow::bail!("mc block handle not found: {mc_seqno}");
    }
}

async fn load_shard_block_handles(
    mc_block_stuff: &BlockStuff,
    storage: &CoreStorage,
) -> Result<Vec<BlockHandle>> {
    let block_handles = storage.block_handle_storage();
    let mut shard_block_handles = Vec::new();

    for entry in mc_block_stuff.load_custom()?.shards.latest_blocks() {
        let block_id = entry?;
        if block_id.seqno == 0 {
            continue;
        }

        let Some(block_handle) = block_handles.load_handle(&block_id) else {
            anyhow::bail!("top shard block handle not found: {block_id}");
        };

        shard_block_handles.push(block_handle);
    }

    Ok(shard_block_handles)
}
