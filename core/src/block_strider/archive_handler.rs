use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use tycho_storage::Storage;
use tycho_util::metrics::HistogramGuard;

use crate::block_strider::{
    ArchiveSubscriber, ArchiveSubscriberContext, BlockSubscriber, BlockSubscriberContext,
};

#[repr(transparent)]
pub struct ArchiveHandler<S> {
    inner: Arc<Inner<S>>,
}

impl<S> ArchiveHandler<S>
where
    S: ArchiveSubscriber,
{
    pub fn new(storage: Storage, archive_subscriber: S) -> Result<Self> {
        let listener = storage.block_storage().archive_listener();

        Ok(Self {
            inner: Arc::new(Inner {
                storage,
                archive_subscriber,
                archive_listener: ArchiveListener::new(listener),
            }),
        })
    }

    async fn handle_block_impl(&self, _cx: &BlockSubscriberContext, _prepared: ()) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_core_archive_handler_handle_block_time");

        // Process archive
        if let Some(archive_id) = self.inner.archive_listener.next_update().await {
            let _histogram = HistogramGuard::begin("tycho_core_subscriber_handle_archive_time");

            let cx = ArchiveSubscriberContext {
                archive_id,
                storage: &self.inner.storage,
            };

            tracing::info!(id = cx.archive_id, "handling archive");

            let guard = self.inner.archive_listener.lock().await;
            self.inner.archive_subscriber.handle_archive(&cx).await?;
            drop(guard);
        }

        // Done
        Ok(())
    }
}

impl<S> Clone for ArchiveHandler<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> BlockSubscriber for ArchiveHandler<S>
where
    S: ArchiveSubscriber,
{
    type Prepared = ();

    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    #[inline]
    fn prepare_block<'a>(&'a self, _cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(self.handle_block_impl(cx, prepared))
    }
}

pub struct ArchiveHandlerPrepared {}

struct Inner<S> {
    storage: Storage,
    archive_subscriber: S,
    archive_listener: ArchiveListener,
}

struct ArchiveListener {
    current_id: AtomicU32,
    listener: Arc<tokio::sync::Mutex<Option<u32>>>,
}

impl ArchiveListener {
    fn new(listener: Arc<tokio::sync::Mutex<Option<u32>>>) -> Self {
        Self {
            listener,
            current_id: Default::default(),
        }
    }

    async fn next_update(&self) -> Option<u32> {
        let id = self.listener.lock().await;

        if let Some(id) = id.as_ref() {
            if *id != self.current_id.load(Ordering::Acquire) {
                self.current_id.store(*id, Ordering::Release);
                return Some(*id);
            }
        }

        None
    }

    async fn lock(&self) -> tokio::sync::MutexGuard<'_, Option<u32>> {
        self.listener.lock().await
    }
}
