use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::broadcast;
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
        let rx = storage.block_storage().subscribe_to_archive_ids();

        Ok(Self {
            inner: Arc::new(Inner {
                storage,
                archive_subscriber,
                archive_listener: ArchiveListener::new(rx),
            }),
        })
    }

    async fn handle_block_impl(&self, _cx: &BlockSubscriberContext, _prepared: ()) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_core_archive_handler_handle_block_time");

        // Process all available archive
        loop {
            let archive_id = {
                let mut rx = self.inner.archive_listener.rx.lock();
                match rx.try_recv() {
                    Ok(id) => id,
                    Err(_) => break,
                }
            };

            let _histogram = HistogramGuard::begin("tycho_core_subscriber_handle_archive_time");

            let cx = ArchiveSubscriberContext {
                archive_id,
                storage: &self.inner.storage,
            };

            tracing::info!(id = cx.archive_id, "handling archive");
            self.inner.archive_subscriber.handle_archive(&cx).await?;
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

struct Inner<S> {
    storage: Storage,
    archive_subscriber: S,
    archive_listener: ArchiveListener,
}

struct ArchiveListener {
    rx: Mutex<broadcast::Receiver<u32>>,
}

impl ArchiveListener {
    fn new(rx: broadcast::Receiver<u32>) -> Self {
        Self { rx: Mutex::new(rx) }
    }
}
