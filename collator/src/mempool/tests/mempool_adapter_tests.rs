use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::{mempool::MempoolAnchor, test_utils::try_init_test_tracing};

use super::{MempoolAdapter, MempoolAdapterStubImpl, MempoolEventListener};

struct MempoolEventStubListener;
#[async_trait]
impl MempoolEventListener for MempoolEventStubListener {
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
        tracing::trace!(
            "MempoolEventStubListener: on_new_anchor event emitted for anchor (id: {}, chain_time: {}, externals: {})",
            anchor.id(),
            anchor.chain_time(),
            anchor.externals_count(),
        );
        Ok(())
    }
}

#[tokio::test]
async fn test_stub_anchors_generator() -> Result<()> {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let adapter = MempoolAdapterStubImpl::new(Arc::new(MempoolEventStubListener {}));

    // try get not existing anchor by id
    let opt_anchor = adapter.get_anchor_by_id(10).await?;
    assert!(opt_anchor.is_none());

    // try get existing anchor by id (after sleep)
    tokio::time::sleep(tokio::time::Duration::from_millis(600 * 6 * 4)).await;
    let opt_anchor = adapter.get_anchor_by_id(3).await?;
    assert!(opt_anchor.is_some());
    assert_eq!(opt_anchor.unwrap().id(), 3);

    // try get next anchor after (id: 3)
    let anchor = adapter.get_next_anchor(3).await?;
    assert_eq!(anchor.id(), 4);

    // try get next anchor after (id: 8), will wait some time
    let anchor = adapter.get_next_anchor(8).await?;
    assert_eq!(anchor.id(), 9);

    // test clear anchors cache
    adapter.clear_anchors_cache(7).await?;
    let opt_anchor = adapter.get_anchor_by_id(3).await?;
    assert!(opt_anchor.is_none());
    let opt_anchor = adapter.get_anchor_by_id(6).await?;
    assert!(opt_anchor.is_none());
    let opt_anchor = adapter.get_anchor_by_id(7).await?;
    assert!(opt_anchor.is_some());
    assert_eq!(opt_anchor.unwrap().id(), 7);

    Ok(())
}
