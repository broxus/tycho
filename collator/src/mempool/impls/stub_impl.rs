use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::models::*;
use everscale_types::prelude::*;
use humantime::format_duration;
use parking_lot::RwLock;
use rand::Rng;
use scopeguard::defer;
use tycho_network::PeerId;

use crate::mempool::{
    ExternalMessage, MempoolAdapter, MempoolAnchor, MempoolAnchorId, MempoolEventListener,
};
use crate::tracing_targets;

#[derive(Clone)]
pub struct MempoolAdapterStubImpl {
    listener: Arc<dyn MempoolEventListener>,
    anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
}

impl MempoolAdapterStubImpl {
    pub fn with_stub_externals(listener: Arc<dyn MempoolEventListener>) -> Self {
        Self::with_generator(listener, |a| {
            tokio::spawn(a.stub_externals_generator());
            Ok(())
        })
        .unwrap()
    }

    pub fn with_externals_from_dir(
        listener: Arc<dyn MempoolEventListener>,
        dir_path: &Path,
    ) -> Result<Self> {
        Self::with_generator(listener, |a| {
            let mut paths = std::fs::read_dir(dir_path)?
                .map(|res| res.map(|e| e.path()))
                .collect::<Result<Vec<_>, _>>()?;
            paths.sort();

            tokio::spawn(a.file_externals_generator(paths));
            Ok(())
        })
    }

    fn with_generator<F>(listener: Arc<dyn MempoolEventListener>, start: F) -> Result<Self>
    where
        F: FnOnce(Self) -> Result<()>,
    {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "creating mempool adapter");

        let adapter = Self {
            listener,
            anchors_cache: Arc::new(RwLock::new(BTreeMap::new())),
        };

        start(adapter.clone())?;

        Ok(adapter)
    }

    #[tracing::instrument(skip_all)]
    async fn stub_externals_generator(self) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "started");
        defer! {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "finished");
        }

        for anchor_id in 1.. {
            tokio::time::sleep(make_round_interval() * 5).await;

            let anchor = make_stub_anchor(anchor_id);
            self.anchors_cache.write().insert(anchor_id, anchor.clone());

            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                anchor_id = anchor.id,
                chain_time = anchor.chain_time,
                externals = anchor.externals.len(),
                "anchor added to cache",
            );

            self.listener.on_new_anchor(anchor).await.unwrap();
        }
    }

    #[tracing::instrument(skip_all)]
    async fn file_externals_generator(self, paths: Vec<PathBuf>) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "started");
        defer! {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "finished");
        }

        let mut iter = paths.into_iter();

        let mut last_chain_time = 0;
        for anchor_id in 1.. {
            tokio::time::sleep(make_round_interval() * 5).await;

            let anchor = 'anchor: {
                if let Some(path) = iter.next() {
                    match make_anchor_from_file(anchor_id, &path) {
                        Ok(anchor) => break 'anchor anchor,
                        Err(e) => {
                            tracing::error!(
                                target: tracing_targets::MEMPOOL_ADAPTER,
                                anchor_id,
                                path = %path.display(),
                                "failed to make anchor from file: {e:?}"
                            );
                        }
                    }
                }

                make_empty_anchor(anchor_id, last_chain_time + 600)
            };

            last_chain_time = anchor.chain_time;
            self.anchors_cache.write().insert(anchor_id, anchor.clone());

            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                anchor_id = anchor.id,
                chain_time = anchor.chain_time,
                externals = anchor.externals.len(),
                "anchor added to cache",
            );

            self.listener.on_new_anchor(anchor).await.unwrap();
        }
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStubImpl {
    async fn on_new_mc_state(&self, mc_block_id: &BlockId) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "STUB: New masterchain state (block_id: {}) processing enqueued to mempool",
            mc_block_id.as_short_id(),
        );
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        let mut last_attempt_at = None;
        loop {
            let Some(anchor) = self.anchors_cache.read().get(&anchor_id).cloned() else {
                if last_attempt_at.is_none() {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        anchor_id,
                        "There is no required anchor in cache. \
                        STUB: Requested it from mempool. Waiting...",
                    );
                }

                last_attempt_at = Some(Instant::now());
                tokio::time::sleep(tokio::time::Duration::from_millis(1020)).await;
                continue;
            };

            match last_attempt_at {
                Some(last) => {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        anchor_id = anchor.id,
                        elapsed = %format_duration(last.elapsed()),
                        "STUB: Returned the anchor from mempool",
                    );
                }
                None => {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        anchor_id = anchor.id,
                        "Requested the anchor from the local cache",
                    );
                }
            }

            return Ok(Some(anchor));
        }
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        let range = (
            std::ops::Bound::Excluded(prev_anchor_id),
            std::ops::Bound::Unbounded,
        );

        let mut last_attempt_at = None;
        loop {
            let res = self
                .anchors_cache
                .read()
                .range(range)
                .next()
                .map(|(_, v)| v.clone());

            let Some(anchor) = res else {
                if last_attempt_at.is_none() {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        "There is no next anchor in cache. \
                        STUB: Requested it from mempool. Waiting...",
                    );
                }

                last_attempt_at = Some(Instant::now());
                tokio::time::sleep(tokio::time::Duration::from_millis(1020)).await;
                continue;
            };

            match last_attempt_at {
                Some(last) => {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        anchor_id = anchor.id,
                        elapsed = %format_duration(last.elapsed()),
                        "STUB: Returned the next anchor from mempool",
                    );
                }
                None => {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        anchor_id = anchor.id,
                        "Requested the next anchor from the local cache",
                    );
                }
            }

            return Ok(anchor);
        }
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache = self.anchors_cache.write();
        anchors_cache.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}

pub(crate) fn make_empty_anchor(id: MempoolAnchorId, chain_time: u64) -> Arc<MempoolAnchor> {
    Arc::new(MempoolAnchor {
        id,
        author: PeerId(Default::default()),
        chain_time,
        externals: vec![],
    })
}

pub(crate) fn make_stub_anchor(id: MempoolAnchorId) -> Arc<MempoolAnchor> {
    let chain_time = if id == 0 {
        2600
    } else {
        id as u64 * 2136 % 1000000000
    };

    let externals_count = (chain_time % 10) as u32;

    let mut externals = vec![];
    for i in 0..externals_count {
        let body = {
            let mut builder = CellBuilder::new();
            builder.store_u32(id).unwrap();
            builder.store_u64(chain_time).unwrap();
            builder.store_u32(i as u32).unwrap();
            builder.build().unwrap()
        };

        let info = ExtInMsgInfo {
            dst: IntAddr::Std(StdAddr::new(
                if i > 0 && i % 3 == 0 { -1 } else { 0 },
                rand::random::<HashBytes>(),
            )),
            ..Default::default()
        };

        let cell = CellBuilder::build_from(Message {
            info: MsgInfo::ExtIn(info.clone()),
            init: None,
            body: body.as_slice().unwrap(),
            layout: None,
        })
        .unwrap();

        externals.push(Arc::new(ExternalMessage { cell, info }));
    }

    Arc::new(MempoolAnchor {
        id,
        author: PeerId(Default::default()),
        chain_time,
        externals,
    })
}

pub(crate) fn make_anchor_from_file(
    id: MempoolAnchorId,
    path: &Path,
) -> Result<Arc<MempoolAnchor>> {
    let data = std::fs::read_to_string(path)?;

    let file_name = path.file_name().unwrap().to_str().unwrap();
    tracing::debug!(
        target: tracing_targets::MEMPOOL_ADAPTER,
        file_name,
        "read external from file"
    );

    let chain_time = file_name.parse().unwrap();

    let cell = Boc::decode_base64(data)?;
    let message: Message<'_> = cell.parse()?;

    let mut externals = vec![];
    if let MsgInfo::ExtIn(info) = message.info {
        externals.push(Arc::new(ExternalMessage { cell, info }));
    }

    Ok(Arc::new(MempoolAnchor {
        id,
        author: PeerId(Default::default()),
        chain_time,
        externals,
    }))
}

fn make_round_interval() -> Duration {
    Duration::from_millis(rand::thread_rng().gen_range(400..600))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MempoolEventStubListener;
    #[async_trait]
    impl MempoolEventListener for MempoolEventStubListener {
        async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()> {
            tracing::trace!(
                "MempoolEventStubListener: on_new_anchor event emitted for anchor \
                (id: {}, chain_time: {}, externals: {})",
                anchor.id,
                anchor.chain_time,
                anchor.externals.len(),
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_stub_anchors_generator() -> Result<()> {
        tycho_util::test::init_logger("test_stub_anchors_generator", "trace");

        let adapter =
            MempoolAdapterStubImpl::with_stub_externals(Arc::new(MempoolEventStubListener));

        // try get not existing anchor by id
        let opt_anchor = adapter.get_anchor_by_id(10).await?;
        assert!(opt_anchor.is_none());

        // try get existing anchor by id (after sleep)
        tokio::time::sleep(tokio::time::Duration::from_millis(600 * 6 * 4)).await;
        let opt_anchor = adapter.get_anchor_by_id(3).await?;
        assert!(opt_anchor.is_some());
        assert_eq!(opt_anchor.unwrap().id, 3);

        // try get next anchor after (id: 3)
        let anchor = adapter.get_next_anchor(3).await?;
        assert_eq!(anchor.id, 4);

        // try get next anchor after (id: 8), will wait some time
        let anchor = adapter.get_next_anchor(8).await?;
        assert_eq!(anchor.id, 9);

        // test clear anchors cache
        adapter.clear_anchors_cache(7).await?;
        let opt_anchor = adapter.get_anchor_by_id(3).await?;
        assert!(opt_anchor.is_none());
        let opt_anchor = adapter.get_anchor_by_id(6).await?;
        assert!(opt_anchor.is_none());
        let opt_anchor = adapter.get_anchor_by_id(7).await?;
        assert!(opt_anchor.is_some());
        assert_eq!(opt_anchor.unwrap().id, 7);

        Ok(())
    }
}
