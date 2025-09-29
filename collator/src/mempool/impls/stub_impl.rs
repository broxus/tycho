use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use bumpalo::Bump;
use humantime::format_duration;
use parking_lot::RwLock;
use rand::Rng;
use scopeguard::defer;
use tycho_network::PeerId;
use tycho_types::models::*;
use tycho_types::prelude::*;

use crate::mempool::{
    DebugStateUpdateContext, ExternalMessage, GetAnchorResult, MempoolAdapter, MempoolAnchor,
    MempoolAnchorId, MempoolEventListener, StateUpdateContext,
};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

pub struct MempoolAdapterStubImpl {
    listener: Arc<dyn MempoolEventListener>,
    anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    sleep_between_anchors: AtomicBool,
    first_anchor_id: Option<u32>,
}

impl MempoolAdapterStubImpl {
    pub fn with_stub_externals(
        listener: Arc<dyn MempoolEventListener>,
        now: Option<u64>,
        first_anchor_id: Option<u32>,
    ) -> Arc<Self> {
        Self::with_generator(listener, first_anchor_id, |a| {
            tokio::spawn(Self::stub_externals_generator(a, now));
            Ok(())
        })
        .unwrap()
    }

    pub fn with_externals_from_dir(
        listener: Arc<dyn MempoolEventListener>,
        dir_path: impl AsRef<Path>,
        first_anchor_id: Option<u32>,
    ) -> Result<Arc<Self>> {
        Self::with_generator(listener, first_anchor_id, move |a| {
            let mut paths = std::fs::read_dir(dir_path)?
                .map(|res| res.map(|e| e.path()))
                .collect::<Result<Vec<_>, _>>()?;
            paths.sort();

            tokio::spawn(Self::file_externals_generator(a, paths));
            Ok(())
        })
    }

    fn with_generator<F>(
        listener: Arc<dyn MempoolEventListener>,
        first_anchor_id: Option<u32>,
        start: F,
    ) -> Result<Arc<Self>>
    where
        F: FnOnce(Arc<Self>) -> Result<()>,
    {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "creating mempool adapter");

        let adapter = Self {
            listener,
            anchors_cache: Arc::new(RwLock::new(BTreeMap::new())),
            sleep_between_anchors: AtomicBool::new(true),
            first_anchor_id,
        };

        let adapter = Arc::new(adapter);

        start(adapter.clone())?;

        Ok(adapter)
    }

    pub fn with_anchors_from_dump(
        listener: Arc<dyn MempoolEventListener>,
        first_anchor_id: u32,
        anchors_path: PathBuf,
    ) -> Result<Arc<Self>> {
        Self::with_generator(listener.clone(), Some(first_anchor_id), {
            move |a| {
                tokio::spawn(Self::anchors_generator(a, anchors_path));
                Ok(())
            }
        })
    }

    #[tracing::instrument(skip_all)]
    async fn stub_externals_generator(self: Arc<Self>, now: Option<u64>) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "started");
        defer! {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "finished");
        }

        let mut prev_anchor_id = self.first_anchor_id.unwrap_or_default();
        let start_anchor_id = prev_anchor_id + 1;
        for anchor_id in start_anchor_id.. {
            if self.sleep_between_anchors.load(Ordering::Acquire) {
                tokio::time::sleep(make_round_interval() * 4).await;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let mut anchor = make_stub_anchor(anchor_id, prev_anchor_id, self.first_anchor_id);
            prev_anchor_id = anchor_id;

            if let Some(now) = now {
                anchor.chain_time += now;
            }

            let anchor = Arc::new(anchor);

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
    async fn anchors_generator(self: Arc<Self>, anchors_path: PathBuf) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "started");
        defer! {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "finished");
        }

        let mut file_map: BTreeMap<MempoolAnchorId, PathBuf> = BTreeMap::new();
        if anchors_path.exists()
            && let Ok(entries) = std::fs::read_dir(&anchors_path)
        {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if let Some(anchor_id) = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_prefix("anchor_"))
                    .and_then(|f| f.split('_').next())
                    && let Ok(anchor_id) = anchor_id.parse::<MempoolAnchorId>()
                {
                    file_map.insert(anchor_id, path);
                }
            }
        }

        let mut prev_anchor_id = self.first_anchor_id.unwrap_or_default();
        let start_anchor_id = prev_anchor_id + 1;
        for anchor_id in start_anchor_id.. {
            if self.sleep_between_anchors.load(Ordering::Acquire) {
                tokio::time::sleep(make_round_interval() * 4).await;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let anchor = if let Some(filepath) = file_map.get(&anchor_id) {
                let filename = filepath.file_name().unwrap().to_str().unwrap();
                let parts: Vec<&str> = filename.split('_').collect();
                let chain_time: u64 = parts[2].parse().unwrap();
                let point_bytes = std::fs::read(filepath).unwrap();
                self.load_anchor_from_file(anchor_id, chain_time, point_bytes, prev_anchor_id)
                    .await
                    .ok()
            } else {
                None
            };

            let anchor = anchor.unwrap_or_else(|| {
                let read = self.anchors_cache.read();
                let chain_time = read
                    .get(&prev_anchor_id)
                    .map(|prev_anchor| prev_anchor.chain_time)
                    .unwrap_or_default();
                make_empty_anchor(anchor_id, prev_anchor_id, chain_time + 1336)
            });

            prev_anchor_id = anchor_id;

            self.anchors_cache.write().insert(anchor_id, anchor.clone());

            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                anchor_id = anchor.id,
                chain_time = anchor.chain_time,
                externals = anchor.externals.len(),
                "anchor added to cache from file",
            );

            self.listener.on_new_anchor(anchor).await.unwrap();
        }
    }

    async fn load_anchor_from_file(
        &self,
        anchor_id: MempoolAnchorId,
        chain_time: u64,
        point_bytes: Vec<u8>,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<Arc<MempoolAnchor>> {
        use tycho_consensus::prelude::Point;
        let bump = Bump::new();
        let payload = Point::read_payload_from_tl_bytes(&point_bytes, &bump).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse payload for anchor {} with error: {e}",
                anchor_id
            )
        })?;

        let mut externals = Vec::new();
        for msg_bytes in payload {
            if let Ok(cell) = Boc::decode_base64(msg_bytes) {
                let message: Message<'_> = cell.parse()?;
                if let MsgInfo::ExtIn(info) = message.info {
                    externals.push(Arc::new(ExternalMessage { cell, info }));
                }
            }
        }

        Ok(Arc::new(MempoolAnchor {
            id: anchor_id,
            prev_id: Some(prev_anchor_id),
            author: PeerId(Default::default()),
            chain_time,
            externals,
        }))
    }

    #[tracing::instrument(skip_all)]
    async fn file_externals_generator(self: Arc<Self>, paths: Vec<PathBuf>) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "started");
        defer! {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "finished");
        }

        let mut iter = paths.into_iter();

        let mut last_chain_time = 0;
        let mut prev_anchor_id = 0;
        for anchor_id in 1.. {
            if self.sleep_between_anchors.load(Ordering::Acquire) {
                tokio::time::sleep(make_round_interval() * 4).await;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let anchor = 'anchor: {
                if let Some(path) = iter.next() {
                    match make_anchor_from_file(anchor_id, prev_anchor_id, &path) {
                        Ok(anchor) => {
                            prev_anchor_id = anchor_id;
                            break 'anchor anchor;
                        }
                        Err(e) => {
                            tracing::error!(
                                target: tracing_targets::MEMPOOL_ADAPTER,
                                anchor_id,
                                prev_anchor_id,
                                path = %path.display(),
                                "failed to make anchor from file: {e:?}"
                            );
                        }
                    }
                }

                make_empty_anchor(anchor_id, prev_anchor_id, last_chain_time + 1336)
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
    async fn handle_mc_state_update(&self, cx: StateUpdateContext) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "STUB: Processing state update from mc block {}: {:?}",
            cx.mc_block_id.as_short_id(), DebugStateUpdateContext(&cx),
        );
        Ok(())
    }

    async fn handle_signed_mc_block(&self, _mc_block_seqno: BlockSeqno) -> Result<()> {
        Ok(())
    }

    async fn get_anchor_by_id(&self, anchor_id: MempoolAnchorId) -> Result<GetAnchorResult> {
        let mut last_attempt_at = None;
        loop {
            let Some(anchor) = self.anchors_cache.read().get(&anchor_id).cloned() else {
                let last_anchor_id = self.anchors_cache.read().last_key_value().map_or(
                    self.first_anchor_id.unwrap_or_default(),
                    |(_, last_anchor)| last_anchor.id,
                );
                if last_anchor_id > anchor_id {
                    return Ok(GetAnchorResult::NotExist);
                } else {
                    let delta = anchor_id.saturating_sub(last_anchor_id);
                    if delta > 20 {
                        self.sleep_between_anchors.store(false, Ordering::Release);
                        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                            "sleep_between_anchors set to False because anchor_id {} ahead last {} on {} > 20",
                            anchor_id, last_anchor_id, delta,
                        );
                        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                            "STUB: mempool return None because requested anchor_id {} ahead last {} on {} > 20",
                            anchor_id, last_anchor_id, delta,
                        );
                        return Ok(GetAnchorResult::NotExist);
                    } else if delta > 3 {
                        self.sleep_between_anchors.store(false, Ordering::Release);
                        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                            "sleep_between_anchors set to False because anchor_id {} ahead last {} on {} > 3",
                            anchor_id, last_anchor_id, delta,
                        );
                    }
                }

                if last_attempt_at.is_none() {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        anchor_id,
                        "There is no required anchor in cache. \
                        STUB: Requested it from mempool. Waiting...",
                    );
                }

                last_attempt_at = Some(Instant::now());
                tokio::time::sleep(tokio::time::Duration::from_millis(1320)).await;
                continue;
            };

            if !self.sleep_between_anchors.fetch_or(true, Ordering::AcqRel) {
                tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                    "sleep_between_anchors set to True when requested was returned by anchor_id {}",
                    anchor_id,
                );
            }

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

            return Ok(GetAnchorResult::Exist(anchor));
        }
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<GetAnchorResult> {
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
                let last_anchor_id = self.anchors_cache.read().last_key_value().map_or(
                    self.first_anchor_id.unwrap_or_default(),
                    |(_, last_anchor)| last_anchor.id,
                );
                let delta = prev_anchor_id.saturating_sub(last_anchor_id);
                if delta >= 20 {
                    tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                        "sleep_between_anchors set to False because prev_anchor_id {} ahead last {} on {} >= 20",
                        prev_anchor_id, last_anchor_id, delta,
                    );
                    tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                        "STUB: mempool return None because prev_anchor_id {} ahead last {} on {} >= 20",
                        prev_anchor_id, last_anchor_id, delta,
                    );
                    return Ok(GetAnchorResult::NotExist);
                } else if delta >= 3 {
                    self.sleep_between_anchors.store(false, Ordering::Release);
                    tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                        "sleep_between_anchors set to False because prev_anchor_id {} ahead last {} on {} >= 3",
                        prev_anchor_id, last_anchor_id, delta,
                    );
                }

                if last_attempt_at.is_none() {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        prev_anchor_id,
                        "There is no next anchor in cache. \
                        STUB: Requested it from mempool. Waiting...",
                    );
                }

                last_attempt_at = Some(Instant::now());
                tokio::time::sleep(tokio::time::Duration::from_millis(1320)).await;
                continue;
            };

            if !self.sleep_between_anchors.fetch_or(true, Ordering::AcqRel) {
                tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
                    "sleep_between_anchors set to True when next was returned after prev_anchor_id {}",
                    prev_anchor_id,
                );
            }

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

            return Ok(GetAnchorResult::Exist(anchor));
        }
    }

    fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache = self.anchors_cache.write();
        anchors_cache.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }

    fn accept_external(&self, _message: bytes::Bytes) {
        panic!("accept_external not implemented for stub");
    }

    async fn update_delayed_config(
        &self,
        _consensus_config: Option<&ConsensusConfig>,
        _genesis_info: &GenesisInfo,
    ) -> Result<()> {
        panic!("update_delayed_config not implemented for stub");
    }
}

pub(crate) fn make_empty_anchor(
    id: MempoolAnchorId,
    prev_id: MempoolAnchorId,
    chain_time: u64,
) -> Arc<MempoolAnchor> {
    Arc::new(MempoolAnchor {
        id,
        prev_id: Some(prev_id),
        author: PeerId(Default::default()),
        chain_time,
        externals: vec![],
    })
}

pub(crate) fn make_stub_anchor(
    id: MempoolAnchorId,
    prev_id: MempoolAnchorId,
    anchor_id_offset: Option<u32>,
) -> MempoolAnchor {
    let chain_time = (id - anchor_id_offset.unwrap_or_default()) as u64 * 1736 % 1000000000;

    let externals_count = (chain_time % 10) as u32;

    let mut externals = vec![];
    for i in 0..externals_count {
        let addr_hash_base = i % 6 + 1;
        let dst = IntAddr::Std(StdAddr::new(
            if i > 0 && i % 3 == 0 { -1 } else { 0 },
            HashBytes([addr_hash_base.try_into().unwrap(); 32]),
        ));
        externals.push(Arc::new(make_stub_external(id, chain_time, i, dst)));
    }

    MempoolAnchor {
        id,
        prev_id: Some(prev_id),
        author: PeerId(Default::default()),
        chain_time,
        externals,
    }
}

pub(crate) fn make_stub_external(
    anchor_id: MempoolAnchorId,
    chain_time: u64,
    msg_idx: u32,
    dst: IntAddr,
) -> ExternalMessage {
    let body = {
        let mut builder = CellBuilder::new();
        builder.store_u32(anchor_id).unwrap();
        builder.store_u64(chain_time).unwrap();
        builder.store_u32(msg_idx).unwrap();
        builder.build().unwrap()
    };

    let info = ExtInMsgInfo {
        dst,
        ..Default::default()
    };

    let cell = CellBuilder::build_from(Message {
        info: MsgInfo::ExtIn(info.clone()),
        init: None,
        body: body.as_slice().unwrap(),
        layout: None,
    })
    .unwrap();

    ExternalMessage { cell, info }
}

pub(crate) fn make_anchor_from_file(
    id: MempoolAnchorId,
    prev_id: MempoolAnchorId,
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
        prev_id: Some(prev_id),
        author: PeerId(Default::default()),
        chain_time,
        externals,
    }))
}

fn make_round_interval() -> Duration {
    Duration::from_millis(rand::rng().random_range(240..340))
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

        let adapter = MempoolAdapterStubImpl::with_stub_externals(
            Arc::new(MempoolEventStubListener),
            None,
            None,
        );

        // try get existing anchor by id
        let result = adapter.get_anchor_by_id(3).await?;
        assert!(result.anchor().is_some());
        assert_eq!(result.anchor().unwrap().id, 3);

        // try get next anchor after (id: 3)
        let result = adapter.get_next_anchor(3).await?;
        assert!(result.anchor().is_some());
        assert_eq!(result.anchor().unwrap().id, 4);

        // try get next anchor after (id: 5), will wait some time
        let result = adapter.get_next_anchor(5).await?;
        assert!(result.anchor().is_some());
        assert_eq!(result.anchor().unwrap().id, 6);

        // test clear anchors cache
        adapter.clear_anchors_cache(6)?;
        let result = adapter.get_anchor_by_id(3).await?;
        assert!(result.anchor().is_none());
        let result = adapter.get_anchor_by_id(4).await?;
        assert!(result.anchor().is_none());
        let result = adapter.get_anchor_by_id(6).await?;
        assert!(result.anchor().is_some());
        assert_eq!(result.anchor().unwrap().id, 6);

        Ok(())
    }
}
