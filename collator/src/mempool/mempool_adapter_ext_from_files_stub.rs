use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::{fs, io};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use everscale_types::boc::Boc;
use everscale_types::cell::{CellBuilder, HashBytes, Load};
use everscale_types::models::{ExtInMsgInfo, IntAddr, MsgInfo, OwnedMessage, StdAddr};
use rand::Rng;
use tycho_block_util::state::ShardStateStuff;

use super::types::{ExternalMessage, MempoolAnchor, MempoolAnchorId};
use crate::mempool::mempool_adapter::{MempoolAdapter, MempoolEventListener};
use crate::tracing_targets;

// FACTORY

pub struct MempoolAdapterExtFilesStubImpl {
    _listener: Arc<dyn MempoolEventListener>,

    _stub_anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
}

impl MempoolAdapterExtFilesStubImpl {
    pub fn new(listener: Arc<dyn MempoolEventListener>) -> Self {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        // TODO: make real implementation, currently runs stub task
        //      that produces the repeating set of anchors
        let stub_anchors_cache = Arc::new(RwLock::new(BTreeMap::new()));

        let mut externals = fs::read_dir("externals")
            .expect("externals dir not found")
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()
            .expect("failed to read externals dir");

        externals.sort();

        tokio::spawn({
            let listener = listener.clone();
            let stub_anchors_cache = stub_anchors_cache.clone();
            async move {
                let mut anchor_id = 0;
                let mut externals_iter = externals.into_iter();
                loop {
                    let rnd_round_interval = rand::thread_rng().gen_range(400..600);
                    tokio::time::sleep(tokio::time::Duration::from_millis(rnd_round_interval * 6))
                        .await;
                    anchor_id += 1;
                    if let Some(external) = externals_iter.next() {
                        let anchor = create_anchor_with_externals_from_file(anchor_id, external);
                        {
                            let mut anchor_cache_rw = stub_anchors_cache
                                .write()
                                .map_err(|e| anyhow!("Poison error on write lock: {:?}", e))
                                .unwrap();
                            tracing::debug!(
                                target: tracing_targets::MEMPOOL_ADAPTER,
                                "Random anchor (id: {}, chain_time: {}, externals: {}) added to cache",
                                anchor.id(),
                                anchor.chain_time(),
                                anchor.externals_count(),
                            );
                            anchor_cache_rw.insert(anchor_id, anchor.clone());
                        }
                        listener.on_new_anchor(anchor).await.unwrap();
                    }
                }
            }
        });
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Stub anchors generator started");

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        Self {
            _listener: listener,
            _stub_anchors_cache: stub_anchors_cache,
        }
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterExtFilesStubImpl {
    async fn enqueue_process_new_mc_block_state(&self, mc_state: ShardStateStuff) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "STUB: New masterchain state (block_id: {}) processing enqueued to mempool",
            mc_state.block_id().as_short_id(),
        );
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        // TODO: make real implementation, currently only return anchor from local cache
        let res = {
            let anchors_cache_r = self
                ._stub_anchors_cache
                .read()
                .map_err(|e| anyhow!("Poison error on read lock: {:?}", e))?;
            anchors_cache_r.get(&anchor_id).cloned()
        };
        if res.is_some() {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Requested anchor (id: {}) found in local cache", anchor_id);
        } else {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "Requested anchor (id: {}) was not found in local cache",
                anchor_id
            );
            tracing::trace!(target: tracing_targets::MEMPOOL_ADAPTER, "STUB: Requesting anchor (id: {}) in mempool...", anchor_id);
            let response_duration = tokio::time::Duration::from_millis(107);
            tokio::time::sleep(response_duration).await;
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "STUB: Requested anchor (id: {}) was not found in mempool (responded in {} ms)",
                anchor_id,
                response_duration.as_millis(),
            );
        }
        Ok(res)
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        // TODO: make real implementation, currently only return anchor from local cache

        let mut stub_first_attempt = true;
        let mut request_timer = std::time::Instant::now();
        loop {
            {
                let anchors_cache_r = self
                    ._stub_anchors_cache
                    .read()
                    .map_err(|e| anyhow!("Poison error on read lock: {:?}", e))?;

                let mut range = anchors_cache_r.range((
                    std::ops::Bound::Excluded(prev_anchor_id),
                    std::ops::Bound::Unbounded,
                ));

                if let Some((next_id, next)) = range.next() {
                    if stub_first_attempt {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "Found in cache next anchor (id: {}) after specified previous (id: {})",
                            next_id,
                            prev_anchor_id,
                        );
                    } else {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "STUB: Returned next anchor (id: {}) after previous (id: {}) from mempool (responded in {} ms)",
                            next_id,
                            prev_anchor_id,
                            request_timer.elapsed().as_millis(),
                        );
                    }
                    return Ok(next.clone());
                } else if stub_first_attempt {
                    tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "There is no next anchor in cache after previous (id: {}). STUB: Requested it from mempool. Waiting...",
                        prev_anchor_id
                    );
                }
            }

            // stub waiting some time until new emulated anchors be added to cache
            if stub_first_attempt {
                request_timer = std::time::Instant::now();
            }
            stub_first_attempt = false;
            tokio::time::sleep(tokio::time::Duration::from_millis(1020)).await;
        }
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache_rw = self
            ._stub_anchors_cache
            .write()
            .map_err(|e| anyhow!("Poison error on write lock: {:?}", e))?;
        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}

pub fn create_anchor_with_externals_from_file(
    anchor_id: MempoolAnchorId,
    external_path: PathBuf,
) -> Arc<MempoolAnchor> {
    let mut externals = vec![];
    let mut file = File::open(&external_path).unwrap();
    let mut buf = vec![];
    file.read_to_end(&mut buf).unwrap();
    let file_name = external_path.file_name().unwrap().to_str().unwrap();
    let timestamp: u64 = file_name.parse().unwrap();
    let mut buffer = vec![];
    BASE64_STANDARD.decode_vec(buf, &mut buffer).unwrap();
    let code = Boc::decode(buffer).expect("failed to decode external boc");
    let message = OwnedMessage::load_from(&mut code.as_slice().unwrap()).unwrap();
    if let MsgInfo::ExtIn(ext_in) = message.info {
        let msg = ExternalMessage::new(code, ext_in);
        externals.push(Arc::new(msg));
    }

    Arc::new(MempoolAnchor::new(anchor_id, timestamp, externals))
}
