use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use everscale_types::models::BlockId;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::BlockProofStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::{BlockHandle, BriefBlockInfo};
use tycho_util::futures::JoinTask;

use crate::node::Node;

/// Boot type when the node has not yet started syncing
///
/// Returns last masterchain key block id
pub async fn cold_boot(
    node: &Arc<Node>,
    zerostates: Option<Vec<PathBuf>>,
) -> anyhow::Result<BlockId> {
    tracing::info!("starting cold boot");

    // Find the last key block (or zerostate) from which we can start downloading other key blocks
    let prev_key_block = prepare_prev_key_block(node, zerostates).await?;

    // Ensure that all key blocks until now (with some offset) are downloaded
    download_key_blocks(node, prev_key_block).await?;

    todo!()
}

async fn prepare_prev_key_block(
    node: &Arc<Node>,
    zerostates: Option<Vec<PathBuf>>,
) -> anyhow::Result<PrevKeyBlock> {
    let block_id = node
        .storage
        .node_state()
        .load_init_mc_block_id()
        .unwrap_or(node.zerostate.as_block_id());

    tracing::info!(block_id = %block_id, "using key block");

    let prev_key_block = match block_id.seqno {
        0 => {
            tracing::info!(%block_id, "using zero state");

            let (handle, state) = match zerostates {
                Some(zerostates) => node.import_zerostates(zerostates).await?,
                None => node.download_zerostates().await?,
            };

            PrevKeyBlock::ZeroState {
                handle: Arc::new(handle),
                state: Arc::new(state),
            }
        }
        _ => {
            tracing::info!(%block_id, "using key block");

            let handle = node
                .storage
                .block_handle_storage()
                .load_handle(&block_id)
                .expect("shouldn't happen");

            let proof = node
                .storage
                .block_storage()
                .load_block_proof(&handle, false)
                .await?;

            PrevKeyBlock::KeyBlock {
                handle: Arc::new(handle),
                proof: Box::new(proof),
            }
        }
    };

    Ok(prev_key_block)
}

async fn download_key_blocks(
    node: &Arc<Node>,
    mut prev_key_block: PrevKeyBlock,
) -> anyhow::Result<()> {
    const BLOCKS_PER_BATCH: u32 = 10;
    const PARALLEL_REQUESTS: usize = 10;

    let (ids_tx, mut ids_rx) = mpsc::unbounded_channel();
    let (tasks_tx, mut tasks_rx) = mpsc::unbounded_channel();

    tokio::spawn({
        let blockchain_rpc_client = node.blockchain_rpc_client.clone();

        async move {
            while let Some(block_id) = tasks_rx.recv().await {
                // TODO: add retry count to interrupt infinite loop
                'inner: loop {
                    tracing::debug!(%block_id, "start downloading next key blocks");

                    let res = blockchain_rpc_client
                        .get_next_key_block_ids(&block_id, BLOCKS_PER_BATCH)
                        .await;

                    match res {
                        Ok(res) => {
                            let (handle, data) = res.split();
                            handle.accept();

                            if data.incomplete {
                                tracing::debug!(%block_id, "stop downloading next key blocks");
                                return;
                            }

                            if ids_tx.send(data.block_ids).is_err() {
                                return;
                            }

                            break 'inner;
                        }
                        Err(e) => {
                            tracing::warn!(%block_id, "failed to download key block ids: {e:?}");
                        }
                    }
                }
            }
        }
    });

    // Start getting next key blocks
    tasks_tx.send(*prev_key_block.handle().id())?;

    while let Some(ids) = ids_rx.recv().await {
        let stream = futures_util::stream::iter(ids)
            .map(|block_id| {
                let storage = node.storage.clone();
                let blockchain_rpc_client = node.blockchain_rpc_client.clone();

                JoinTask::new(async move {
                    let block_storage = storage.block_storage();
                    let block_handle_storage = storage.block_handle_storage();

                    // TODO: add retry count to interrupt infinite loop
                    loop {
                        // Check whether block proof is already stored locally
                        if let Some(handle) = block_handle_storage.load_handle(&block_id) {
                            if let Ok(proof) = block_storage.load_block_proof(&handle, false).await {
                                return WithArchiveData::loaded(proof);
                            }
                        }

                        let res = blockchain_rpc_client
                            .get_key_block_proof(&block_id)
                            .await;

                        match res {
                            Ok(res) => {
                                let (handle, data) = res.split();

                                match BlockProofStuff::deserialize(block_id, &data.data, false) {
                                    Ok(proof) => {
                                        handle.accept();
                                        return proof.with_archive_data(&data.data);
                                    },
                                    Err(e) => {
                                        tracing::error!(%block_id, "failed to deserialize block proof: {e}");
                                        handle.reject();
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(%block_id, "failed to download block proof: {e:?}");
                            }
                        }
                    }
                })
            })
            .buffered(PARALLEL_REQUESTS);

        let mut proofs = stream.collect::<Vec<_>>().await;
        proofs.sort_by_key(|x| *x.id());

        // Save previous key block to restart downloading in case of error
        let prev_key_block_id = *prev_key_block.handle().id();

        let proofs_len = proofs.len();
        for (index, proof) in proofs.into_iter().enumerate() {
            // Verify block proof
            match prev_key_block.check_next_proof(&proof.data) {
                Ok(info) => {
                    // Save block proof
                    let block_id = proof.id();

                    let handle = node
                        .storage
                        .block_storage()
                        .store_block_proof(&proof, info.with_mc_seq_no(block_id.seqno).into())
                        .await?
                        .handle;

                    let block_utime = handle.meta().gen_utime();
                    let prev_utime = prev_key_block.handle().meta().gen_utime();

                    // Update init_mc_block_id
                    if tycho_block_util::state::is_persistent_state(block_utime, prev_utime) {
                        node.storage
                            .node_state()
                            .store_init_mc_block_id(handle.id());
                    }

                    // Trigger task to getting next key blocks
                    if index == proofs_len.saturating_sub(1) {
                        tasks_tx.send(*proof.data.id())?;
                    }

                    // Update prev_key_block
                    prev_key_block = PrevKeyBlock::KeyBlock {
                        handle: Arc::new(handle),
                        proof: Box::new(proof.data),
                    };
                }
                Err(e) => {
                    tracing::warn!("got invalid key block proof: {e:?}");

                    // Restart downloading proofs
                    tasks_tx.send(prev_key_block_id)?;

                    break;
                }
            }
        }
    }

    Ok(())
}

enum PrevKeyBlock {
    ZeroState {
        handle: Arc<BlockHandle>,
        state: Arc<ShardStateStuff>,
    },
    KeyBlock {
        handle: Arc<BlockHandle>,
        proof: Box<BlockProofStuff>,
    },
}

impl PrevKeyBlock {
    fn handle(&self) -> &Arc<BlockHandle> {
        match self {
            Self::KeyBlock { handle, .. } | Self::ZeroState { handle, .. } => handle,
        }
    }

    fn check_next_proof(&self, next_proof: &BlockProofStuff) -> anyhow::Result<BriefBlockInfo> {
        let (virt_block, virt_block_info) = next_proof
            .pre_check_block_proof()
            .context("Failed to pre check block proof")?;

        let res = BriefBlockInfo::from(&virt_block_info);

        match self {
            // Check block proof with zero state
            PrevKeyBlock::ZeroState { state, .. } => {
                tycho_block_util::block::check_with_master_state(
                    next_proof,
                    state,
                    &virt_block,
                    &virt_block_info,
                )
            }
            // Check block proof with previous key block
            PrevKeyBlock::KeyBlock { proof, .. } => {
                tycho_block_util::block::check_with_prev_key_block_proof(
                    next_proof,
                    proof,
                    &virt_block,
                    &virt_block_info,
                )
            }
        }
        .map(move |_| res)
    }
}
