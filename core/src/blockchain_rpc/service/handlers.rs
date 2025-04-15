use std::num::{NonZeroU32, NonZeroU64};

use anyhow::Context;
use bytes::Bytes;
use everscale_types::models::BlockId;
use tycho_storage::{ArchiveId, BlockConnection, KeyBlocksDirection, PersistentStateKind};
use tycho_util::metrics::HistogramGuard;

use super::{Inner, RPC_METHOD_TIMINGS_METRIC};
use crate::blockchain_rpc::{BAD_REQUEST_ERROR_CODE, INTERNAL_ERROR_CODE, NOT_FOUND_ERROR_CODE};
use crate::proto::blockchain::{
    rpc, ArchiveInfo, BlockData, BlockFull, Data, KeyBlockIds, KeyBlockProof, PersistentStateInfo,
};
use crate::proto::overlay;

impl<B> Inner<B> {
    pub(super) fn handle_get_next_key_block_ids(
        &self,
        req: &rpc::GetNextKeyBlockIds,
    ) -> overlay::Response<KeyBlockIds> {
        let block_handle_storage = self.storage().block_handle_storage();

        let limit = std::cmp::min(req.max_size as usize, self.config.max_key_blocks_list_len);

        let get_next_key_block_ids = || {
            if !req.block_id.shard.is_masterchain() {
                anyhow::bail!("first block id is not from masterchain");
            }

            let mut iterator = block_handle_storage
                .key_blocks_iterator(KeyBlocksDirection::ForwardFrom(req.block_id.seqno))
                .take(limit + 1);

            if let Some(id) = iterator.next() {
                anyhow::ensure!(
                    id.root_hash == req.block_id.root_hash,
                    "first block root hash mismatch"
                );
                anyhow::ensure!(
                    id.file_hash == req.block_id.file_hash,
                    "first block file hash mismatch"
                );
            }

            Ok::<_, anyhow::Error>(iterator.take(limit).collect::<Vec<_>>())
        };

        match get_next_key_block_ids() {
            Ok(ids) => {
                let incomplete = ids.len() < limit;
                overlay::Response::Ok(KeyBlockIds {
                    block_ids: ids,
                    incomplete,
                })
            }
            Err(e) => {
                tracing::warn!("get_next_key_block_ids failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) async fn handle_get_block_full(
        &self,
        req: &rpc::GetBlockFull,
    ) -> overlay::Response<BlockFull> {
        match self.get_block_full(&req.block_id).await {
            Ok(block_full) => overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_block_full failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) async fn handle_get_next_block_full(
        &self,
        req: &rpc::GetNextBlockFull,
    ) -> overlay::Response<BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_connection_storage = self.storage().block_connection_storage();

        let get_next_block_full = async {
            let next_block_id = match block_handle_storage.load_handle(&req.prev_block_id) {
                Some(handle) if handle.has_next1() => block_connection_storage
                    .load_connection(&req.prev_block_id, BlockConnection::Next1)
                    .context("connection not found")?,
                _ => return Ok(BlockFull::NotFound),
            };

            self.get_block_full(&next_block_id).await
        };

        match get_next_block_full.await {
            Ok(block_full) => overlay::Response::Ok(block_full),
            Err(e) => {
                tracing::warn!("get_next_block_full failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) fn handle_get_block_data_chunk(
        &self,
        req: &rpc::GetBlockDataChunk,
    ) -> overlay::Response<Data> {
        let block_storage = self.storage.block_storage();
        match block_storage.get_block_data_chunk(&req.block_id, req.offset) {
            Ok(Some(data)) => overlay::Response::Ok(Data {
                data: Bytes::from_owner(data),
            }),
            Ok(None) => overlay::Response::Err(NOT_FOUND_ERROR_CODE),
            Err(e) => {
                tracing::warn!("get_block_data_chunk failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) async fn handle_get_key_block_proof(
        &self,
        req: &rpc::GetKeyBlockProof,
    ) -> overlay::Response<KeyBlockProof> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let get_key_block_proof = async {
            match block_handle_storage.load_handle(&req.block_id) {
                Some(handle) if handle.has_proof() => {
                    let data = block_storage.load_block_proof_raw(&handle).await?;
                    Ok::<_, anyhow::Error>(KeyBlockProof::Found {
                        proof: Bytes::from_owner(data),
                    })
                }
                _ => Ok(KeyBlockProof::NotFound),
            }
        };

        match get_key_block_proof.await {
            Ok(key_block_proof) => overlay::Response::Ok(key_block_proof),
            Err(e) => {
                tracing::warn!("get_key_block_proof failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) async fn handle_get_archive_info(
        &self,
        req: &rpc::GetArchiveInfo,
    ) -> overlay::Response<ArchiveInfo> {
        let mc_seqno = req.mc_seqno;
        let node_state = self.storage.node_state();

        match node_state.load_last_mc_block_id() {
            Some(last_applied_mc_block) => {
                if mc_seqno > last_applied_mc_block.seqno {
                    return overlay::Response::Ok(ArchiveInfo::TooNew);
                }

                let block_storage = self.storage().block_storage();

                let id = block_storage.get_archive_id(mc_seqno);
                let size_res = match id {
                    ArchiveId::Found(id) => block_storage.get_archive_size(id),
                    ArchiveId::TooNew | ArchiveId::NotFound => Ok(None),
                };

                overlay::Response::Ok(match (id, size_res) {
                    (ArchiveId::Found(id), Ok(Some(size))) if size > 0 => ArchiveInfo::Found {
                        id: id as u64,
                        size: NonZeroU64::new(size as _).unwrap(),
                        chunk_size: block_storage.archive_chunk_size(),
                    },
                    (ArchiveId::TooNew, Ok(None)) => ArchiveInfo::TooNew,
                    _ => ArchiveInfo::NotFound,
                })
            }
            None => {
                tracing::warn!("get_archive_id failed: no blocks applied");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) async fn handle_get_archive_chunk(
        &self,
        req: &rpc::GetArchiveChunk,
    ) -> overlay::Response<Data> {
        let block_storage = self.storage.block_storage();

        let get_archive_chunk = || async {
            let archive_slice = block_storage
                .get_archive_chunk(req.archive_id as u32, req.offset)
                .await?;

            Ok::<_, anyhow::Error>(archive_slice)
        };

        match get_archive_chunk().await {
            Ok(data) => overlay::Response::Ok(Data {
                data: Bytes::from_owner(data),
            }),
            Err(e) => {
                tracing::warn!("get_archive_chunk failed: {e:?}");
                overlay::Response::Err(INTERNAL_ERROR_CODE)
            }
        }
    }

    pub(super) fn handle_get_persistent_state_info(
        &self,
        req: &rpc::GetPersistentShardStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        let label = [("method", "getPersistentStateInfo")];
        let _hist = HistogramGuard::begin_with_labels(RPC_METHOD_TIMINGS_METRIC, &label);
        let res = self.read_persistent_state_info(&req.block_id, PersistentStateKind::Shard);
        overlay::Response::Ok(res)
    }

    pub(super) fn handle_get_queue_persistent_state_info(
        &self,
        req: &rpc::GetPersistentQueueStateInfo,
    ) -> overlay::Response<PersistentStateInfo> {
        let res = self.read_persistent_state_info(&req.block_id, PersistentStateKind::Queue);
        overlay::Response::Ok(res)
    }

    pub(super) async fn handle_get_persistent_shard_state_chunk(
        &self,
        req: &rpc::GetPersistentShardStateChunk,
    ) -> overlay::Response<Data> {
        self.read_persistent_state_chunk(&req.block_id, req.offset, PersistentStateKind::Shard)
            .await
    }

    pub(super) async fn handle_get_persistent_queue_state_chunk(
        &self,
        req: &rpc::GetPersistentQueueStateChunk,
    ) -> overlay::Response<Data> {
        self.read_persistent_state_chunk(&req.block_id, req.offset, PersistentStateKind::Queue)
            .await
    }

    pub(super) async fn get_block_full(&self, block_id: &BlockId) -> anyhow::Result<BlockFull> {
        let block_handle_storage = self.storage().block_handle_storage();
        let block_storage = self.storage().block_storage();

        let handle = match block_handle_storage.load_handle(block_id) {
            Some(handle) if handle.has_all_block_parts() => handle,
            _ => return Ok(BlockFull::NotFound),
        };

        let Some(data) = block_storage.get_block_data_chunk(block_id, 0)? else {
            return Ok(BlockFull::NotFound);
        };

        let data_chunk_size = block_storage.block_data_chunk_size();
        let data_size = if data.len() < data_chunk_size.get() as usize {
            // NOTE: Skip one RocksDB read for relatively small blocks
            //       Average block size is 4KB, while the chunk size is 1MB.
            data.len() as u32
        } else {
            match block_storage.get_block_data_size(block_id)? {
                Some(size) => size,
                None => return Ok(BlockFull::NotFound),
            }
        };

        let block = BlockData {
            data: Bytes::from_owner(data),
            size: NonZeroU32::new(data_size).expect("shouldn't happen"),
            chunk_size: data_chunk_size,
        };

        let (proof, queue_diff) = tokio::join!(
            block_storage.load_block_proof_raw(&handle),
            block_storage.load_queue_diff_raw(&handle)
        );

        Ok(BlockFull::Found {
            block_id: *block_id,
            block,
            proof: Bytes::from_owner(proof?),
            queue_diff: Bytes::from_owner(queue_diff?),
        })
    }

    pub(super) fn read_persistent_state_info(
        &self,
        block_id: &BlockId,
        state_kind: PersistentStateKind,
    ) -> PersistentStateInfo {
        let persistent_state_storage = self.storage().persistent_state_storage();
        if self.config.serve_persistent_states {
            if let Some(info) = persistent_state_storage.get_state_info(block_id, state_kind) {
                return PersistentStateInfo::Found {
                    size: info.size,
                    chunk_size: info.chunk_size,
                };
            }
        }
        PersistentStateInfo::NotFound
    }

    pub(super) async fn read_persistent_state_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        state_kind: PersistentStateKind,
    ) -> overlay::Response<Data> {
        let persistent_state_storage = self.storage().persistent_state_storage();

        let persistent_state_request_validation = || {
            anyhow::ensure!(
                self.config.serve_persistent_states,
                "persistent states are disabled"
            );
            Ok::<_, anyhow::Error>(())
        };

        if let Err(e) = persistent_state_request_validation() {
            tracing::debug!("persistent state request validation failed: {e:?}");
            return overlay::Response::Err(BAD_REQUEST_ERROR_CODE);
        }

        match persistent_state_storage
            .read_state_part(block_id, offset, state_kind)
            .await
        {
            Some(data) => overlay::Response::Ok(Data { data: data.into() }),
            None => {
                tracing::debug!("failed to read persistent state part");
                overlay::Response::Err(NOT_FOUND_ERROR_CODE)
            }
        }
    }
}
