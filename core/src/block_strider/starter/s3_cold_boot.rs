use std::fs::File;
use std::pin::pin;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use object_store::path::Path;
use tycho_block_util::archive::{ArchiveData, WithArchiveData};
use tycho_block_util::block::{BlockProofStuff, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::fs::FileBuilder;
use tycho_types::models::{BlockId, OutMsgQueueUpdates};
use tycho_util::sync::rayon_run;

use crate::block_strider::{CheckProof, ProofChecker};
use crate::s3::S3Client;
use crate::storage::{BlockHandle, CoreStorage, NewBlockMeta, PersistentStateKind};

pub struct S3Starter {
    start_from: u32,
    s3_client: S3Client,
    storage: CoreStorage,
}

impl S3Starter {
    pub fn new(
        start_from: &Option<u32>,
        s3_client: &S3Client,
        storage: &CoreStorage,
    ) -> Result<Self> {
        Ok(Self {
            s3_client: s3_client.clone(),
            storage: storage.clone(),
            start_from: start_from.unwrap_or(u32::MAX),
        })
    }

    pub async fn choose_key_block(&self) -> Result<BlockId> {
        let prefix_path = Path::from("states");

        let mut closest: Option<BlockId> = None;

        let mut list = self.s3_client.list(Some(&prefix_path));
        while let Some(item) = list.next().await {
            let item = item?;

            let Some(extension) = item.location.extension() else {
                continue;
            };

            if PersistentStateKind::from_extension(extension) != Some(PersistentStateKind::Shard) {
                continue;
            }

            let filename = item
                .location
                .filename()
                .and_then(|f| f.rsplit_once('.').map(|(name, _)| name))
                .ok_or_else(|| anyhow!("invalid filename format for persistent state"))?;

            let block_id = BlockId::from_str(filename)?;

            if !block_id.is_masterchain() {
                continue;
            }

            if block_id.seqno > self.start_from {
                continue;
            }

            if closest.as_ref().is_none_or(|c| block_id.seqno > c.seqno) {
                closest = Some(block_id);
            }
        }

        closest.ok_or_else(|| anyhow!("key block not found"))
    }

    pub async fn download_start_blocks_and_states(&self, mc_block_id: &BlockId) -> Result<()> {
        // Download and save masterchain block and state
        let (_, init_mc_block) = self
            .download_block_with_states(mc_block_id, mc_block_id)
            .await?;

        tracing::info!(
            block_id = %init_mc_block.id(),
            "downloaded init mc block state"
        );

        // Download and save blocks and states from other shards
        for (_, block_id) in init_mc_block.shard_blocks()? {
            let (handle, _) = self
                .download_block_with_states(mc_block_id, &block_id)
                .await?;

            self.storage
                .block_handle_storage()
                .set_block_committed(&handle);
        }

        Ok(())
    }

    async fn download_block_with_states(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, BlockStuff)> {
        // First download the block itself, with all its parts (proof and queue diff).
        let (handle, block) = self.download_block_data(mc_block_id, block_id).await?;
        self.storage
            .block_handle_storage()
            .set_block_persistent(&handle);

        // Download persistent shard state
        {
            let state_update = block.as_ref().load_state_update()?;

            let (_, shard_state) = self.download_shard_state(mc_block_id, block_id).await?;
            let state_hash = *shard_state.root_cell().repr_hash();
            anyhow::ensure!(
                state_update.new_hash == state_hash,
                "downloaded shard state hash mismatch"
            );
        }

        // Download persistent queue state
        // NOTE: There is no queue state for zerostate, and there might be a situation
        //       where there were no blocks in the shard.
        if block_id.seqno != 0 {
            let top_update = &block.as_ref().out_msg_queue_updates;
            self.download_queue_state(&handle, top_update).await?;
        }

        Ok((handle, block))
    }

    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    async fn download_block_data(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, BlockStuff)> {
        let s3_client = &self.s3_client;
        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();

        let block_handle = block_handles.load_handle(block_id);
        if let Some(handle) = &block_handle {
            // NOTE: Block data is stored only after all proofs/queues are verified
            if handle.has_data() {
                let block = blocks.load_block_data(handle).await?;

                tracing::info!("using the stored block");
                return Ok((handle.clone(), block));
            }
        }

        let proof_checker = ProofChecker::new(self.storage.clone());

        // TODO: add retry count to interrupt infinite loop
        'outer: loop {
            let full = 'res: {
                match s3_client.get_key_block_full(block_id).await {
                    Ok(res) => match res {
                        Some(data) if &data.block_id == block_id => {
                            break 'res data;
                        }
                        Some(_) => {
                            tracing::warn!("received block id mismatch");
                        }
                        None => tracing::warn!("block not found"),
                    },
                    Err(e) => tracing::warn!("failed to download block: {e:?}"),
                }

                // TODO: Backoff
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue 'outer;
            };

            let block_stuff_fut = pin!(rayon_run({
                let block_id = *block_id;
                let block_data = full.block_data.clone();
                move || BlockStuff::deserialize_checked(&block_id, &block_data)
            }));

            let other_data_fut = pin!(rayon_run({
                let block_id = *block_id;
                let proof_data = full.proof_data.clone();
                let queue_diff_data = full.queue_diff_data.clone();
                move || {
                    (
                        BlockProofStuff::deserialize(&block_id, &proof_data),
                        QueueDiffStuff::deserialize(&block_id, &queue_diff_data),
                    )
                }
            }));

            let (block_stuff, (block_proof, queue_diff)) =
                futures_util::future::join(block_stuff_fut, other_data_fut).await;

            match (block_stuff, block_proof, queue_diff) {
                (Ok(block), Ok(proof), Ok(diff)) => {
                    let proof = WithArchiveData::new(proof, full.proof_data);
                    let diff = WithArchiveData::new(diff, full.queue_diff_data);
                    match proof_checker
                        .check_proof(CheckProof {
                            mc_block_id,
                            block: &block,
                            proof: &proof,
                            queue_diff: &diff,
                            store_on_success: true,
                        })
                        .await
                    {
                        Ok(meta) => {
                            let archive_data = ArchiveData::New(full.block_data);
                            let res = blocks.store_block_data(&block, &archive_data, meta).await?;

                            tracing::info!("using the downloaded block");
                            return Ok((res.handle, block));
                        }
                        Err(e) => {
                            tracing::error!("got invalid block proof: {e:?}");
                        }
                    }
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                    tracing::error!("failed to deserialize shard block or block proof: {e:?}");
                }
            }

            // TODO: Backoff
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tracing::instrument(skip_all, fields(block_id = %block_handle.id()))]
    async fn download_queue_state(
        &self,
        block_handle: &BlockHandle,
        _top_update: &OutMsgQueueUpdates,
    ) -> Result<()> {
        let block_id = block_handle.id();

        let temp = self.storage.context().temp_files();
        let persistent_states = self.storage.persistent_state_storage();

        let state_file = temp.file(format!("queue_state_{block_id}"));
        let state_file_path = state_file.path().to_owned();

        // NOTE: Intentionally dont spawn yet
        let remove_state_file = async move {
            if let Err(e) = tokio::fs::remove_file(&state_file_path).await {
                tracing::warn!(
                    path = %state_file_path.display(),
                    "failed to remove downloaded queue state: {e:?}",
                );
            }
        };

        let mc_seqno = block_handle.ref_by_mc_seqno();
        let try_save_persistent = |block_handle: &BlockHandle, mut state_file: FileBuilder| {
            let block_handle = block_handle.clone();
            async move {
                // Reuse downloaded (and validated) file as is.
                let state_file = state_file.read(true).open()?;
                persistent_states
                    .store_queue_state_file(mc_seqno, &block_handle, state_file)
                    .await
            }
        };

        for attempt in 0..MAX_PERSISTENT_STATE_RETRIES {
            let _file = match self
                .download_persistent_state_file(block_id, PersistentStateKind::Queue, &state_file)
                .await
            {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(attempt, "failed to download persistent queue state: {e:?}");
                    continue;
                }
            };

            // TODO: queue state validation
            // self.queue_state_handler
            //     .import_from_file(top_update, file, block_id)
            //     .await?;

            try_save_persistent(block_handle, state_file)
                .await
                .with_context(|| "failed to store persistent queue state")?;

            remove_state_file.await;

            tracing::info!("using the downloaded queue state");
            return Ok(());
        }

        anyhow::bail!("ran out of attempts")
    }

    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    async fn download_shard_state(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, ShardStateStuff)> {
        enum StoreZeroStateFrom {
            File(FileBuilder),
            State(ShardStateStuff),
        }

        let shard_states = self.storage.shard_state_storage();
        let persistent_states = self.storage.persistent_state_storage();
        let block_handles = self.storage.block_handle_storage();

        let temp = self.storage.context().temp_files();

        let state_file = temp.file(format!("state_{block_id}"));
        let state_file_path = state_file.path().to_owned();

        // NOTE: Intentionally dont spawn yet
        let remove_state_file = async move {
            if let Err(e) = tokio::fs::remove_file(&state_file_path).await {
                tracing::warn!(
                    path = %state_file_path.display(),
                    "failed to remove downloaded shard state: {e:?}",
                );
            }
        };

        let mc_seqno = mc_block_id.seqno;
        let try_save_persistent = |block_handle: &BlockHandle, from: StoreZeroStateFrom| {
            let block_handle = block_handle.clone();
            async move {
                match from {
                    // Fast reuse the downloaded file if possible
                    StoreZeroStateFrom::File(mut state_file) => {
                        // Reuse downloaded (and validated) file as is.
                        let state_file = state_file.read(true).open()?;
                        persistent_states
                            .store_shard_state_file(mc_seqno, &block_handle, state_file)
                            .await
                    }
                    // Possibly slow full state traversal
                    StoreZeroStateFrom::State(state) => {
                        // Store zerostate as is
                        persistent_states
                            .store_shard_state(
                                mc_seqno,
                                &block_handle,
                                state.ref_mc_state_handle().clone(),
                            )
                            .await
                    }
                }
            }
        };

        // Fast path goes first. If the state exists we only need to try to save persistent.
        let block_handle = block_handles.load_handle(block_id);
        if let Some(handle) = &block_handle
            && handle.has_state()
        {
            let state = shard_states.load_state(mc_seqno, block_id).await?;

            if !handle.has_persistent_shard_state() {
                let from = if state_file.exists() {
                    StoreZeroStateFrom::File(state_file)
                } else {
                    StoreZeroStateFrom::State(state.clone())
                };
                try_save_persistent(handle, from).await?;
            }

            remove_state_file.await;

            tracing::info!("using the stored shard state");
            return Ok((handle.clone(), state));
        }

        // Try download the state
        for attempt in 0..MAX_PERSISTENT_STATE_RETRIES {
            let file = match self
                .download_persistent_state_file(block_id, PersistentStateKind::Shard, &state_file)
                .await
            {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(attempt, "failed to download persistent shard state: {e:?}");
                    continue;
                }
            };

            // NOTE: `store_state_file` error is mostly unrecoverable since the operation
            //       context is too large to be atomic.
            // TODO: Make this operation recoverable to allow an infinite number of attempts.
            shard_states
                .store_state_file(block_id, file)
                .await
                .context("failed to store shard state file")?;

            let state = shard_states
                .load_state(mc_seqno, block_id)
                .await
                .context("failed to reload saved shard state")?;

            let block_handle = match block_handle {
                Some(handle) => handle,
                None => {
                    let (handle, _) = block_handles.create_or_load_handle(block_id, NewBlockMeta {
                        is_key_block: block_id.is_masterchain(),
                        gen_utime: state.as_ref().gen_utime,
                        ref_by_mc_seqno: mc_block_id.seqno,
                    });
                    handle
                }
            };

            let from = StoreZeroStateFrom::File(state_file);
            try_save_persistent(&block_handle, from).await?;

            remove_state_file.await;

            tracing::info!("using the downloaded shard state");
            return Ok((block_handle, state));
        }

        anyhow::bail!("ran out of attempts")
    }

    async fn download_persistent_state_file(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
        state_file: &FileBuilder,
    ) -> Result<File> {
        let mut temp_file = state_file.with_extension("temp");
        let temp_file_path = temp_file.path().to_owned();
        scopeguard::defer! {
            std::fs::remove_file(temp_file_path).ok();
        };

        let s3_client = &self.s3_client;
        loop {
            if state_file.exists() {
                // Use the downloaded state file if it exists
                return state_file.clone().read(true).open();
            }

            let output = temp_file.write(true).create(true).truncate(true).open()?;
            s3_client
                .download_persistent_state(block_id, kind, output)
                .await?;

            tokio::fs::rename(temp_file.path(), state_file.path()).await?;

            // NOTE: File will be loaded on the next iteration of the loop
        }
    }
}

const MAX_PERSISTENT_STATE_RETRIES: usize = 10;
