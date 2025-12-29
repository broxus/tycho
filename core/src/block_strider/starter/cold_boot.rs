use std::fs::File;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tycho_block_util::archive::{ArchiveData, WithArchiveData};
use tycho_block_util::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::fs::FileBuilder;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;
use tycho_util::sync::rayon_run;
use tycho_util::time::now_sec;

use super::{ColdBootType, StarterInner, ZerostateProvider};
use crate::block_strider::{CheckProof, ProofChecker};
use crate::blockchain_rpc::BlockchainRpcClient;
use crate::overlay_client::PunishReason;
use crate::proto::blockchain::KeyBlockProof;
use crate::storage::{
    BlockHandle, CoreStorage, KeyBlocksDirection, MaybeExistingHandle, NewBlockMeta,
    PersistentStateKind, ShardStateParams,
};

impl StarterInner {
    #[tracing::instrument(skip_all)]
    pub async fn cold_boot<P>(
        &self,
        boot_type: ColdBootType,
        zerostates: Option<P>,
    ) -> Result<BlockId>
    where
        P: ZerostateProvider,
    {
        tracing::info!("started");

        let last_mc_block_id = match boot_type {
            ColdBootType::Genesis => {
                // Either import or download a zerostate.
                let init_block = self.prepare_init_block(zerostates).await?;

                // Always use zerostate id as an initial block id when doing sync from genesis.
                *init_block.handle().id()
            }
            ColdBootType::LatestPersistent => {
                // Find the last known key block (or zerostate)
                // from which we can start downloading other key blocks
                let init_block = self.prepare_init_block(zerostates).await?;

                // Ensure that all key blocks until now (with some offset) are downloaded
                self.download_key_blocks(init_block).await?;

                // Choose the latest key block with persistent state
                let last_key_block = self.choose_key_block()?;

                if last_key_block.id().seqno > self.zerostate.seqno {
                    // If the last suitable key block is not zerostate, we must download all blocks
                    // with their states from shards for that
                    self.download_start_blocks_and_states(last_key_block.id())
                        .await?;
                }

                *last_key_block.id()
            }
        };

        self.storage
            .node_state()
            .store_last_mc_block_id(&last_mc_block_id);

        tracing::info!(
            last_mc_block_id = %last_mc_block_id,
            "finished",
        );

        Ok(last_mc_block_id)
    }

    // === Sync steps ===

    /// Prepare the initial block to start syncing.
    async fn prepare_init_block<P>(&self, zerostates: Option<P>) -> Result<InitBlock>
    where
        P: ZerostateProvider,
    {
        let node_state = self.storage.node_state();
        let block_id = node_state
            .load_init_mc_block_id()
            .unwrap_or(self.zerostate.as_block_id());
        anyhow::ensure!(
            block_id.seqno >= self.zerostate.seqno,
            "old storage cannot be resued for hardforks"
        );

        tracing::info!(init_block_id = %block_id, "preparing init block");
        let prev_key_block = if block_id.seqno == self.zerostate.seqno {
            tracing::info!(%block_id, "using zero state");

            let (handle, state) = match zerostates {
                Some(zerostates) => self.import_zerostates(zerostates).await?,
                None => self.download_zerostates().await?,
            };

            // NOTE: Ensure that init block id is always present
            node_state.store_init_mc_block_id(handle.id());

            InitBlock::ZeroState {
                handle: Arc::new(handle),
                state: Arc::new(state),
            }
        } else {
            tracing::info!(%block_id, "using key block");

            let handle = self
                .storage
                .block_handle_storage()
                .load_handle(&block_id)
                .expect("shouldn't happen");

            let proof = self
                .storage
                .block_storage()
                .load_block_proof(&handle)
                .await?;

            InitBlock::KeyBlock {
                handle: Arc::new(handle),
                proof: Box::new(proof),
            }
        };

        Ok(prev_key_block)
    }

    /// Download all key blocks since the initial block.
    async fn download_key_blocks(&self, mut prev_key_block: InitBlock) -> Result<()> {
        const BLOCKS_PER_BATCH: u32 = 10;
        const PARALLEL_REQUESTS: usize = 10;

        let (ids_tx, mut ids_rx) = mpsc::unbounded_channel();
        let (tasks_tx, mut tasks_rx) = mpsc::unbounded_channel();

        tokio::spawn({
            let blockchain_rpc_client = self.blockchain_rpc_client.clone();

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

                                if ids_tx.send((block_id, data.block_ids)).is_err() {
                                    tracing::debug!(%block_id, "stop downloading next key blocks");
                                    return;
                                }

                                break 'inner;
                            }
                            Err(e) => {
                                tracing::warn!(%block_id, "failed to download key block ids: {e:?}");

                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                }
            }
        });

        // Start getting next key blocks
        tasks_tx.send(*prev_key_block.handle().id())?;

        let satisfies_offset = |gen_utime: u32, now_utime: u32| match self.config.custom_boot_offset
        {
            None => BlockStuff::can_use_for_boot(gen_utime, now_utime),
            Some(t) => now_utime.saturating_sub(gen_utime) as u64 >= t.as_secs(),
        };

        let satisfies_seqno = |seqno: u32| match self.config.start_from {
            None => true,
            Some(start_from) => start_from > seqno,
        };

        let mut retry_counter = 0usize;
        while let Some((requested_key_block, ids)) = ids_rx.recv().await {
            let stream = futures_util::stream::iter(ids)
                .map(|block_id| {
                    JoinTask::new(download_block_proof_task(
                        self.storage.clone(),
                        self.blockchain_rpc_client.clone(),
                        block_id,
                    ))
                })
                .buffered(PARALLEL_REQUESTS);

            let mut proofs = stream.collect::<Vec<_>>().await;
            proofs.sort_by_key(|x| *x.id());

            // Save previous key block to restart downloading in case of error
            let fallback_key_block = prev_key_block.clone();

            let now_utime = now_sec();
            let mut has_newer = false;
            let proofs_len = proofs.len();
            for (index, proof) in proofs.into_iter().enumerate() {
                // Verify block proof
                match prev_key_block.check_next_proof(&proof.data) {
                    Ok(meta)
                        if satisfies_offset(meta.gen_utime, now_utime)
                            && satisfies_seqno(meta.ref_by_mc_seqno) =>
                    {
                        // Save block proof
                        let handle = self
                            .storage
                            .block_storage()
                            .store_block_proof(&proof, MaybeExistingHandle::New(meta))
                            .await?
                            .handle;

                        let block_utime = handle.gen_utime();
                        let prev_utime = prev_key_block.handle().gen_utime();

                        // Update init_mc_block_id
                        if BlockStuff::compute_is_persistent(block_utime, prev_utime) {
                            self.storage
                                .node_state()
                                .store_init_mc_block_id(handle.id());
                        }

                        // Trigger task to getting next key blocks
                        if index == proofs_len.saturating_sub(1) {
                            tasks_tx.send(*proof.data.id())?;
                        }

                        // Update prev_key_block
                        prev_key_block = InitBlock::KeyBlock {
                            handle: Arc::new(handle),
                            proof: Box::new(proof.data),
                        };
                    }
                    Ok(_) => {
                        has_newer = true;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("got invalid key block proof: {e:?}");

                        // Restart downloading proofs
                        tasks_tx.send(*fallback_key_block.handle().id())?;
                        prev_key_block = fallback_key_block;

                        break;
                    }
                }
            }

            let last_utime = prev_key_block.handle().gen_utime();
            let no_proofs = proofs_len == 0;

            tracing::debug!(
                now_utime,
                last_utime,
                last_known_block_id = %prev_key_block.handle().id(),
            );

            // Prevent infinite key blocks loading
            if has_newer || no_proofs && retry_counter >= MAX_EMPTY_PROOF_RETRIES {
                break;
            }

            if no_proofs {
                retry_counter += 1;
                tracing::warn!(
                    attempt = retry_counter,
                    block_id = %requested_key_block,
                    "retry getting next key block ids"
                );
                tasks_tx.send(requested_key_block)?;
            } else {
                retry_counter = 0;
            }
        }

        Ok(())
    }

    /// Select the latest suitable key block with persistent state
    fn choose_key_block(&self) -> Result<BlockHandle> {
        let block_handle_storage = self.storage.block_handle_storage();

        let mut key_blocks = block_handle_storage
            .key_blocks_iterator(KeyBlocksDirection::Backward)
            .map(|block_id| {
                block_handle_storage
                    .load_handle(&block_id)
                    .context("Key block handle not found")
            })
            .peekable();

        // Iterate all key blocks in reverse order (from the latest to the oldest)
        while let Some(handle) = key_blocks.next().transpose()? {
            let handle_utime = handle.gen_utime();
            let prev_utime = match key_blocks.peek() {
                Some(Ok(prev_block)) => prev_block.gen_utime(),
                Some(Err(e)) => anyhow::bail!("failed to load previous key block: {e:?}"),
                None => 0,
            };

            // Skip not persistent
            let is_persistent = BlockStuff::compute_is_persistent(handle_utime, prev_utime);
            if !is_persistent {
                tracing::debug!(seq_no = handle.id().seqno, "skipping key block");
                continue;
            }

            // Use first suitable key block
            tracing::info!(block_id = %handle.id(), "found best key block handle");
            return Ok(handle);
        }

        // NOTE: Should be unreachable since we will definitely have a zerostate
        anyhow::bail!("no suitable key block found")
    }

    async fn download_start_blocks_and_states(&self, mc_block_id: &BlockId) -> Result<()> {
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

    // === Helper methods ===

    async fn import_zerostates<P>(&self, provider: P) -> Result<(BlockHandle, ShardStateStuff)>
    where
        P: ZerostateProvider,
    {
        tracing::info!("import zerostates");

        let state_storage = self.storage.shard_state_storage();

        // Read all zerostates
        let mut zerostates = FastHashMap::default();
        for loaded in provider.load_zerostates() {
            let state = loaded?;
            let file_hash = Boc::file_hash_blake(&state);
            tracing::info!(%file_hash, "found zerostate file");
            if zerostates.insert(file_hash, state).is_some() {
                anyhow::bail!("duplicate zerostate {file_hash}");
            }
        }

        let Some(mc_zerostate) = zerostates.remove(&self.zerostate.file_hash) else {
            anyhow::bail!(
                "missing mc zerostate for file hash {}",
                self.zerostate.file_hash
            );
        };

        let mc_block_id = self.zerostate.as_block_id();
        tracing::info!(%mc_block_id, "importing masterchain zerostate");
        let root_hash = state_storage
            .store_state_bytes(&mc_block_id, mc_zerostate, ShardStateParams {
                is_zerostate: true,
            })
            .await?;
        anyhow::ensure!(
            root_hash == self.zerostate.root_hash,
            "imported zerostate root hash mismatch"
        );

        let mc_zerostate = state_storage
            .load_state(mc_block_id.seqno, &mc_block_id)
            .await?;

        let global_id = mc_zerostate.state().global_id;
        let gen_utime = mc_zerostate.state().gen_utime;

        let persistent_states = self.storage.persistent_state_storage();
        let handle_storage = self.storage.block_handle_storage();

        let ref_by_mc_seqno = mc_block_id.seqno;
        let (handle, _) = handle_storage.create_or_load_handle(&mc_block_id, NewBlockMeta {
            is_key_block: true,
            gen_utime,
            ref_by_mc_seqno,
        });

        for entry in mc_zerostate.shards()?.latest_blocks() {
            let block_id = entry.context("invalid mc zerostate")?;

            let state_bytes = match zerostates.remove(&block_id.file_hash) {
                Some(existing) => {
                    // TODO: use filename with optional path in returned value
                    tracing::debug!(block_id = %block_id, "using custom zerostate");
                    existing
                }
                None => {
                    let (computed_id, bytes) =
                        make_shard_state(global_id, block_id.shard, gen_utime)
                            .context("failed to create shard zerostate")?;
                    anyhow::ensure!(
                        computed_id == block_id,
                        "custom zerostate must be provided for {}",
                        block_id.shard
                    );
                    bytes
                }
            };

            tracing::info!(%block_id, "importing shard zerostate");
            let root_hash = state_storage
                .store_state_bytes(&block_id, state_bytes, ShardStateParams {
                    is_zerostate: true,
                })
                .await?;
            anyhow::ensure!(
                root_hash == block_id.root_hash,
                "imported zerostate root hash mismatch"
            );

            let (handle, _) = handle_storage.create_or_load_handle(&block_id, NewBlockMeta {
                is_key_block: false,
                gen_utime,
                ref_by_mc_seqno,
            });
            let state = state_storage
                .load_state(mc_block_id.seqno, handle.id())
                .await?;

            // todo: error text
            anyhow::ensure!(state.state().shard_ident == block_id.shard);
            anyhow::ensure!(state.state().seqno == block_id.seqno);

            handle_storage.set_is_zerostate(&handle);
            handle_storage.set_has_shard_state(&handle);
            handle_storage.set_block_committed(&handle);

            persistent_states
                .store_shard_state(
                    mc_block_id.seqno,
                    &handle,
                    state.ref_mc_state_handle().clone(),
                )
                .await?;

            tracing::debug!(%block_id, "imported persistent shard state");
        }

        anyhow::ensure!(
            zerostates.is_empty(),
            "unused zerostates left: {}",
            zerostates.len()
        );

        handle_storage.set_is_zerostate(&handle);
        handle_storage.set_has_shard_state(&handle);
        handle_storage.set_block_committed(&handle);

        persistent_states
            .store_shard_state(
                mc_block_id.seqno,
                &handle,
                mc_zerostate.ref_mc_state_handle().clone(),
            )
            .await?;

        tracing::info!("imported zerostates");

        Ok((handle, mc_zerostate))
    }

    async fn download_zerostates(&self) -> Result<(BlockHandle, ShardStateStuff)> {
        let zerostate_id = self.zerostate.as_block_id();
        tracing::info!(zerostate_id = %zerostate_id, "download zerostates");

        let (handle, state) = self
            .download_shard_state(&zerostate_id, &zerostate_id, true)
            .await?;

        for item in state.shards()?.latest_blocks() {
            let block_id = item?;
            let _state = self
                .download_shard_state(&zerostate_id, &block_id, true)
                .await?;
        }

        Ok((handle, state))
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
        if !self.ignore_states {
            let state_update = block.as_ref().load_state_update()?;

            let (_, shard_state) = self
                .download_shard_state(mc_block_id, block_id, false)
                .await?;
            let state_hash = *shard_state.root_cell().repr_hash();
            anyhow::ensure!(
                state_update.new_hash == state_hash,
                "downloaded shard state hash mismatch"
            );
        }

        // Download persistent queue state
        // NOTE: There is no queue state for zerostate, and there might be a situation
        //       where there were no blocks in the shard.
        if !self.ignore_states && block_id.seqno > self.zerostate.seqno {
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
        let client = &self.starter_client;
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

        let proof_checker = ProofChecker::new(self.zerostate, self.storage.clone());

        // TODO: add retry count to interrupt infinite loop
        'outer: loop {
            let (full, punish) = 'res: {
                match client.get_block_full(mc_block_id.seqno, block_id).await {
                    Ok(res) => {
                        if &res.data.block_id == block_id {
                            break 'res (res.data, res.punish);
                        }

                        (res.punish)(PunishReason::Malicious);
                        tracing::warn!("received block id mismatch");
                    }
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
                            (punish)(PunishReason::Malicious);
                            tracing::error!("got invalid block proof: {e:?}");
                        }
                    }
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                    (punish)(PunishReason::Malicious);
                    tracing::error!("failed to deserialize shard block or block proof: {e:?}");
                }
            }

            // TODO: Backoff
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // NOTE: We cannot use block handle here since we also need to use this method
    //       for downloading zerostates, for which we cannot know the `gen_utime`
    //       in advance.
    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    async fn download_shard_state(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
        is_zerostate: bool,
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
            let state = shard_states
                .load_state(mc_seqno, block_id)
                .await
                .context("failed to load state on downloaded shard state")?;

            if !handle.has_persistent_shard_state() {
                let from = if state_file.exists() {
                    StoreZeroStateFrom::File(state_file)
                } else {
                    StoreZeroStateFrom::State(state.clone())
                };
                try_save_persistent(handle, from)
                    .await
                    .context("failed to store persistent shard state")?;
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
                .store_state_file(block_id, file, ShardStateParams { is_zerostate })
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

            // set flag that state stored
            block_handles.set_has_shard_state(&block_handle);

            let from = StoreZeroStateFrom::File(state_file);
            try_save_persistent(&block_handle, from)
                .await
                .context("failed to store persistent shard state")?;

            remove_state_file.await;

            tracing::info!("using the downloaded shard state");
            return Ok((block_handle, state));
        }

        anyhow::bail!("ran out of attempts")
    }

    #[tracing::instrument(skip_all, fields(block_id = %block_handle.id()))]
    async fn download_queue_state(
        &self,
        block_handle: &BlockHandle,
        top_update: &OutMsgQueueUpdates,
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
            let file = match self
                .download_persistent_state_file(block_id, PersistentStateKind::Queue, &state_file)
                .await
            {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(attempt, "failed to download persistent queue state: {e:?}");
                    continue;
                }
            };

            self.queue_state_handler
                .import_from_file(top_update, file, block_id)
                .await?;

            try_save_persistent(block_handle, state_file)
                .await
                .context("failed to store persistent queue state")?;

            remove_state_file.await;

            tracing::info!("using the downloaded queue state");
            return Ok(());
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

        let client = &self.starter_client;
        loop {
            if state_file.exists() {
                // Use the downloaded state file if it exists
                return state_file.clone().read(true).open();
            }

            let pending_state = client.find_persistent_state(block_id, kind).await?;

            let output = temp_file.write(true).create(true).truncate(true).open()?;
            _ = (pending_state.download)(output).await?;

            tokio::fs::rename(temp_file.path(), state_file.path()).await?;

            // NOTE: File will be loaded on the next iteration of the loop
        }
    }
}

async fn download_block_proof_task(
    storage: CoreStorage,
    rpc_client: BlockchainRpcClient,
    block_id: BlockId,
) -> BlockProofStuffAug {
    let block_storage = storage.block_storage();
    let block_handle_storage = storage.block_handle_storage();

    // Check whether block proof is already stored locally
    if let Some(handle) = block_handle_storage.load_handle(&block_id)
        && let Ok(proof) = block_storage.load_block_proof(&handle).await
    {
        return WithArchiveData::loaded(proof);
    }

    // TODO: add retry count to interrupt infinite loop
    loop {
        let res = rpc_client.get_key_block_proof(&block_id).await;

        match res {
            Ok(res) => 'validate: {
                let (handle, data) = res.split();
                let KeyBlockProof::Found { proof: data } = data else {
                    tracing::debug!(%block_id, "block proof not found");
                    handle.accept();
                    break 'validate;
                };

                match BlockProofStuff::deserialize(&block_id, &data) {
                    Ok(proof) => {
                        handle.accept();
                        return WithArchiveData::new(proof, data);
                    }
                    Err(e) => {
                        tracing::error!(%block_id, "failed to deserialize block proof: {e:?}");
                        handle.reject();
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%block_id, "failed to download block proof: {e:?}");
            }
        }

        // TODO: Backoff
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn make_shard_state(global_id: i32, shard_ident: ShardIdent, now: u32) -> Result<(BlockId, Bytes)> {
    let state = ShardStateUnsplit {
        global_id,
        shard_ident,
        gen_utime: now,
        min_ref_mc_seqno: u32::MAX,
        ..Default::default()
    };

    let root = CellBuilder::build_from(&state)?;
    let root_hash = *root.repr_hash();
    let boc = Boc::encode(root);
    let file_hash = Boc::file_hash_blake(&boc);

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    Ok((block_id, Bytes::from(boc)))
}

#[derive(Clone)]
enum InitBlock {
    ZeroState {
        handle: Arc<BlockHandle>,
        state: Arc<ShardStateStuff>,
    },
    KeyBlock {
        handle: Arc<BlockHandle>,
        proof: Box<BlockProofStuff>,
    },
}

impl InitBlock {
    fn handle(&self) -> &Arc<BlockHandle> {
        match self {
            Self::KeyBlock { handle, .. } | Self::ZeroState { handle, .. } => handle,
        }
    }

    fn check_next_proof(&self, next_proof: &BlockProofStuff) -> Result<NewBlockMeta> {
        let (virt_block, virt_block_info) = next_proof
            .pre_check_block_proof()
            .context("Failed to pre check block proof")?;

        let res = NewBlockMeta {
            is_key_block: virt_block_info.key_block,
            gen_utime: virt_block_info.gen_utime,
            ref_by_mc_seqno: next_proof.proof().proof_for.seqno,
        };

        match self {
            // Check block proof with zero state
            InitBlock::ZeroState { state, .. } => tycho_block_util::block::check_with_master_state(
                next_proof,
                state,
                &virt_block,
                &virt_block_info,
            ),
            // Check block proof with previous key block
            InitBlock::KeyBlock { proof, .. } => {
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

const MAX_EMPTY_PROOF_RETRIES: usize = 10;
const MAX_PERSISTENT_STATE_RETRIES: usize = 10;
