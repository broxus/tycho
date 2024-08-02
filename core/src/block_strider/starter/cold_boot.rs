use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_types::models::*;
use everscale_types::prelude::*;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, KEY_BLOCK_UTIME_STEP,
};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::{BlockHandle, BlockMetaData, BlockProofHandle, KeyBlocksDirection, Storage};
use tycho_util::futures::JoinTask;
use tycho_util::time::now_sec;
use tycho_util::FastHashMap;

use super::{StarterInner, ZerostateProvider};
use crate::blockchain_rpc::BlockchainRpcClient;
use crate::proto::blockchain::{BlockFull, KeyBlockProof};

impl StarterInner {
    #[tracing::instrument(skip_all)]
    pub async fn cold_boot<P>(&self, zerostates: Option<P>) -> Result<BlockId>
    where
        P: ZerostateProvider,
    {
        tracing::info!("started");

        // Find the last known key block (or zerostate)
        // from which we can start downloading other key blocks
        let init_block = self.prepare_init_block(zerostates).await?;

        // Ensure that all key blocks until now (with some offset) are downloaded
        self.download_key_blocks(init_block).await?;

        // Choose the latest key block with persistent state
        let last_key_block = self.choose_key_block()?;

        if last_key_block.id().seqno != 0 {
            // If the last suitable key block is not zerostate, we must download all blocks
            // with their states from shards for that
            self.download_start_blocks_and_states(last_key_block.id())
                .await?;
        }

        self.storage
            .node_state()
            .store_last_mc_block_id(last_key_block.id());

        tracing::info!(last_mc_block_id = %last_key_block.id(), "finished");
        Ok(*last_key_block.id())
    }

    // === Sync steps ===

    /// Prepare the initial block to start syncing.
    async fn prepare_init_block<P>(&self, zerostates: Option<P>) -> Result<InitBlock>
    where
        P: ZerostateProvider,
    {
        let block_id = self
            .storage
            .node_state()
            .load_init_mc_block_id()
            .unwrap_or(self.zerostate.as_block_id());

        tracing::info!(init_block_id = %block_id, "preparing init block");
        let prev_key_block = if block_id.seqno == 0 {
            tracing::info!(%block_id, "using zero state");

            let (handle, state) = match zerostates {
                Some(zerostates) => self.import_zerostates(zerostates).await?,
                None => self.download_zerostates().await?,
            };

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
                .load_block_proof(&handle, false)
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

                                if ids_tx.send(data.block_ids).is_err() {
                                    tracing::debug!(%block_id, "stop downloading next key blocks");
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

            let proofs_len = proofs.len();
            for (index, proof) in proofs.into_iter().enumerate() {
                // Verify block proof
                match prev_key_block.check_next_proof(&proof.data) {
                    Ok(meta) => {
                        // Save block proof
                        let handle = self
                            .storage
                            .block_storage()
                            .store_block_proof(&proof, BlockProofHandle::New(meta))
                            .await?
                            .handle;

                        let block_utime = handle.meta().gen_utime();
                        let prev_utime = prev_key_block.handle().meta().gen_utime();

                        // Update init_mc_block_id
                        if tycho_block_util::state::is_persistent_state(block_utime, prev_utime) {
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
                    Err(e) => {
                        tracing::warn!("got invalid key block proof: {e:?}");

                        // Restart downloading proofs
                        tasks_tx.send(*fallback_key_block.handle().id())?;
                        prev_key_block = fallback_key_block;

                        break;
                    }
                }
            }

            let now_utime = now_sec();
            let last_utime = prev_key_block.handle().meta().gen_utime();

            tracing::debug!(
                now_utime,
                last_utime,
                last_known_block_id = %prev_key_block.handle().id(),
            );

            // Prevent infinite key blocks loading
            if last_utime + 2 * KEY_BLOCK_UTIME_STEP > now_utime {
                break;
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

        let mut last_known_key_block = None;

        // Iterate all key blocks in reverse order (from the latest to the oldest)
        while let Some(handle) = key_blocks.next().transpose()? {
            let handle_utime = handle.meta().gen_utime();
            let prev_utime = match key_blocks.peek() {
                Some(Ok(prev_block)) => prev_block.meta().gen_utime(),
                Some(Err(e)) => anyhow::bail!("failed to load previous key block: {e:?}"),
                None => 0,
            };

            let is_persistent =
                tycho_block_util::state::is_persistent_state(handle_utime, prev_utime);

            tracing::debug!(
                seq_no = handle.id().seqno,
                is_persistent,
                "new key block candidate",
            );

            // Skip not persistent
            if !is_persistent {
                tracing::debug!("ignoring state: not persistent");
                continue;
            }

            // Skip too new key blocks
            if handle_utime + INITIAL_SYNC_TIME_SECONDS > now_sec() {
                last_known_key_block = Some(handle);

                tracing::debug!("ignoring state: too new");
                continue;
            }

            // Use first suitable key block
            tracing::info!(block_id = %handle.id(), "found best key block handle");
            return Ok(handle);
        }

        let key_block = last_known_key_block.context("persistent shard state not found")?;

        tracing::warn!(
            seq_no = key_block.id().seqno,
            "starting from too new key block"
        );

        Ok(key_block)
    }

    async fn download_start_blocks_and_states(&self, mc_block_id: &BlockId) -> Result<()> {
        // Download and save masterchain block and state
        let (_, init_mc_block) = self
            .download_block_with_state(mc_block_id, mc_block_id)
            .await?;

        tracing::info!(
            block_id = %init_mc_block.id(),
            "downloaded init mc block state"
        );

        // Download and save blocks and states from other shards
        for (_, block_id) in init_mc_block.shard_blocks()? {
            self.download_block_with_state(mc_block_id, &block_id)
                .await?;
        }

        Ok(())
    }

    // === Helper methods ===

    async fn import_zerostates<P>(&self, provider: P) -> Result<(BlockHandle, ShardStateStuff)>
    where
        P: ZerostateProvider,
    {
        // Use a separate tracker for zerostates
        let tracker = MinRefMcStateTracker::default();

        // Read all zerostates
        let mut zerostates = FastHashMap::default();
        for loaded in provider.load_zerostates(&tracker) {
            let state = loaded?;
            if let Some(prev) = zerostates.insert(*state.block_id(), state) {
                anyhow::bail!("duplicate zerostate {}", prev.block_id());
            }
        }

        // Find the masterchain zerostate
        let zerostate_id = self.zerostate.as_block_id();
        let Some(masterchain_zerostate) = zerostates.remove(&zerostate_id) else {
            anyhow::bail!("missing mc zerostate for {zerostate_id}");
        };

        // Prepare the list of zerostates to import
        let mut to_import = vec![masterchain_zerostate.clone()];

        let global_id = masterchain_zerostate.state().global_id;
        let gen_utime = masterchain_zerostate.state().gen_utime;

        for entry in masterchain_zerostate.shards()?.iter() {
            let (shard_ident, descr) = entry.context("invalid mc zerostate")?;
            anyhow::ensure!(descr.seqno == 0, "invalid shard description {shard_ident}");

            let block_id = BlockId {
                shard: shard_ident,
                seqno: 0,
                root_hash: descr.root_hash,
                file_hash: descr.file_hash,
            };

            let state = match zerostates.remove(&block_id) {
                Some(existing) => {
                    tracing::debug!(block_id = %block_id, "using custom zerostate");
                    existing
                }
                None => {
                    tracing::debug!(block_id = %block_id, "creating default zerostate");
                    let state = make_shard_state(&tracker, global_id, shard_ident, gen_utime)
                        .context("failed to create shard zerostate")?;

                    anyhow::ensure!(
                        state.block_id() == &block_id,
                        "custom zerostate must be provided for {shard_ident}",
                    );

                    state
                }
            };

            to_import.push(state);
        }

        anyhow::ensure!(
            zerostates.is_empty(),
            "unused zerostates left: {}",
            zerostates.len()
        );

        // Import all zerostates
        let handle_storage = self.storage.block_handle_storage();
        let state_storage = self.storage.shard_state_storage();
        let persistent_state_storage = self.storage.persistent_state_storage();

        for state in to_import {
            let (handle, status) =
                handle_storage.create_or_load_handle(state.block_id(), BlockMetaData {
                    is_key_block: state.block_id().is_masterchain(),
                    gen_utime,
                    mc_ref_seqno: Some(0),
                });

            let stored = state_storage
                .store_state(&handle, &state)
                .await
                .with_context(|| {
                    format!("failed to import zerostate for {}", state.block_id().shard)
                })?;

            tracing::debug!(
                block_id = %state.block_id(),
                handle_status = ?status,
                stored,
                "importing zerostate"
            );

            persistent_state_storage
                .store_state(&handle, state.root_cell().repr_hash())
                .await?;
        }

        tracing::info!("imported zerostates");

        let state = state_storage.load_state(&zerostate_id).await?;
        let handle = handle_storage
            .load_handle(&zerostate_id)
            .expect("shouldn't happen");

        Ok((handle, state))
    }

    async fn download_zerostates(&self) -> Result<(BlockHandle, ShardStateStuff)> {
        let zerostate_id = self.zerostate.as_block_id();

        let (handle, state) = self.load_or_download_state(&zerostate_id).await?;

        for item in state.shards()?.latest_blocks() {
            let block_id = item?;
            let _state = self.load_or_download_state(&block_id).await?;
        }

        Ok((handle, state))
    }

    async fn load_or_download_state(
        &self,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, ShardStateStuff)> {
        let storage = &self.storage;
        let blockchain_rpc_client = &self.blockchain_rpc_client;

        let block_handles = storage.block_handle_storage();
        let persistent_states = storage.persistent_state_storage();

        let (handle, state) = match block_handles.load_handle(block_id) {
            Some(handle) if handle.meta().has_state() => {
                // Load state
                let shard_state_storage = storage.shard_state_storage();
                let state = shard_state_storage
                    .load_state(block_id)
                    .await
                    .context("Failed to load zerostate")?;

                (handle, state)
            }
            _ => {
                // Download state
                let state = blockchain_rpc_client
                    .download_and_store_state(block_id, storage.clone())
                    .await?;

                let (handle, _) = storage.block_handle_storage().create_or_load_handle(
                    block_id,
                    BlockMetaData::zero_state(state.state().gen_utime, block_id.is_masterchain()),
                );

                storage
                    .shard_state_storage()
                    .store_state(&handle, &state)
                    .await?;

                (handle, state)
            }
        };

        // TODO: Reuse downloaded state?
        persistent_states
            .store_state(&handle, state.root_cell().repr_hash())
            .await?;

        Ok((handle, state))
    }

    async fn download_block_with_state(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, BlockStuff)> {
        let block_storage = self.storage.block_storage();
        let block_handle_storage = self.storage.block_handle_storage();

        let mc_seqno = mc_block_id.seqno;

        let handle = block_handle_storage
            .load_handle(block_id)
            .filter(|handle| handle.meta().has_data());

        // Download block data and proof
        let (block, handle) = 'data: {
            if let Some(handle) = handle {
                break 'data (block_storage.load_block_data(&handle).await?, handle);
            }

            let blockchain_rpc_client = &self.blockchain_rpc_client;

            // TODO: add retry count to interrupt infinite loop
            let (block, proof, meta_data) = loop {
                let (handle, block_full) =
                    match blockchain_rpc_client.get_block_full(block_id).await {
                        Ok(res) => res.split(),
                        Err(e) => {
                            tracing::warn!(%block_id, "failed to download block: {e:?}");

                            // TODO: Backoff
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                match block_full {
                    BlockFull::Found {
                        block_id,
                        block: block_data,
                        proof: proof_data,
                        is_link,
                    } => {
                        let block = match BlockStuff::deserialize_checked(&block_id, &block_data) {
                            Ok(block) => WithArchiveData::new(block, block_data),
                            Err(e) => {
                                tracing::error!(%block_id, "failed to deserialize block: {e}");
                                handle.reject();
                                continue;
                            }
                        };

                        let proof = match BlockProofStuff::deserialize(
                            &block_id,
                            &proof_data,
                            is_link,
                        ) {
                            Ok(proof) => WithArchiveData::new(proof, proof_data),
                            Err(e) => {
                                tracing::error!(%block_id, "failed to deserialize block proof: {e}");
                                handle.reject();
                                continue;
                            }
                        };

                        match proof.pre_check_block_proof() {
                            Ok((_, block_info)) => {
                                break (block, proof, BlockMetaData {
                                    is_key_block: block_info.key_block,
                                    gen_utime: block_info.gen_utime,
                                    mc_ref_seqno: Some(mc_seqno),
                                });
                            }
                            Err(e) => {
                                tracing::error!("received invalid block: {e:?}");
                            }
                        }
                    }
                    BlockFull::NotFound => {
                        tracing::warn!(%block_id, "block not found");
                        handle.reject();
                    }
                }
            };

            tracing::info!(%block_id, "downloaded block data");

            let mut handle = block_storage
                .store_block_data(&block, &block.archive_data, meta_data)
                .await?
                .handle;

            if !handle.meta().has_proof() {
                handle = block_storage
                    .store_block_proof(&proof, handle.into())
                    .await?
                    .handle;
            }

            (block.data, handle)
        };

        // Download block state
        if !handle.meta().has_state() {
            let state_update = block.block().load_state_update()?;

            tracing::info!(block_id = %handle.id(), "downloading state");
            let (_, shard_state) = self.load_or_download_state(block_id).await?;
            tracing::info!(block_id = %handle.id(), "downloaded state");

            let state_hash = *shard_state.root_cell().repr_hash();
            anyhow::ensure!(
                state_update.new_hash == state_hash,
                "downloaded shard state hash mismatch"
            );
        }

        Ok((handle, block))
    }
}

async fn download_block_proof_task(
    storage: Storage,
    rpc_client: BlockchainRpcClient,
    block_id: BlockId,
) -> BlockProofStuffAug {
    let block_storage = storage.block_storage();
    let block_handle_storage = storage.block_handle_storage();

    // Check whether block proof is already stored locally
    if let Some(handle) = block_handle_storage.load_handle(&block_id) {
        if let Ok(proof) = block_storage.load_block_proof(&handle, false).await {
            return WithArchiveData::loaded(proof);
        }
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

                match BlockProofStuff::deserialize(&block_id, &data, false) {
                    Ok(proof) => {
                        handle.accept();
                        return WithArchiveData::new(proof, data);
                    }
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

        // TODO: Backoff
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn make_shard_state(
    tracker: &MinRefMcStateTracker,
    global_id: i32,
    shard_ident: ShardIdent,
    now: u32,
) -> Result<ShardStateStuff> {
    let state = ShardStateUnsplit {
        global_id,
        shard_ident,
        gen_utime: now,
        min_ref_mc_seqno: u32::MAX,
        ..Default::default()
    };

    let root = CellBuilder::build_from(&state)?;
    let root_hash = *root.repr_hash();
    let file_hash = Boc::file_hash_blake(Boc::encode(&root));

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    ShardStateStuff::from_root(&block_id, root, tracker)
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

    fn check_next_proof(&self, next_proof: &BlockProofStuff) -> Result<BlockMetaData> {
        let (virt_block, virt_block_info) = next_proof
            .pre_check_block_proof()
            .context("Failed to pre check block proof")?;

        let res = BlockMetaData {
            is_key_block: virt_block_info.key_block,
            gen_utime: virt_block_info.gen_utime,
            mc_ref_seqno: Some(next_proof.proof().proof_for.seqno),
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

const INITIAL_SYNC_TIME_SECONDS: u32 = 300;
