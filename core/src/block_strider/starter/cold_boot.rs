use std::fs::File;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tycho_block_util::archive::{ArchiveData, WithArchiveData};
use tycho_block_util::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::fs::FileBuilder;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;
use tycho_util::sync::rayon_run;
use tycho_util::time::now_sec;

use super::{ColdBootType, StarterInner, ZerostateProvider};
use crate::block_strider::{CheckProof, ProofChecker};
use crate::blockchain_rpc::{BlockchainRpcClient, DataRequirement};
use crate::overlay_client::PunishReason;
use crate::proto::blockchain::KeyBlockProof;
use crate::storage::{
    BlockHandle, CoreStorage, KeyBlocksDirection, MaybeExistingHandle, NewBlockMeta,
    PersistentStateKind,
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(cold_boot)),
            file!(),
            line!(),
        );
        tracing::info!("started");
        let last_mc_block_id = match boot_type {
            ColdBootType::Genesis => {
                let node_state = self.storage.node_state();
                if let Some(init_mc_block) = node_state.load_init_mc_block_id() {
                    anyhow::ensure!(
                        init_mc_block.seqno == 0,
                        "cold boot type cannot be changed after partial boot, \
                        you must reset node state for that",
                    );
                }
                let init_block = {
                    __guard.end_section(line!());
                    let __result = self.prepare_init_block(zerostates).await;
                    __guard.start_section(line!());
                    __result
                }?;
                assert_eq!(init_block.handle().id().seqno, 0);
                *init_block.handle().id()
            }
            ColdBootType::LatestPersistent => {
                let init_block = {
                    __guard.end_section(line!());
                    let __result = self.prepare_init_block(zerostates).await;
                    __guard.start_section(line!());
                    __result
                }?;
                {
                    __guard.end_section(line!());
                    let __result = self.download_key_blocks(init_block).await;
                    __guard.start_section(line!());
                    __result
                }?;
                let last_key_block = self.choose_key_block()?;
                if last_key_block.id().seqno != 0 {
                    {
                        __guard.end_section(line!());
                        let __result = self
                            .download_start_blocks_and_states(last_key_block.id())
                            .await;
                        __guard.start_section(line!());
                        __result
                    }?;
                }
                *last_key_block.id()
            }
        };
        self.storage
            .node_state()
            .store_last_mc_block_id(&last_mc_block_id);
        tracing::info!(last_mc_block_id = % last_mc_block_id, "finished");
        Ok(last_mc_block_id)
    }
    /// Prepare the initial block to start syncing.
    async fn prepare_init_block<P>(&self, zerostates: Option<P>) -> Result<InitBlock>
    where
        P: ZerostateProvider,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(prepare_init_block)),
            file!(),
            line!(),
        );
        let node_state = self.storage.node_state();
        let block_id = node_state
            .load_init_mc_block_id()
            .unwrap_or(self.zerostate.as_block_id());
        tracing::info!(init_block_id = % block_id, "preparing init block");
        let prev_key_block = if block_id.seqno == 0 {
            tracing::info!(% block_id, "using zero state");
            let (handle, state) = match zerostates {
                Some(zerostates) => {
                    __guard.end_section(line!());
                    let __result = self.import_zerostates(zerostates).await;
                    __guard.start_section(line!());
                    __result
                }?,
                None => {
                    __guard.end_section(line!());
                    let __result = self.download_zerostates().await;
                    __guard.start_section(line!());
                    __result
                }?,
            };
            node_state.store_init_mc_block_id(handle.id());
            InitBlock::ZeroState {
                handle: Arc::new(handle),
                state: Arc::new(state),
            }
        } else {
            tracing::info!(% block_id, "using key block");
            let handle = self
                .storage
                .block_handle_storage()
                .load_handle(&block_id)
                .expect("shouldn't happen");
            let proof = {
                __guard.end_section(line!());
                let __result = self.storage.block_storage().load_block_proof(&handle).await;
                __guard.start_section(line!());
                __result
            }?;
            InitBlock::KeyBlock {
                handle: Arc::new(handle),
                proof: Box::new(proof),
            }
        };
        Ok(prev_key_block)
    }
    /// Download all key blocks since the initial block.
    async fn download_key_blocks(&self, mut prev_key_block: InitBlock) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_key_blocks)),
            file!(),
            line!(),
        );
        const BLOCKS_PER_BATCH: u32 = 10;
        const PARALLEL_REQUESTS: usize = 10;
        let (ids_tx, mut ids_rx) = mpsc::unbounded_channel();
        let (tasks_tx, mut tasks_rx) = mpsc::unbounded_channel();
        tokio::spawn({
            let blockchain_rpc_client = self.blockchain_rpc_client.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    line!(),
                );
                while let Some(block_id) = {
                    __guard.end_section(line!());
                    let __result = tasks_rx.recv().await;
                    __guard.start_section(line!());
                    __result
                } {
                    'inner: loop {
                        tracing::debug!(% block_id, "start downloading next key blocks");
                        let res = {
                            __guard.end_section(line!());
                            let __result = blockchain_rpc_client
                                .get_next_key_block_ids(&block_id, BLOCKS_PER_BATCH)
                                .await;
                            __guard.start_section(line!());
                            __result
                        };
                        match res {
                            Ok(res) => {
                                let (handle, data) = res.split();
                                handle.accept();
                                if ids_tx.send((block_id, data.block_ids)).is_err() {
                                    tracing::debug!(
                                        % block_id, "stop downloading next key blocks"
                                    );
                                    return;
                                }
                                break 'inner;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    % block_id, "failed to download key block ids: {e:?}"
                                );
                                {
                                    __guard.end_section(line!());
                                    let __result = tokio::time::sleep(Duration::from_secs(1)).await;
                                    __guard.start_section(line!());
                                    __result
                                };
                            }
                        }
                    }
                }
            }
        });
        tasks_tx.send(*prev_key_block.handle().id())?;
        let satisfies_offset = |gen_utime: u32, now_utime: u32| match self.config.custom_boot_offset
        {
            None => BlockStuff::can_use_for_boot(gen_utime, now_utime),
            Some(t) => now_utime.saturating_sub(gen_utime) as u64 >= t.as_secs(),
        };
        let mut retry_counter = 0usize;
        while let Some((requested_key_block, ids)) = {
            __guard.end_section(line!());
            let __result = ids_rx.recv().await;
            __guard.start_section(line!());
            __result
        } {
            let stream = futures_util::stream::iter(ids)
                .map(|block_id| {
                    JoinTask::new(download_block_proof_task(
                        self.storage.clone(),
                        self.blockchain_rpc_client.clone(),
                        block_id,
                    ))
                })
                .buffered(PARALLEL_REQUESTS);
            let mut proofs = {
                __guard.end_section(line!());
                let __result = stream.collect::<Vec<_>>().await;
                __guard.start_section(line!());
                __result
            };
            proofs.sort_by_key(|x| *x.id());
            let fallback_key_block = prev_key_block.clone();
            let now_utime = now_sec();
            let mut has_newer = false;
            let proofs_len = proofs.len();
            for (index, proof) in proofs.into_iter().enumerate() {
                match prev_key_block.check_next_proof(&proof.data) {
                    Ok(meta) if satisfies_offset(meta.gen_utime, now_utime) => {
                        let handle = {
                            __guard.end_section(line!());
                            let __result = self
                                .storage
                                .block_storage()
                                .store_block_proof(&proof, MaybeExistingHandle::New(meta))
                                .await;
                            __guard.start_section(line!());
                            __result
                        }?
                        .handle;
                        let block_utime = handle.gen_utime();
                        let prev_utime = prev_key_block.handle().gen_utime();
                        if BlockStuff::compute_is_persistent(block_utime, prev_utime) {
                            self.storage
                                .node_state()
                                .store_init_mc_block_id(handle.id());
                        }
                        if index == proofs_len.saturating_sub(1) {
                            tasks_tx.send(*proof.data.id())?;
                        }
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
                        tasks_tx.send(*fallback_key_block.handle().id())?;
                        prev_key_block = fallback_key_block;
                        break;
                    }
                }
            }
            let last_utime = prev_key_block.handle().gen_utime();
            let no_proofs = proofs_len == 0;
            tracing::debug!(
                now_utime, last_utime, last_known_block_id = % prev_key_block.handle()
                .id(),
            );
            if has_newer || no_proofs && retry_counter >= MAX_EMPTY_PROOF_RETRIES {
                break;
            }
            if no_proofs {
                retry_counter += 1;
                tracing::warn!(
                    attempt = retry_counter, block_id = % requested_key_block,
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
        while let Some(handle) = key_blocks.next().transpose()? {
            let handle_utime = handle.gen_utime();
            let prev_utime = match key_blocks.peek() {
                Some(Ok(prev_block)) => prev_block.gen_utime(),
                Some(Err(e)) => anyhow::bail!("failed to load previous key block: {e:?}"),
                None => 0,
            };
            let is_persistent = BlockStuff::compute_is_persistent(handle_utime, prev_utime);
            if !is_persistent {
                tracing::debug!(seq_no = handle.id().seqno, "skipping key block");
                continue;
            }
            tracing::info!(block_id = % handle.id(), "found best key block handle");
            return Ok(handle);
        }
        anyhow::bail!("no suitable key block found")
    }
    async fn download_start_blocks_and_states(&self, mc_block_id: &BlockId) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(),
                "::",
                stringify!(download_start_blocks_and_states)
            ),
            file!(),
            line!(),
        );
        let (_, init_mc_block) = {
            __guard.end_section(line!());
            let __result = self
                .download_block_with_states(mc_block_id, mc_block_id)
                .await;
            __guard.start_section(line!());
            __result
        }?;
        tracing::info!(
            block_id = % init_mc_block.id(), "downloaded init mc block state"
        );
        for (_, block_id) in init_mc_block.shard_blocks()? {
            let (handle, _) = {
                __guard.end_section(line!());
                let __result = self
                    .download_block_with_states(mc_block_id, &block_id)
                    .await;
                __guard.start_section(line!());
                __result
            }?;
            self.storage
                .block_handle_storage()
                .set_block_committed(&handle);
        }
        Ok(())
    }
    async fn import_zerostates<P>(&self, provider: P) -> Result<(BlockHandle, ShardStateStuff)>
    where
        P: ZerostateProvider,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(import_zerostates)),
            file!(),
            line!(),
        );
        tracing::info!("import zerostates");
        let state_storage = self.storage.shard_state_storage();
        let tracker = state_storage.min_ref_mc_state();
        let mut zerostates = FastHashMap::default();
        for loaded in provider.load_zerostates(tracker) {
            let state = loaded?;
            if let Some(prev) = zerostates.insert(*state.block_id(), state) {
                anyhow::bail!("duplicate zerostate {}", prev.block_id());
            }
        }
        let zerostate_id = self.zerostate.as_block_id();
        let Some(masterchain_zerostate) = zerostates.remove(&zerostate_id) else {
            anyhow::bail!("missing mc zerostate for {zerostate_id}");
        };
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
                    tracing::debug!(block_id = % block_id, "using custom zerostate");
                    existing
                }
                None => {
                    tracing::debug!(block_id = % block_id, "creating default zerostate");
                    let state = make_shard_state(tracker, global_id, shard_ident, gen_utime)
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
        let handle_storage = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();
        for state in to_import {
            let (handle, status) =
                handle_storage.create_or_load_handle(state.block_id(), NewBlockMeta {
                    is_key_block: state.block_id().is_masterchain(),
                    gen_utime,
                    ref_by_mc_seqno: 0,
                });
            let stored = {
                __guard.end_section(line!());
                let __result = state_storage
                    .store_state(&handle, &state, Default::default())
                    .await;
                __guard.start_section(line!());
                __result
            }
            .with_context(|| {
                format!("failed to import zerostate for {}", state.block_id().shard)
            })?;
            tracing::debug!(
                block_id = % state.block_id(), handle_status = ? status, stored,
                "importing zerostate"
            );
            {
                __guard.end_section(line!());
                let __result = persistent_states
                    .store_shard_state(0, &handle, state.ref_mc_state_handle().clone())
                    .await;
                __guard.start_section(line!());
                __result
            }?;
        }
        tracing::info!("imported zerostates");
        let state = {
            __guard.end_section(line!());
            let __result = state_storage.load_state(&zerostate_id).await;
            __guard.start_section(line!());
            __result
        }?;
        let handle = handle_storage
            .load_handle(&zerostate_id)
            .expect("shouldn't happen");
        Ok((handle, state))
    }
    async fn download_zerostates(&self) -> Result<(BlockHandle, ShardStateStuff)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_zerostates)),
            file!(),
            line!(),
        );
        let zerostate_id = self.zerostate.as_block_id();
        tracing::info!(zerostate_id = % zerostate_id, "download zerostates");
        let (handle, state) = {
            __guard.end_section(line!());
            let __result = self
                .download_shard_state(&zerostate_id, &zerostate_id)
                .await;
            __guard.start_section(line!());
            __result
        }?;
        for item in state.shards()?.latest_blocks() {
            let block_id = item?;
            let _state = {
                __guard.end_section(line!());
                let __result = self.download_shard_state(&zerostate_id, &block_id).await;
                __guard.start_section(line!());
                __result
            }?;
        }
        Ok((handle, state))
    }
    async fn download_block_with_states(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, BlockStuff)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_block_with_states)),
            file!(),
            line!(),
        );
        let (handle, block) = {
            __guard.end_section(line!());
            let __result = self.download_block_data(mc_block_id, block_id).await;
            __guard.start_section(line!());
            __result
        }?;
        self.storage
            .block_handle_storage()
            .set_block_persistent(&handle);
        {
            let state_update = block.as_ref().load_state_update()?;
            let (_, shard_state) = {
                __guard.end_section(line!());
                let __result = self.download_shard_state(mc_block_id, block_id).await;
                __guard.start_section(line!());
                __result
            }?;
            let state_hash = *shard_state.root_cell().repr_hash();
            anyhow::ensure!(
                state_update.new_hash == state_hash,
                "downloaded shard state hash mismatch"
            );
        }
        if block_id.seqno != 0 {
            let top_update = &block.as_ref().out_msg_queue_updates;
            {
                __guard.end_section(line!());
                let __result = self.download_queue_state(&handle, top_update).await;
                __guard.start_section(line!());
                __result
            }?;
        }
        Ok((handle, block))
    }
    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    async fn download_block_data(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, BlockStuff)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_block_data)),
            file!(),
            line!(),
        );
        let rpc = &self.blockchain_rpc_client;
        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let block_handle = block_handles.load_handle(block_id);
        if let Some(handle) = &block_handle {
            if handle.has_data() {
                let block = {
                    __guard.end_section(line!());
                    let __result = blocks.load_block_data(handle).await;
                    __guard.start_section(line!());
                    __result
                }?;
                tracing::info!("using the stored block");
                return Ok((handle.clone(), block));
            }
        }
        let proof_checker = ProofChecker::new(self.storage.clone());
        'outer: loop {
            let (full, neighbour) = 'res: {
                match {
                    __guard.end_section(line!());
                    let __result = rpc
                        .get_block_full(block_id, DataRequirement::Expected)
                        .await;
                    __guard.start_section(line!());
                    __result
                } {
                    Ok(res) => match res.data {
                        Some(data) if &data.block_id == block_id => {
                            break 'res (data, res.neighbour);
                        }
                        Some(_) => {
                            res.neighbour.punish(PunishReason::Malicious);
                            tracing::warn!("received block id mismatch");
                        }
                        None => tracing::warn!("block not found"),
                    },
                    Err(e) => tracing::warn!("failed to download block: {e:?}"),
                }
                {
                    __guard.end_section(line!());
                    let __result = tokio::time::sleep(Duration::from_millis(100)).await;
                    __guard.start_section(line!());
                    __result
                };
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
            let (block_stuff, (block_proof, queue_diff)) = {
                __guard.end_section(line!());
                let __result = futures_util::future::join(block_stuff_fut, other_data_fut).await;
                __guard.start_section(line!());
                __result
            };
            match (block_stuff, block_proof, queue_diff) {
                (Ok(block), Ok(proof), Ok(diff)) => {
                    let proof = WithArchiveData::new(proof, full.proof_data);
                    let diff = WithArchiveData::new(diff, full.queue_diff_data);
                    match {
                        __guard.end_section(line!());
                        let __result = proof_checker
                            .check_proof(CheckProof {
                                mc_block_id,
                                block: &block,
                                proof: &proof,
                                queue_diff: &diff,
                                store_on_success: true,
                            })
                            .await;
                        __guard.start_section(line!());
                        __result
                    } {
                        Ok(meta) => {
                            let archive_data = ArchiveData::New(full.block_data);
                            let res = {
                                __guard.end_section(line!());
                                let __result =
                                    blocks.store_block_data(&block, &archive_data, meta).await;
                                __guard.start_section(line!());
                                __result
                            }?;
                            tracing::info!("using the downloaded block");
                            return Ok((res.handle, block));
                        }
                        Err(e) => {
                            neighbour.punish(PunishReason::Malicious);
                            tracing::error!("got invalid block proof: {e}");
                        }
                    }
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                    neighbour.punish(PunishReason::Malicious);
                    tracing::error!("failed to deserialize shard block or block proof: {e}");
                }
            }
            {
                __guard.end_section(line!());
                let __result = tokio::time::sleep(Duration::from_millis(100)).await;
                __guard.start_section(line!());
                __result
            };
        }
    }
    #[tracing::instrument(skip_all, fields(block_id = %block_id))]
    async fn download_shard_state(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<(BlockHandle, ShardStateStuff)> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_shard_state)),
            file!(),
            line!(),
        );
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
        let remove_state_file = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            if let Err(e) = {
                __guard.end_section(line!());
                let __result = tokio::fs::remove_file(&state_file_path).await;
                __guard.start_section(line!());
                __result
            } {
                tracing::warn!(
                    path = % state_file_path.display(),
                    "failed to remove downloaded shard state: {e:?}",
                );
            }
        };
        let mc_seqno = mc_block_id.seqno;
        let try_save_persistent = |block_handle: &BlockHandle, from: StoreZeroStateFrom| {
            let block_handle = block_handle.clone();
            async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    line!(),
                );
                match from {
                    StoreZeroStateFrom::File(mut state_file) => {
                        let state_file = state_file.read(true).open()?;
                        {
                            __guard.end_section(line!());
                            let __result = persistent_states
                                .store_shard_state_file(mc_seqno, &block_handle, state_file)
                                .await;
                            __guard.start_section(line!());
                            __result
                        }
                    }
                    StoreZeroStateFrom::State(state) => {
                        __guard.end_section(line!());
                        let __result = persistent_states
                            .store_shard_state(
                                mc_seqno,
                                &block_handle,
                                state.ref_mc_state_handle().clone(),
                            )
                            .await;
                        __guard.start_section(line!());
                        __result
                    }
                }
            }
        };
        let block_handle = block_handles.load_handle(block_id);
        if let Some(handle) = &block_handle
            && handle.has_state()
        {
            let state = {
                __guard.end_section(line!());
                let __result = shard_states.load_state(block_id).await;
                __guard.start_section(line!());
                __result
            }?;
            if !handle.has_persistent_shard_state() {
                let from = if state_file.exists() {
                    StoreZeroStateFrom::File(state_file)
                } else {
                    StoreZeroStateFrom::State(state.clone())
                };
                if let Err(e) = {
                    __guard.end_section(line!());
                    let __result = try_save_persistent(handle, from).await;
                    __guard.start_section(line!());
                    __result
                } {
                    tracing::error!(
                        % block_id, "failed to store persistent shard state: {e:?}"
                    );
                }
            }
            {
                __guard.end_section(line!());
                let __result = remove_state_file.await;
                __guard.start_section(line!());
                __result
            };
            tracing::info!("using the stored shard state");
            return Ok((handle.clone(), state));
        }
        for attempt in 0..MAX_PERSISTENT_STATE_RETRIES {
            let file = match {
                __guard.end_section(line!());
                let __result = self
                    .download_persistent_state_file(
                        block_id,
                        PersistentStateKind::Shard,
                        &state_file,
                    )
                    .await;
                __guard.start_section(line!());
                __result
            } {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(attempt, "failed to download persistent shard state: {e}");
                    continue;
                }
            };
            let state = {
                __guard.end_section(line!());
                let __result = shard_states.store_state_file(block_id, file).await;
                __guard.start_section(line!());
                __result
            }?;
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
            if let Err(e) = {
                __guard.end_section(line!());
                let __result = try_save_persistent(&block_handle, from).await;
                __guard.start_section(line!());
                __result
            } {
                tracing::error!("failed to store persistent shard state: {e:?}");
            }
            {
                __guard.end_section(line!());
                let __result = remove_state_file.await;
                __guard.start_section(line!());
                __result
            };
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_queue_state)),
            file!(),
            line!(),
        );
        let block_id = block_handle.id();
        let temp = self.storage.context().temp_files();
        let state_file = temp.file(format!("queue_state_{block_id}"));
        let state_file_path = state_file.path().to_owned();
        let remove_state_file = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            if let Err(e) = {
                __guard.end_section(line!());
                let __result = tokio::fs::remove_file(&state_file_path).await;
                __guard.start_section(line!());
                __result
            } {
                tracing::warn!(
                    path = % state_file_path.display(),
                    "failed to remove downloaded queue state: {e:?}",
                );
            }
        };
        for attempt in 0..MAX_PERSISTENT_STATE_RETRIES {
            let file = match {
                __guard.end_section(line!());
                let __result = self
                    .download_persistent_state_file(
                        block_id,
                        PersistentStateKind::Queue,
                        &state_file,
                    )
                    .await;
                __guard.start_section(line!());
                __result
            } {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(attempt, "failed to download persistent queue state: {e}");
                    continue;
                }
            };
            if let Some(queue_state_handler) = &self.queue_state_handler {
                {
                    __guard.end_section(line!());
                    let __result = queue_state_handler
                        .import_from_file(top_update, file, block_id)
                        .await;
                    __guard.start_section(line!());
                    __result
                }?;
            }
            {
                __guard.end_section(line!());
                let __result = remove_state_file.await;
                __guard.start_section(line!());
                __result
            };
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(),
                "::",
                stringify!(download_persistent_state_file)
            ),
            file!(),
            line!(),
        );
        let mut temp_file = state_file.with_extension("temp");
        let temp_file_path = temp_file.path().to_owned();
        scopeguard::defer! {
            std::fs::remove_file(temp_file_path).ok();
        };
        let rpc = &self.blockchain_rpc_client;
        loop {
            if state_file.exists() {
                return state_file.clone().read(true).open();
            }
            let pending_state = {
                __guard.end_section(line!());
                let __result = rpc.find_persistent_state(block_id, kind).await;
                __guard.start_section(line!());
                __result
            }?;
            let output = temp_file.write(true).create(true).truncate(true).open()?;
            {
                __guard.end_section(line!());
                let __result = rpc.download_persistent_state(pending_state, output).await;
                __guard.start_section(line!());
                __result
            }?;
            {
                __guard.end_section(line!());
                let __result = tokio::fs::rename(temp_file.path(), state_file.path()).await;
                __guard.start_section(line!());
                __result
            }?;
        }
    }
}
async fn download_block_proof_task(
    storage: CoreStorage,
    rpc_client: BlockchainRpcClient,
    block_id: BlockId,
) -> BlockProofStuffAug {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(download_block_proof_task)),
        file!(),
        line!(),
    );
    let block_storage = storage.block_storage();
    let block_handle_storage = storage.block_handle_storage();
    if let Some(handle) = block_handle_storage.load_handle(&block_id)
        && let Ok(proof) = {
            __guard.end_section(line!());
            let __result = block_storage.load_block_proof(&handle).await;
            __guard.start_section(line!());
            __result
        }
    {
        return WithArchiveData::loaded(proof);
    }
    loop {
        let res = {
            __guard.end_section(line!());
            let __result = rpc_client.get_key_block_proof(&block_id).await;
            __guard.start_section(line!());
            __result
        };
        match res {
            Ok(res) => 'validate: {
                let (handle, data) = res.split();
                let KeyBlockProof::Found { proof: data } = data else {
                    tracing::debug!(% block_id, "block proof not found");
                    handle.accept();
                    break 'validate;
                };
                match BlockProofStuff::deserialize(&block_id, &data) {
                    Ok(proof) => {
                        handle.accept();
                        return WithArchiveData::new(proof, data);
                    }
                    Err(e) => {
                        tracing::error!(
                            % block_id, "failed to deserialize block proof: {e}"
                        );
                        handle.reject();
                    }
                }
            }
            Err(e) => {
                tracing::warn!(% block_id, "failed to download block proof: {e:?}");
            }
        }
        {
            __guard.end_section(line!());
            let __result = tokio::time::sleep(Duration::from_millis(100)).await;
            __guard.start_section(line!());
            __result
        };
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
            InitBlock::ZeroState { state, .. } => tycho_block_util::block::check_with_master_state(
                next_proof,
                state,
                &virt_block,
                &virt_block_info,
            ),
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
