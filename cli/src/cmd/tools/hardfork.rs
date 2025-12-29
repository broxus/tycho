use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use clap::Parser;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::ZerostateId;
use tycho_core::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta, ShardStateWriter};
use tycho_storage::fs::Dir;
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::cell::Lazy;
use tycho_types::dict::AugDict;
use tycho_types::models::{
    BlockId, BlockIdShort, ConsensusInfo, GenesisInfo, McStateExtra, ShardHashes, ShardIdent,
    ShardStateUnsplit, ValidatorInfo,
};
use tycho_types::prelude::*;
use tycho_util::cli::signal;
use tycho_util::progress_bar::ProgressBar;
use tycho_util::sync::CancellationFlag;

use crate::util::print_json;

/// Saves masterchain and shardchain states to files to run network from
#[derive(Parser)]
pub struct Cmd {
    /// Path to the node's database directory.
    #[clap()]
    db: PathBuf,

    /// Seqno of the masterchain block for which to dump the states
    #[clap()]
    mc_seqno: u32,

    /// Path to the directory where the dump files will be saved.
    #[clap(long)]
    output: PathBuf,

    /// Override global id.
    #[clap(long)]
    global_id: Option<i32>,

    /// Override time (in milliseconds).
    #[clap(long)]
    time: Option<u64>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env_lossy(),
            )
            .init();

        let fut = async move {
            let ctx = StorageContext::new(StorageConfig {
                root_dir: self.db,
                rocksdb_enable_metrics: false,
                rocksdb_lru_capacity: ByteSize::mib(256),
            })
            .await?;

            let storage = CoreStorage::open(ctx, CoreStorageConfig::default().without_gc()).await?;

            let Some(mc_block_id) = storage
                .shard_state_storage()
                .load_mc_block_id(self.mc_seqno)
                .with_context(|| format!("mc block not found. seqno {}", self.mc_seqno))?
            else {
                anyhow::bail!("mc block not found. seqno {}", self.mc_seqno)
            };

            let state = storage
                .shard_state_storage()
                .load_state(self.mc_seqno, &mc_block_id)
                .await?;

            let handler = ShardStateHandler {
                gen_utime: self.time.unwrap_or_else(tycho_util::time::now_millis),
                global_id: self.global_id,
                storage: storage.clone(),
                output_path: Dir::new(self.output)?,
            };

            let mut shard_states = HashMap::new();
            for entry in state.shards()?.latest_blocks() {
                let block_id = entry?;
                let state = storage
                    .shard_state_storage()
                    .load_state(self.mc_seqno, &block_id)
                    .await?;

                let hashes = handler.save_sc_state(&mc_block_id, state).await?;
                shard_states.insert(block_id.shard, hashes);
            }

            let hashes = handler.save_mc_state(&state, &shard_states).await?;

            print_json(serde_json::json!({
                "zerostate": ZerostateId {
                    seqno: mc_block_id.seqno,
                    root_hash: hashes.root_hash,
                    file_hash: hashes.file_hash,
                },
            }))
        };

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(signal::run_or_terminate(fut))
    }
}

struct ShardStateHandler {
    global_id: Option<i32>,
    gen_utime: u64,
    storage: CoreStorage,
    output_path: Dir,
}

impl ShardStateHandler {
    async fn save_mc_state(
        &self,
        state: &ShardStateStuff,
        shard_hashes: &HashMap<ShardIdent, NewHashes>,
    ) -> Result<NewHashes> {
        let mc_seqno = state.as_ref().seqno;

        let mut ssu = state.root_cell().parse::<ShardStateUnsplit>()?;
        self.prepare_zerostate(&mut ssu)?;

        let Some(custom) = ssu.load_custom()? else {
            anyhow::bail!("no mc state extra found in mc shard state");
        };

        let mut shards = Vec::new();
        for i in custom.shards.iter() {
            let (ident, mut shard_description) = i?;
            let Some(hashes) = shard_hashes.get(&ident) else {
                anyhow::bail!("new hashes not found for: {ident}");
            };

            shard_description.reg_mc_seqno = mc_seqno;
            shard_description.root_hash = hashes.root_hash;
            shard_description.file_hash = hashes.file_hash;
            shard_description.nx_cc_updated = true;
            shard_description.next_catchain_seqno = 0;
            shard_description.ext_processed_to_anchor_id = 0;
            shard_description.min_ref_mc_seqno = u32::MAX;
            shard_description.gen_utime = ssu.gen_utime;

            shards.push((ident, shard_description));
        }

        let curr_vset = custom.config.params.get_current_validator_set()?;
        let collation_config = custom.config.params.get_collation_config()?;
        let session_seqno = 0;
        let Some((_, validator_list_hash_short)) =
            curr_vset.compute_mc_subset(session_seqno, collation_config.shuffle_mc_validators)
        else {
            anyhow::bail!("Failed to compute a validator subset for zerostate");
        };

        ssu.custom = Some(Lazy::new(&McStateExtra {
            shards: ShardHashes::from_shards(shards.iter().map(|(ident, descr)| (ident, descr)))?,
            config: custom.config,
            validator_info: ValidatorInfo {
                validator_list_hash_short,
                catchain_seqno: session_seqno,
                nx_cc_updated: true,
            },
            consensus_info: ConsensusInfo {
                vset_switch_round: session_seqno,
                prev_vset_switch_round: session_seqno,
                genesis_info: GenesisInfo {
                    start_round: 0,
                    genesis_millis: (ssu.gen_utime as u64) * 1000,
                },
                prev_shuffle_mc_validators: collation_config.shuffle_mc_validators,
            },
            prev_blocks: AugDict::new(),
            after_key_block: true,
            last_key_block: None,
            block_create_stats: None,
            global_balance: ssu.total_balance.clone(),
        })?);

        let root = CellBuilder::build_from(ssu)?;

        self.save_state(
            mc_seqno,
            BlockIdShort {
                shard: ShardIdent::MASTERCHAIN,
                seqno: mc_seqno,
            },
            root,
        )
        .await
    }

    async fn save_sc_state(
        &self,
        mc_block_id: &BlockId,
        state: ShardStateStuff,
    ) -> Result<NewHashes> {
        let block_id = state.block_id().as_short_id();

        let mut ssu = state.root_cell().parse::<ShardStateUnsplit>()?;
        self.prepare_zerostate(&mut ssu)?;

        let root = CellBuilder::build_from(ssu)?;
        self.save_state(mc_block_id.seqno, block_id, root).await
    }

    async fn save_state(
        &self,
        mc_seqno: u32,
        block_id: BlockIdShort,
        root: Cell,
    ) -> Result<NewHashes> {
        // NOTE: Handle is created as a phantom block at the same height,
        // so that if the node is restarted, its GC could be able to delete
        // this entry. As a source of uniqueness we use the root hash of
        // the modified shard state.
        let (handle, _) = self.storage.block_handle_storage().create_or_load_handle(
            &BlockId {
                shard: block_id.shard,
                seqno: block_id.seqno,
                root_hash: *root.repr_hash(),
                file_hash: HashBytes::ZERO,
            },
            NewBlockMeta {
                is_key_block: false,
                gen_utime: (self.gen_utime / 1000) as u32,
                ref_by_mc_seqno: mc_seqno,
            },
        );

        self.storage
            .shard_state_storage()
            .store_state_root(&handle, root, Default::default())
            .await?;

        let cancelled = CancellationFlag::new();
        scopeguard::defer! {
            cancelled.cancel();
        }

        let cancelled = cancelled.clone();
        let storage = self.storage.clone();
        let output = self.output_path.clone();
        tokio::task::spawn_blocking(move || {
            let writer = ShardStateWriter::new(
                storage.shard_state_storage().cell_storage().db(),
                &output,
                handle.id(),
            );

            let mut progress_bar = ProgressBar::builder()
                .exact_unit("bytes")
                .build(move |msg| {
                    tracing::info!(
                        shard = %block_id.shard,
                        "saving shard state to file... {msg}"
                    );
                });

            let root_hash = handle.id().root_hash;
            let file_hash = writer.write_tracked(
                &root_hash,
                &block_id.shard.to_string(),
                &mut progress_bar,
                Some(&cancelled),
            )?;
            progress_bar.complete();

            tracing::info!(
                %block_id,
                %root_hash,
                %file_hash,
                "saved shard state"
            );

            Ok(NewHashes {
                root_hash,
                file_hash,
            })
        })
        .await?
    }

    fn prepare_zerostate(&self, state: &mut ShardStateUnsplit) -> Result<()> {
        let state_time = state.gen_utime as u64 * 1000 + state.gen_utime_ms as u64;
        anyhow::ensure!(
            state_time <= self.gen_utime,
            "cannot make hardfork in the past"
        );

        state.global_id = self.global_id.unwrap_or(state.global_id);
        state.gen_utime = (self.gen_utime / 1000) as u32;
        state.gen_utime_ms = (self.gen_utime % 1000) as u16;
        state.min_ref_mc_seqno = u32::MAX;
        state.processed_upto = ShardStateUnsplit::empty_processed_upto_info().clone();
        state.master_ref = None;
        state.overload_history = 0;

        Ok(())
    }
}

struct NewHashes {
    root_hash: HashBytes,
    file_hash: HashBytes,
}
