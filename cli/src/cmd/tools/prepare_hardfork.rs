use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::ZerostateId;
use tycho_core::storage::{
    BlockHandle, BlockMeta, CoreStorage, CoreStorageConfig, NewBlockMeta, ShardStateWriter,
};
use tycho_storage::fs::Dir;
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::cell::{CellSlice, HashBytes, Lazy};
use tycho_types::dict::AugDict;
use tycho_types::models::{
    BlockId, ConsensusInfo, GenesisInfo, McStateExtra, ShardHashes, ShardIdent, ShardStateUnsplit,
    ValidatorInfo,
};
use tycho_types::prelude::{Cell, CellBuilder, CellFamily, Load, Store};
use tycho_util::progress_bar::ProgressBar;

/// Saves masterchain and shardchain states to files to run network from
#[derive(Parser)]
pub struct Cmd {
    /// Path to the node's database directory.
    #[clap(long)]
    db: Option<PathBuf>,

    /// Path to the directory where the dump files will be saved.
    #[clap(long)]
    output: Option<PathBuf>,

    /// Seqno of the masterchain block for which to dump the states
    #[clap(short, long)]
    mc_seqno: u32,
}

impl Cmd {
    pub fn run(self) -> anyhow::Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(self.run_impl())
    }

    async fn run_impl(self) -> anyhow::Result<()> {
        let db_path = self.db.unwrap_or_default();

        let ctx = StorageContext::new(StorageConfig {
            root_dir: db_path,
            rocksdb_enable_metrics: false,
            rocksdb_lru_capacity: ByteSize::mib(256),
        })
        .await?;

        let storage = CoreStorage::open(ctx, CoreStorageConfig::default()).await?;

        let Some(mc_block_id) = storage
            .shard_state_storage()
            .load_mc_block_id(self.mc_seqno)
            .with_context(|| format!("mc block not found. seqno {}", self.mc_seqno))?
        else {
            anyhow::bail!("mc block not found. seqno {}", self.mc_seqno)
        };

        let ref_by_mc_seqno = storage
            .block_handle_storage()
            .load_handle(&mc_block_id)
            .map(|block_handle| block_handle.ref_by_mc_seqno());

        let state = storage
            .shard_state_storage()
            .load_state(ref_by_mc_seqno.unwrap_or(mc_block_id.seqno), &mc_block_id)
            .await?;

        let handler = ShardStateHandler {
            storage: storage.clone(),
            output_path: Dir::new(self.output.unwrap_or(PathBuf::from("./states")))?,
        };

        let mut zerostate_mapping = HashMap::new();

        for i in state.shards()?.iter() {
            let (ident, shard_description) = i?;
            let block_id = shard_description.as_block_id(ident);

            let state = storage
                .shard_state_storage()
                .load_state(0, &block_id)
                .await?;

            let zerostate_id = handler
                .save_shard_state(&mc_block_id, &block_id, state)
                .await?;
            zerostate_mapping.insert(block_id.file_hash, zerostate_id);
        }

        handler
            .save_master_state(&mc_block_id, &state, &zerostate_mapping)
            .await?;

        Ok(())
    }
}

struct ShardStateHandler {
    storage: CoreStorage,
    output_path: Dir,
}

impl ShardStateHandler {
    async fn save_shard_state(
        &self,
        mc_block_id: &BlockId,
        block_id: &BlockId,
        shard_state_stuff: ShardStateStuff,
    ) -> anyhow::Result<ZerostateId> {
        let state_cell = shard_state_stuff.root_cell();
        let mut slice = CellSlice::new(state_cell.as_ref())?;
        let mut ssu = ShardStateUnsplit::load_from(&mut slice)?;
        ssu.min_ref_mc_seqno = u32::MAX;
        ssu.processed_upto = ShardStateUnsplit::empty_processed_upto_info().clone();

        let mut builder = CellBuilder::new();
        ssu.store_into(&mut builder, Cell::empty_context())?;
        let updated_shard_state = builder.build()?;

        let handle = BlockHandle::new(
            &BlockId {
                shard: block_id.shard,
                seqno: u32::MAX,
                root_hash: block_id.root_hash,
                file_hash: block_id.file_hash,
            },
            BlockMeta::with_data(NewBlockMeta {
                is_key_block: false,
                gen_utime: ssu.gen_utime,
                ref_by_mc_seqno: mc_block_id.seqno,
            }),
            Default::default(),
        );

        self.storage
            .block_handle_storage()
            .store_handle(&handle, true);

        self.storage
            .shard_state_storage()
            .store_state_root(&handle, updated_shard_state.clone(), Default::default())
            .await?;

        let root_hash = updated_shard_state.repr_hash();
        let writer = ShardStateWriter::new(
            self.storage.shard_state_storage().cell_storage().db(),
            &self.output_path,
            block_id,
        );

        let shard_id = block_id.shard.to_string();
        let mut progress_bar = ProgressBar::builder()
            .exact_unit("bytes")
            .build(move |msg| tracing::info!("Saving shard {} state to file... {msg}", shard_id));

        let file_hash = writer.write_tracked(
            root_hash,
            &block_id.shard.to_string(),
            &mut progress_bar,
            None,
        )?;
        progress_bar.complete();

        println!(
            "Saved shard state for {}. root hash: {},  file hash: {},",
            block_id.shard, root_hash, file_hash
        );

        Ok(ZerostateId {
            seqno: block_id.seqno,
            root_hash: *root_hash,
            file_hash,
        })
    }
    async fn save_master_state(
        &self,
        mc_block_id: &BlockId,
        master_state: &ShardStateStuff,
        shardstate_mapping: &HashMap<HashBytes, ZerostateId>,
    ) -> anyhow::Result<()> {
        let state_cell = master_state.root_cell();
        let mut slice = CellSlice::new(state_cell.as_ref())?;
        let mut ssu = ShardStateUnsplit::load_from(&mut slice)?;
        let Some(custom) = ssu
            .load_custom()
            .context("no custom found in mc shard state unsplit")?
        else {
            anyhow::bail!("no custom found in mc shard state unsplit");
        };

        let mut shard_hashes = Vec::new();
        for i in custom.shards.iter() {
            let (ident, mut shard_description) = i?;
            let Some(id) = shardstate_mapping.get(&shard_description.file_hash) else {
                anyhow::bail!(
                    "state with file_hash: {} description not found",
                    shard_description.file_hash
                );
            };

            shard_description.reg_mc_seqno = id.seqno;
            shard_description.file_hash = id.file_hash;
            shard_description.root_hash = id.root_hash;
            shard_description.nx_cc_updated = true;
            shard_description.next_catchain_seqno = 0;
            shard_description.ext_processed_to_anchor_id = 0;
            shard_description.min_ref_mc_seqno = u32::MAX;
            shard_description.gen_utime = master_state.state().gen_utime;

            shard_hashes.push((ident, shard_description));
        }

        let curr_vset = custom.config.params.get_current_validator_set()?;
        let collation_config = custom.config.params.get_collation_config()?;
        let session_seqno = 0;
        let Some((_, validator_list_hash_short)) =
            curr_vset.compute_mc_subset(session_seqno, collation_config.shuffle_mc_validators)
        else {
            anyhow::bail!(
                "Failed to compute a validator subset for zerostate (shard_id = {}, session_seqno = {})",
                ShardIdent::MASTERCHAIN,
                session_seqno,
            );
        };

        ssu.processed_upto = ShardStateUnsplit::empty_processed_upto_info().clone();
        ssu.custom = Some(Lazy::new(&McStateExtra {
            shards: ShardHashes::from_shards(
                shard_hashes.iter().map(|(ident, descr)| (ident, descr)),
            )?,
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

        let mut builder = CellBuilder::new();
        ssu.store_into(&mut builder, Cell::empty_context())?;
        let updated_master_state = builder.build()?;

        let root_hash = *updated_master_state.repr_hash();

        let handle = BlockHandle::new(
            &BlockId {
                shard: mc_block_id.shard,
                seqno: u32::MAX,
                root_hash: mc_block_id.root_hash,
                file_hash: mc_block_id.file_hash,
            },
            BlockMeta::with_data(NewBlockMeta {
                is_key_block: true,
                gen_utime: ssu.gen_utime,
                ref_by_mc_seqno: ssu.min_ref_mc_seqno,
            }),
            Default::default(),
        );

        self.storage
            .block_handle_storage()
            .store_handle(&handle, true);

        self.storage
            .shard_state_storage()
            .store_state_root(&handle, updated_master_state, Default::default())
            .await?;

        let writer = ShardStateWriter::new(
            self.storage.shard_state_storage().cell_storage().db(),
            &self.output_path,
            mc_block_id,
        );
        let shard_id = mc_block_id.shard.to_string();
        let mut progress_bar = ProgressBar::builder()
            .exact_unit("bytes")
            .build(move |msg| tracing::info!("Saving shard {} state to file... {msg}", shard_id));

        let file_hash = writer.write_tracked(
            &root_hash,
            &mc_block_id.shard.to_string(),
            &mut progress_bar,
            None,
        )?;

        println!(
            "Saved mc state for {}. root hash: {},  file hash: {},",
            mc_block_id.shard, root_hash, file_hash
        );

        Ok(())
    }
}
