use std::collections::HashMap;
use std::path::PathBuf;

use bytesize::ByteSize;
use clap::Parser;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::global_config::ZerostateId;
use tycho_core::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta, ShardStateWriter};
use tycho_storage::fs::Dir;
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::cell::{CellSlice, HashBytes, Lazy};
use tycho_types::models::{BlockId, ShardHashes, ShardStateUnsplit};
use tycho_types::prelude::{Cell, CellBuilder, CellFamily, Load, Store};

#[derive(Parser)]
pub struct Cmd {
    /// Path to the node config. If not specified, will use db path
    #[clap(long)]
    config: Option<PathBuf>,

    /// Path to the node's database directory.
    #[clap(long)]
    db: Option<PathBuf>,

    /// Path to the directory where the dump files will be saved.
    #[clap(long)]
    output: Option<PathBuf>,

    /// The ID of the block for which to dump the state. Can be a masterchain or a shardchain block.
    #[clap(short, long, allow_hyphen_values(true))]
    mc_block_id: BlockId,
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

        let ref_by_mc_seqno = storage
            .block_handle_storage()
            .load_handle(&self.mc_block_id)
            .map(|block_handle| block_handle.ref_by_mc_seqno());

        let state = storage
            .shard_state_storage()
            .load_state(
                ref_by_mc_seqno.unwrap_or(self.mc_block_id.seqno),
                &self.mc_block_id,
            )
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
            let zerostate_id = handler.save_shard_state(&block_id, state).await?;
            zerostate_mapping.insert(block_id.file_hash, zerostate_id);
        }

        handler
            .save_master_state(&self.mc_block_id, &state, &zerostate_mapping)
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
        block_id: &BlockId,
        shard_state_stuff: ShardStateStuff,
    ) -> anyhow::Result<ZerostateId> {
        let root_hash = shard_state_stuff.root_cell().repr_hash();
        let writer = ShardStateWriter::new(self.storage.cells_db(), &self.output_path, block_id);
        let file_hash = writer.write(root_hash, None)?;

        Ok(ZerostateId {
            seqno: Some(block_id.seqno),
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
        let Some(mut custom) = ssu.load_custom()? else {
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
            shard_description.file_hash = id.file_hash;
            shard_description.root_hash = id.root_hash;

            shard_hashes.push((ident, shard_description));
        }

        custom.shards =
            ShardHashes::from_shards(shard_hashes.iter().map(|(ident, descr)| (ident, descr)))?;
        ssu.custom = Some(Lazy::new(&custom)?);

        let mut builder = CellBuilder::new();
        ssu.store_into(&mut builder, Cell::empty_context())?;
        let updated_master_state = builder.build()?;
        let clone = updated_master_state.clone();
        let root_hash = clone.repr_hash();

        let (handle, _) =
            self.storage
                .block_handle_storage()
                .create_or_load_handle(mc_block_id, NewBlockMeta {
                    is_key_block: true,
                    gen_utime: ssu.gen_utime,
                    ref_by_mc_seqno: ssu.min_ref_mc_seqno,
                });

        self.storage
            .shard_state_storage()
            .store_state_root(&handle, updated_master_state, Default::default())
            .await?;

        let writer = ShardStateWriter::new(self.storage.cells_db(), &self.output_path, mc_block_id);
        writer.write(root_hash, None)?;

        Ok(())
    }
}
