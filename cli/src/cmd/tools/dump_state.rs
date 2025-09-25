use std::collections::VecDeque;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::QueueStateHeader;
use tycho_core::node::ConfiguredStorage;
use tycho_core::storage::{
    BlockConnection, CoreStorage, CoreStorageConfig, QueueStateWriter, ShardStateWriter,
};
use tycho_storage::StorageContext;
use tycho_storage::fs::Dir;
use tycho_types::boc::Boc;
use tycho_types::models::{BlockId, PrevBlockRef};

use crate::node::NodeConfig;

/// Dumps node state for a specific block, intended for testing collation of the next block.
/// This tool interacts directly with the node's database, bypassing the need for a running node,
/// which is useful for analyzing failed nodes.
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
    output: PathBuf,

    /// The ID of the block for which to dump the state. Can be a masterchain or a shardchain block.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(self.run_impl())
    }

    async fn run_impl(self) -> Result<()> {
        let output_dir = Dir::new(&self.output)?;
        output_dir.create_if_not_exists()?;

        let storage = if let Some(config) = self.config {
            let node_config =
                NodeConfig::from_file(config).context("failed to load node config")?;

            let store =
                ConfiguredStorage::new(&node_config.storage, &node_config.core_storage).await?;
            store.core_storage
        } else if let Some(root_dir) = self.db {
            println!("Opening database at: {}", root_dir.display());
            let storage_config = tycho_storage::StorageConfig {
                root_dir,
                ..Default::default()
            };
            let core_storage_config = CoreStorageConfig::default();
            let context = StorageContext::new(storage_config)
                .await
                .context("Failed to create storage context")?;
            CoreStorage::open(context, core_storage_config)
                .await
                .context("Failed to open core storage")?
        } else {
            return Err(anyhow::anyhow!("Either config or db must be specified"));
        };

        let dumper = Dumper {
            storage,
            output_dir,
        };

        let target_block_id = self.block_id;

        let master_block_id = if target_block_id.is_masterchain() {
            target_block_id
        } else {
            println!("Input is a shard block. Finding corresponding master block...");
            let handle = dumper
                .storage
                .block_handle_storage()
                .load_handle(&target_block_id)
                .context(format!(
                    "Target shard block handle not found for {}",
                    target_block_id
                ))?;
            let mc_seqno = handle.ref_by_mc_seqno();
            dumper
                .find_master_block_by_seqno(mc_seqno)
                .await
                .context(format!("Master block for seqno {} not found", mc_seqno))?
        };

        println!("Master block found: {}", master_block_id);

        // Dump master block and its state
        println!("Dumping master block and state...");
        dumper.dump_block_and_state(&master_block_id).await?;

        let master_block = dumper.load_block_stuff(&master_block_id).await?;
        let shards = master_block.load_custom()?.shards.latest_blocks();

        println!("Dumping top shard blocks and states...");
        for shard_block in shards {
            let block_id = shard_block?;
            if let Err(e) = dumper.dump_block_and_state(&block_id).await {
                println!("Failed to dump shard block {}: {}", block_id, e);
            };
        }

        // Dump master block's queue state
        println!("Dumping master block queue state...");
        dumper.dump_queue_state(&master_block_id).await?;

        // If the original target was a shard block, dump its chain and final state
        if !target_block_id.is_masterchain() {
            println!("Dumping shard block chain...");
            let shard_chain = dumper.get_shard_chain(target_block_id).await?;
            for block_id in &shard_chain {
                dumper.dump_block_and_state(block_id).await?;
            }

            if let Some(last_shard_block_id) = shard_chain.last() {
                println!("Dumping final shard queue state...");
                dumper.dump_queue_state(last_shard_block_id).await?;
            }
        }

        println!("Dump completed successfully to: {}", self.output.display());

        Ok(())
    }
}

struct Dumper {
    storage: CoreStorage,
    output_dir: Dir,
}

impl Dumper {
    async fn dump_block_and_state(&self, block_id: &BlockId) -> Result<()> {
        println!("Dumping data for block {}", block_id);
        self.dump_block_data(block_id).await?;
        self.dump_persistent_state(block_id).await?;
        Ok(())
    }

    async fn find_master_block_by_seqno(&self, mc_seqno: u32) -> Option<BlockId> {
        self.storage
            .block_handle_storage()
            .key_blocks_iterator(tycho_core::storage::KeyBlocksDirection::ForwardFrom(
                mc_seqno,
            ))
            .find(|id| id.seqno == mc_seqno)
    }

    async fn dump_block_data(&self, block_id: &BlockId) -> Result<()> {
        let block = self.load_block_stuff(block_id).await?;
        let raw_data = Boc::encode(block.root_cell());
        let path = self
            .output_dir
            .path()
            .join(format!("{}.block", block_id.to_string().replace(':', "_")));
        std::fs::write(&path, raw_data)
            .context(format!("Failed to write block file for {}", block_id))?;
        println!(" - Block data saved to {}", path.display());
        Ok(())
    }

    async fn dump_persistent_state(&self, block_id: &BlockId) -> Result<()> {
        let dir = Dir::new(self.output_dir.path().join("persistents"))?;
        let writer = ShardStateWriter::new(self.storage.db(), &dir, block_id);
        let state = self
            .storage
            .shard_state_storage()
            .load_state(block_id)
            .await?;
        writer
            .write(state.root_cell().repr_hash(), None)
            .context(format!("Failed to write state for {}", block_id))?;
        println!(" - Persistent state saved");
        Ok(())
    }

    async fn dump_queue_state(&self, block_id: &BlockId) -> Result<()> {
        let mut top_block_handle = self
            .storage
            .block_handle_storage()
            .load_handle(block_id)
            .context(format!(
                "Handle not found for queue state dump of block {}",
                block_id
            ))?;
        let mut top_block = self.load_block_stuff(block_id).await?;
        let mut tail_len = top_block.block().out_msg_queue_updates.tail_len as usize;

        let mut messages = Vec::new();
        let mut queue_diffs = Vec::new();

        while tail_len > 0 {
            let queue_diff = self
                .storage
                .block_storage()
                .load_queue_diff(&top_block_handle)
                .await?;
            let block_extra = top_block.load_extra()?;
            let out_messages = block_extra.load_out_msg_description()?;

            messages.push(queue_diff.zip(&out_messages));
            queue_diffs.push(queue_diff.diff().clone());

            if tail_len == 1 {
                break;
            }

            let prev_block_id = match top_block.load_info()?.load_prev_ref()? {
                PrevBlockRef::Single(block_ref) => block_ref.as_block_id(block_id.shard),
                PrevBlockRef::AfterMerge { .. } => {
                    anyhow::bail!("Merge blocks not supported in dump tool")
                }
            };

            top_block_handle = self
                .storage
                .block_handle_storage()
                .load_handle(&prev_block_id)
                .context(format!(
                    "Previous block handle not found: {}",
                    prev_block_id
                ))?;
            top_block = self.load_block_stuff(&prev_block_id).await?;
            tail_len -= 1;
        }

        let state_header = QueueStateHeader {
            shard_ident: block_id.shard,
            seqno: block_id.seqno,
            queue_diffs,
        };

        let dir = Dir::new(self.output_dir.path().join("queues"))?;
        let writer = QueueStateWriter::new(&dir, block_id, state_header, messages);
        writer
            .write(None)
            .context(format!("Failed to write queue state for {}", block_id))?;
        println!(" - Queue state saved");
        Ok(())
    }

    async fn get_shard_chain(&self, target_block_id: BlockId) -> Result<Vec<BlockId>> {
        let mut chain = VecDeque::new();
        let mut current_block_id = target_block_id;

        loop {
            if current_block_id.seqno == 0 {
                break;
            }
            chain.push_front(current_block_id);

            let handle = self
                .storage
                .block_handle_storage()
                .load_handle(&current_block_id)
                .context(format!(
                    "Handle not found for shard chain block {}",
                    current_block_id
                ))?;

            if handle.is_persistent() {
                let state = self
                    .storage
                    .shard_state_storage()
                    .load_state(&current_block_id)
                    .await?;
                if state.state().min_ref_mc_seqno < handle.ref_by_mc_seqno() {
                    break;
                }
            }

            if let Some(prev_id) = self
                .storage
                .block_connection_storage()
                .load_connection(&current_block_id, BlockConnection::Prev1)
            {
                current_block_id = prev_id;
            } else {
                break;
            }
        }
        Ok(chain.into())
    }

    async fn load_block_stuff(&self, block_id: &BlockId) -> Result<BlockStuff> {
        let handle = self
            .storage
            .block_handle_storage()
            .load_handle(block_id)
            .context(format!("Failed to load handle for block {}", block_id))?;
        self.storage.block_storage().load_block_data(&handle).await
    }
}
