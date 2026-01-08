use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::QueueStateHeader;
use tycho_block_util::state::ShardStateStuff;
use tycho_collator::mempool::{DumpAnchors, DumpedAnchor};
use tycho_collator::types::processed_upto::ProcessedUptoInfoExtension;
use tycho_collator::types::{McData, ShardDescriptionShortExt};
use tycho_core::node::ConfiguredStorage;
use tycho_core::storage::{
    BlockConnection, CoreStorage, CoreStorageConfig, QueueStateWriter, ShardStateWriter,
};
use tycho_storage::StorageContext;
use tycho_storage::fs::Dir;
use tycho_types::boc::Boc;
use tycho_types::models::{BlockId, PrevBlockRef, ShardIdent};

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

        let storage = self.get_core_storage().await?;

        let dumper = Dumper {
            storage,
            output_dir,
        };

        let target_block_id = self.block_id;

        let master_block_id = if target_block_id.is_masterchain() {
            target_block_id
        } else {
            println!("Input is a shard block. Finding corresponding master block...");

            let shard_block = dumper.load_block_stuff(&target_block_id).await?;
            let info = shard_block.load_info()?;
            info.load_master_ref()
                .context("Shard block has no master ref")?
                .ok_or_else(|| {
                    anyhow::anyhow!("Shard block {} has no master ref", target_block_id)
                })?
                .as_block_id(ShardIdent::MASTERCHAIN)
        };

        println!("Master block found: {}", master_block_id);

        println!("Dumping master block and state...");

        let mc_state_stuff = dumper
            .dump_block_and_state(&master_block_id, master_block_id.seqno)
            .await?;
        let mc_data = McData::load_from_state(&mc_state_stuff, Default::default())?;

        println!("Dumping top shard blocks, states and queues...");

        for block_id in mc_data
            .shards
            .iter()
            .map(|(shard_ident, descr)| descr.get_block_id(*shard_ident))
        {
            if let Err(e) = dumper
                .dump_block_and_state(&block_id, master_block_id.seqno)
                .await
            {
                println!("Failed to dump shard block {}: {}", block_id, e);
            };
            if let Err(e) = dumper.dump_queue_state(&block_id).await {
                println!(
                    "Failed to dump queue state for top shard block {}: {}",
                    block_id, e
                );
            };
        }

        println!("Dumping master block queue state...");

        dumper.dump_queue_state(&master_block_id).await?;

        // If the original target was a shard block, dump its chain and final state
        if !target_block_id.is_masterchain() {
            println!("Dumping shard block chain...");

            let shard_chain = dumper.get_shard_chain(target_block_id, &mc_data).await?;
            for block_id in &shard_chain {
                dumper
                    .dump_block_and_state(block_id, master_block_id.seqno)
                    .await?;
            }

            if let Some(last_shard_block_id) = shard_chain.last() {
                println!("Dumping final shard queue state...");

                dumper.dump_queue_state(last_shard_block_id).await?;
            }
        }

        println!("Dumping mempool state...");

        dumper.dump_mempool_state(mc_state_stuff, mc_data).await?;

        println!("Dump completed successfully to: {}", self.output.display());

        Ok(())
    }

    async fn get_core_storage(&self) -> Result<CoreStorage> {
        if let Some(ref config) = self.config {
            let node_config =
                NodeConfig::from_file(config).context("failed to load node config")?;

            let store =
                ConfiguredStorage::new(&node_config.storage, &node_config.core_storage).await?;
            Ok(store.core_storage)
        } else if let Some(ref root_dir) = self.db {
            println!("Opening database at: {}", root_dir.display());
            let storage_config = tycho_storage::StorageConfig {
                root_dir: root_dir.clone(),
                ..Default::default()
            };
            let core_storage_config = CoreStorageConfig::default();
            let context = StorageContext::new(storage_config)
                .await
                .context("Failed to create storage context")?;
            CoreStorage::open(context, core_storage_config)
                .await
                .context("Failed to open core storage")
        } else {
            Err(anyhow::anyhow!("Either config or db must be specified"))
        }
    }
}

struct Dumper {
    storage: CoreStorage,
    output_dir: Dir,
}

impl Dumper {
    async fn dump_block_and_state(
        &self,
        block_id: &BlockId,
        master_block_seqno: u32,
    ) -> Result<ShardStateStuff> {
        println!("Dumping data for block {}", block_id);
        if let Err(e) = self.dump_block_data(block_id).await {
            if block_id.seqno == 0 {
                println!(
                    "Skipping block data dump for zerostate block {}: {}",
                    block_id, e
                );
            } else {
                return Err(e);
            }
        }
        self.dump_persistent_state(block_id, master_block_seqno)
            .await
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

    async fn dump_persistent_state(
        &self,
        block_id: &BlockId,
        master_block_seqno: u32,
    ) -> Result<ShardStateStuff> {
        let dir = Dir::new(self.output_dir.path().join("persistents"))?;
        let writer = ShardStateWriter::new(
            self.storage.shard_state_storage().cell_storage().db(),
            &dir,
            block_id,
        );
        let ref_by_mc_seqno = self
            .storage
            .block_handle_storage()
            .load_handle(block_id)
            .map(|block_handle| block_handle.ref_by_mc_seqno());

        let state = self
            .storage
            .shard_state_storage()
            .load_state(ref_by_mc_seqno.unwrap_or(master_block_seqno), block_id)
            .await?;

        writer
            .write(state.root_cell().repr_hash(), None)
            .context(format!("Failed to write state for {}", block_id))?;
        println!(" - Persistent state saved");
        Ok(state)
    }

    async fn dump_queue_state(&self, block_id: &BlockId) -> Result<()> {
        println!("Dumping queue state for block {}", block_id);
        if block_id.seqno == 0 {
            // For zerostate blocks, there is no queue state to dump
            return Ok(());
        }

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

    async fn get_shard_chain(
        &self,
        target_block_id: BlockId,
        mc_data: &McData,
    ) -> Result<Vec<BlockId>> {
        let top_seqno = mc_data
            .shards
            .iter()
            .find(|(shard, _)| shard == &target_block_id.shard)
            .map_or(0, |(_, descr)| descr.seqno);

        let mut chain = VecDeque::new();
        let mut current_block_id = target_block_id;

        loop {
            if current_block_id.seqno <= top_seqno {
                break;
            }
            chain.push_front(current_block_id);

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

    async fn dump_mempool_state(
        &self,
        mc_state_stuff: ShardStateStuff,
        mc_data: Arc<McData>,
    ) -> Result<()> {
        let (mut top_processed_to_anchor_mc, _) =
            mc_data.processed_upto.get_min_externals_processed_to()?;

        top_processed_to_anchor_mc =
            std::cmp::min(mc_data.top_processed_to_anchor, top_processed_to_anchor_mc);

        let mc_state_extra = mc_state_stuff.state_extra()?;

        let dump_anchors =
            DumpAnchors::new(self.storage.context()).context("Failed to create DumpAnchors")?;

        let mempool_node_config = tycho_consensus::prelude::MempoolNodeConfig::default();

        let consensus_config = mc_state_extra.config.get_consensus_config()?;

        let dumped_anchors = dump_anchors
            .load(
                top_processed_to_anchor_mc,
                &mempool_node_config,
                &consensus_config,
                mc_state_extra.consensus_info.genesis_info,
            )
            .context("Failed to load dumped anchors")?;

        let dst_dir = self.output_dir.path().join("mempool");
        tokio::fs::create_dir_all(&dst_dir).await?;

        for dumped_anchor in dumped_anchors {
            let filename = format!("anchor_{}.json", dumped_anchor.id);
            let filepath = dst_dir.join(filename);

            let mut externals = Vec::new();
            for external in &dumped_anchor.externals {
                externals.push(Boc::encode_base64(&external.cell));
            }

            let content = serde_json::to_string(&DumpedAnchor {
                id: dumped_anchor.id,
                prev_id: dumped_anchor.prev_id,
                author: dumped_anchor.author,
                chain_time: dumped_anchor.chain_time,
                externals,
            })?;

            tokio::fs::write(&filepath, &content.as_bytes()).await?;
        }

        println!(" - Mempool state saved");

        Ok(())
    }
}
