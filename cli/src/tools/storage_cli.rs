use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_types::models::BlockId;
use serde::Deserialize;
use tycho_storage::{BlockConnection, KeyBlocksDirection, Storage, StorageConfig};

fn init_storage(path: Option<&PathBuf>) -> Result<Storage> {
    let default = PathBuf::from("./db");
    let config_path = path.unwrap_or(&default);
    let config: StorageConfig = tycho_util::serde_helpers::load_json_from_file(config_path)?;
    Storage::builder()
        .with_config(config)
        //.with_rpc_storage(node_config.rpc.is_some())
        .build()
}

#[derive(Subcommand)]
pub enum StorageCmd {
    GetNextKeyblockIds(GetNextKeyBlockIdsCmd),
    GetBlockFull(BlockCmd),
    GetNextBlockFull(BlockCmd),
    GetArchiveInfo(GetArchiveInfoCmd),
    GetArchiveSlice(GetArchiveSliceCmd),
    GetPersistentStateInfo(BlockCmd),
    GetPersistentStatePart,
}
impl StorageCmd {
    pub(crate) fn run(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            match self {
                Self::GetNextKeyblockIds(cmd) => cmd.run(),
                Self::GetBlockFull(cmd) => cmd.run().await,
                Self::GetNextBlockFull(cmd) => cmd.run_next().await,
                Self::GetArchiveInfo(cmd) => cmd.run(),
                Self::GetArchiveSlice(cmd) => cmd.run(),
                Self::GetPersistentStateInfo(cmd) => cmd.get_state_info(),
                Self::GetPersistentStatePart => Ok(()),
            }
        })
    }
}

#[derive(Deserialize, Parser)]
pub struct GetNextKeyBlockIdsCmd {
    pub block_id: BlockId,
    pub limit: usize,
    pub storage_path: Option<PathBuf>,
}

impl GetNextKeyBlockIdsCmd {
    pub fn run(&self) -> Result<()> {
        let storage = init_storage(self.storage_path.as_ref())?;

        let block_handle_storage = storage.block_handle_storage();

        let get_next_key_block_ids = || {
            if !self.block_id.shard.is_masterchain() {
                anyhow::bail!("first block id is not from masterchain");
            }

            let mut iterator = block_handle_storage
                .key_blocks_iterator(KeyBlocksDirection::ForwardFrom(self.block_id.seqno))
                .take(self.limit);

            if let Some(id) = iterator.next() {
                anyhow::ensure!(
                    id.root_hash == self.block_id.root_hash,
                    "first block root hash mismatch"
                );
                anyhow::ensure!(
                    id.file_hash == self.block_id.file_hash,
                    "first block file hash mismatch"
                );
            }

            Ok::<_, anyhow::Error>(iterator.take(self.limit).collect::<Vec<_>>())
        };

        match get_next_key_block_ids() {
            Ok(ids) => {
                if ids.len() < self.limit {
                    println!(
                        "Found {} blocks which is less then specified limit of {}",
                        ids.len(),
                        self.limit
                    );
                }
                for i in ids.iter() {
                    println!("Found block {i}");
                }
                Ok(())
            }
            Err(e) => {
                println!("Operation failed: {e:?}");
                Ok(())
            }
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct BlockCmd {
    pub block_id: BlockId,
    pub storage_path: Option<PathBuf>,
}

impl BlockCmd {
    pub async fn run(&self) -> Result<()> {
        let storage = init_storage(self.storage_path.as_ref())?;
        let block_handle_storage = storage.block_handle_storage();
        let block_storage = storage.block_storage();

        let get_block_full = async {
            let mut is_link = false;
            match block_handle_storage.load_handle(&self.block_id) {
                Some(handle)
                    if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) =>
                {
                    let block = block_storage.load_block_data_raw(&handle).await?;
                    let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                    println!("Found block full {}\n", &self.block_id);
                    println!("Block is link: {}\n", is_link);
                    println!("Block hex {}\n", hex::encode(block));
                    println!("Block proof {}\n", hex::encode(proof));
                }
                _ => {
                    println!("Found block empty {}\n", &self.block_id);
                }
            };
            Ok::<(), anyhow::Error>(())
        };

        match get_block_full.await {
            Ok(()) => Ok(()),
            Err(e) => {
                println!("Get block full failed: {e:?}");
                Ok(())
            }
        }
    }

    pub async fn run_next(&self) -> Result<()> {
        let storage = init_storage(self.storage_path.as_ref())?;
        let block_handle_storage = storage.block_handle_storage();
        let block_connection_storage = storage.block_connection_storage();
        let block_storage = storage.block_storage();
        let get_next_block_full = async {
            let next_block_id = match block_handle_storage.load_handle(&self.block_id) {
                Some(handle) if handle.meta().has_next1() => block_connection_storage
                    .load_connection(&self.block_id, BlockConnection::Next1)
                    .context("connection not found")?,
                _ => return Ok(()),
            };

            let mut is_link = false;
            match block_handle_storage.load_handle(&next_block_id) {
                Some(handle)
                    if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) =>
                {
                    let block = block_storage.load_block_data_raw(&handle).await?;
                    let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                    println!("Found block full {}\n", &self.block_id);
                    println!("Block is link: {}\n", is_link);
                    println!("Block hex {}\n", hex::encode(block));
                    println!("Block proof {}\n", hex::encode(proof));
                }
                _ => {
                    println!("Found block empty {}\n", &self.block_id);
                }
            };
            Ok::<(), anyhow::Error>(())
        };

        match get_next_block_full.await {
            Ok(_) => Ok(()),
            Err(e) => {
                println!("Get next block full failed: {e:?}");
                Ok(())
            }
        }
    }

    fn get_state_info(&self) -> Result<()> {
        let storage = init_storage(self.storage_path.as_ref())?;
        let persistent_state_storage = storage.persistent_state_storage();

        if let Some(info) = persistent_state_storage.get_state_info(&self.block_id) {
            println!("Find persistent state of size: {}", info.size);
        } else {
            println!("Persistent state not found");
        }
        Ok(())
    }
}

#[derive(Deserialize, Parser)]
pub struct GetArchiveInfoCmd {
    pub mc_seqno: u32,
    pub storage_path: Option<PathBuf>,
}

impl GetArchiveInfoCmd {
    pub fn run(&self) -> Result<()> {
        let storage = init_storage(self.storage_path.as_ref())?;
        let node_state = storage.node_state();

        match node_state.load_last_mc_block_id() {
            Some(last_applied_mc_block) => {
                if self.mc_seqno > last_applied_mc_block.seqno {
                    println!("Archive not found. Requested seqno is gt last applied seqno");
                    return Ok(());
                }

                let block_storage = storage.block_storage();

                match block_storage.get_archive_id(self.mc_seqno) {
                    Some(id) => println!("Archive found with id: {}", id),
                    None => println!("Archive not found"),
                }
                Ok(())
            }
            None => {
                println!("Get archive id failed: no blocks applied");
                Ok(())
            }
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct GetArchiveSliceCmd {
    pub archive_id: u64,
    pub limit: u32,
    pub offset: u64,
    pub storage_path: Option<PathBuf>,
}

impl GetArchiveSliceCmd {
    pub fn run(&self) -> Result<()> {
        let storage = init_storage(self.storage_path.as_ref())?;
        let block_storage = storage.block_storage();

        let get_archive_slice = || {
            let Some(archive_slice) = block_storage.get_archive_slice(
                self.archive_id as u32,
                self.offset as usize,
                self.limit as usize,
            )?
            else {
                anyhow::bail!("Archive not found");
            };

            Ok(archive_slice)
        };

        match get_archive_slice() {
            Ok(data) => {
                println!("Archive slice: {}", hex::encode(data));
                Ok(())
            }
            Err(e) => {
                println!("Get archive slice failed: {e:?}");
                Ok(())
            }
        }
    }
}

#[derive(Deserialize)]
pub struct GetPersistentStateInfo {}
