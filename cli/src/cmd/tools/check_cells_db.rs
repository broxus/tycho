use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bumpalo_herd::Herd;
use bytesize::ByteSize;
use clap::Parser;
use tempfile::TempDir;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tycho_core::storage::{
    CellStorageDb, CellsDbOps, CoreStorage, CoreStorageConfig, split_shard_accounts,
};
use tycho_storage::kv::StoredValue;
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::cell::Cell;
use tycho_types::models::BlockId;
use tycho_types::prelude::*;
use tycho_util::cli::signal;
use weedb::rocksdb::IteratorMode;

/// Check that the cells database is consistent.
#[derive(Parser)]
pub struct Cmd {
    /// Path to the database root directory
    #[arg()]
    db_root: PathBuf,

    /// Optional root for the temporary directory
    #[arg(long)]
    temp_dir: Option<PathBuf>,

    /// Base workchain accounts split depth.
    #[arg(long, default_value_t = 4)]
    accounts_split_depth: u8,
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
            anyhow::ensure!(
                self.db_root.exists(),
                "database root path not found: {}",
                self.db_root.display()
            );

            let temp_dir = copy_db_partial(&self.db_root, self.temp_dir.as_deref(), &[
                tycho_core::storage::CELLS_DB_SUBDIR,
            ])?;

            tracing::info!("checking cells database consistency");

            let ctx = StorageContext::new(StorageConfig {
                root_dir: temp_dir.path().to_path_buf(),
                rocksdb_enable_metrics: false,
                rocksdb_lru_capacity: ByteSize::mib(256),
            })
            .await?;

            let mut core_storage_config = CoreStorageConfig::default().without_gc();
            core_storage_config.blob_db.pre_create_cas_tree = false;
            let storage = CoreStorage::open(ctx, core_storage_config).await?;

            let shard_states = storage.shard_state_storage();
            let cell_storage = shard_states.cell_storage();
            let cells_db = cell_storage.db();

            tracing::info!("removing all states from the copied database");

            let mut herd = Herd::new();
            let mut total_states = 0;
            let mut total_cells = 0;
            for entry in cells_db.shard_states().iterator(IteratorMode::Start) {
                let (key, value) = entry?;

                let block_id = BlockId::from_slice(&key);
                let root_hash = HashBytes::from_slice(&value[..32]);
                let cell = cell_storage.load_cell(&root_hash, 0)?;
                total_states += 1;

                tracing::info!(%block_id, total_states, "removing state");

                let (removed_cells, local_batch) = if block_id.is_masterchain() {
                    drop(cell);
                    cell_storage.remove_cell(herd.get().as_bump(), &root_hash)?
                } else {
                    // TODO: should handle parts
                    let split_at = split_shard_accounts(
                        &block_id.shard,
                        Cell::from(cell),
                        self.accounts_split_depth,
                    )?
                    .into_keys()
                    .collect();

                    cell_storage.remove_cell_mt(&herd, &root_hash, split_at, false)?
                };
                total_cells += removed_cells;

                cells_db
                    .rocksdb()
                    .write_opt(local_batch, cells_db.cells().write_config())?;

                tracing::info!(%block_id, total_states, removed_cells, "removed state");
                herd.reset();
            }

            match cells_db {
                CellStorageDb::Main(db) => {
                    db.trigger_compaction().await;
                    db.trigger_compaction().await;
                }
                CellStorageDb::Part(db) => {
                    db.trigger_compaction().await;
                    db.trigger_compaction().await;
                }
            }

            let cells_left = cells_db.cells().iterator(IteratorMode::Start).count();
            tracing::info!(total_states, total_cells, cells_left, "done");

            anyhow::ensure!(
                cells_left == 0,
                "not all cells were removed, cells_left={cells_left}"
            );
            Ok(())
        };

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(signal::run_or_terminate(fut))
    }
}

fn copy_db_partial(src: &Path, tmp_root: Option<&Path>, subdirs: &[&str]) -> Result<TempDir> {
    const PREFIX: &str = "check_cells_db-";

    let temp_dir = if let Some(tmp_root) = tmp_root {
        std::fs::create_dir_all(tmp_root).context("failed to create tmp root")?;
        TempDir::with_prefix_in(PREFIX, tmp_root)
            .context("failed to create temp dir in tmp root")?
    } else {
        TempDir::with_prefix(PREFIX).context("failed to create temp dir")?
    };

    let dst = temp_dir.path();
    for subdir in subdirs {
        let src = src.join(subdir);
        anyhow::ensure!(src.exists(), "{} not found", src.display());
        copy_dir(&src, &dst.join(subdir))?;
    }

    Ok(temp_dir)
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if ty.is_dir() {
            copy_dir(&src_path, &dst_path)?;
        } else {
            std::fs::copy(src_path, dst_path)?;
        }
    }
    Ok(())
}
