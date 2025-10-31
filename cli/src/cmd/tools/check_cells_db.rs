use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bumpalo_herd::Herd;
use bytesize::ByteSize;
use clap::Parser;
use tempfile::TempDir;
use tycho_block_util::dict::split_aug_dict_raw;
use tycho_core::storage::{CoreStorage, CoreStorageConfig};
use tycho_storage::kv::StoredValue;
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::cell::Cell;
use tycho_types::models::{BlockId, ShardAccounts};
use tycho_types::prelude::*;
use tycho_util::FastHashSet;
use weedb::rocksdb::IteratorMode;

/// Check that the cells database is consistent.
#[derive(Parser)]
pub struct Cmd {
    /// Path to the database root directory
    #[arg(short, long)]
    db_root: std::path::PathBuf,

    /// Optional path for the temporary directory
    #[arg(long)]
    temp_dir_path: Option<std::path::PathBuf>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        check_cells_db_rt(self.db_root, self.temp_dir_path)
    }
}

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if ty.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(src_path, dst_path)?;
        }
    }
    Ok(())
}

fn check_cells_db_rt(
    db_path: std::path::PathBuf,
    temp_dir_path: Option<std::path::PathBuf>,
) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move { check_cells_db(db_path, temp_dir_path).await })
}

async fn check_cells_db(
    original_db_path: std::path::PathBuf,
    temp_dir_path: Option<std::path::PathBuf>,
) -> Result<()> {
    println!("Checking cells database consistency...");

    let temp_dir = if let Some(tp) = temp_dir_path {
        std::fs::create_dir_all(&tp)?;
        TempDir::with_prefix_in("check_cells_db-", tp)?
    } else {
        TempDir::with_prefix("check_cells_db-")?
    };
    let temp_db_root = temp_dir.path();

    if !original_db_path.exists() {
        return Err(anyhow::anyhow!(
            "Database root path not found at {}",
            original_db_path.display()
        ));
    }

    copy_db(original_db_path, temp_db_root)?;

    let cells_left = clean_states(temp_db_root).await?;

    drop(temp_dir);

    if cells_left == 0 {
        println!(
            "✓ Cells database consistency check passed: cell storage is empty after removing all states."
        );
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "✗ Cells database consistency check failed: {} cells left after removing all states.",
            cells_left
        ))
    }
}

fn copy_db(original_db_path: PathBuf, temp_db_root: &Path) -> Result<()> {
    let cells_src = original_db_path.join("cells");
    let cells_dst = temp_db_root.join("cells");
    if !cells_src.exists() {
        return Err(anyhow::anyhow!(
            "Cells database not found at {}",
            cells_src.display()
        ));
    }
    println!(
        "Copying cells database from {} to {}...",
        cells_src.display(),
        cells_dst.display()
    );
    copy_dir_recursive(&cells_src, &cells_dst)?;
    Ok(())
}

async fn clean_states(temp_db_root: &Path) -> Result<usize> {
    let ctx = StorageContext::new(StorageConfig {
        root_dir: temp_db_root.to_path_buf(),
        rocksdb_enable_metrics: false,
        rocksdb_lru_capacity: ByteSize::mib(256),
    })
    .await?;
    let storage = CoreStorage::open(ctx, CoreStorageConfig::default()).await?;
    let shard_state_storage = storage.shard_state_storage();
    let cells_db = storage.cells_db().clone();
    println!("Removing all states from the copied database...");
    let cell_storage = shard_state_storage.cell_storage().clone();
    let mut herd = Herd::new();
    let mut total_states = 0;
    let mut total_cells = 0;
    let states: Vec<_> = cells_db
        .shard_states
        .iterator(IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()?;
    for (key, value) in states {
        let root_hash = HashBytes::from_slice(&value);
        let cell = cell_storage.load_cell(&root_hash, 0)?;
        let block_id = BlockId::from_slice(&key);
        total_states += 1;

        println!("Processing block_id: {}", block_id);

        let (removed_cells, mut local_batch) = if block_id.is_masterchain() {
            cell_storage.remove_cell(herd.get().as_bump(), &root_hash)?
        } else {
            let cell_arc: Cell = Cell::from(cell.clone());
            let split_at = split_shard_accounts_for_check(&cell_arc, 5)?;
            cell_storage.remove_cell_mt(&herd, &root_hash, split_at)?
        };
        total_cells += removed_cells;

        local_batch.delete_cf(&cells_db.shard_states.cf(), key);
        cells_db
            .rocksdb()
            .write_opt(local_batch, cells_db.cells.write_config())?;
        println!(
            "Removed cells for {} done. Traversed: {}",
            block_id, removed_cells
        );

        herd.reset();
    }
    cells_db.trigger_compaction().await;
    cells_db.trigger_compaction().await;
    let cells_left = cells_db.cells.iterator(IteratorMode::Start).count();
    println!(
        "States removed: {}, Cells removed: {}, Cells left in database: {}",
        total_states, total_cells, cells_left
    );
    Ok(cells_left)
}

fn split_shard_accounts_for_check(
    root_cell: &(impl AsRef<DynCell> + ?Sized),
    split_depth: u8,
) -> Result<FastHashSet<HashBytes>> {
    // Cell#0 - processed_upto
    // Cell#1 - accounts
    let shard_accounts = root_cell
        .as_ref()
        .reference_cloned(1)
        .context("invalid shard state")?
        .parse::<ShardAccounts>()
        .context("failed to load shard accounts")?;

    Ok(split_aug_dict_raw(shard_accounts, split_depth)
        .context("failed to split shard accounts")?
        .into_keys()
        .collect::<FastHashSet<HashBytes>>())
}
