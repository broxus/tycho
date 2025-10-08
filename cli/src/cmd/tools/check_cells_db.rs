use std::sync::Arc;

use anyhow::Result;
use bytesize::ByteSize;
use clap::Parser;
use tempfile::TempDir;
use tycho_core::storage::{CoreStorage, CoreStorageConfig};
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::prelude::*;
use weedb::rocksdb::IteratorMode;

/// Check that the cells database is consistent.
#[derive(Parser)]
pub struct Cmd {
    /// Path to the cells database directory
    #[arg(short, long)]
    cells_db_path: std::path::PathBuf,

    /// Optional path for the temporary directory
    #[arg(long)]
    temp_dir_path: Option<std::path::PathBuf>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        check_cells_db_rt(self.cells_db_path, self.temp_dir_path)
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

    // 1. Create a temporary copy of the database
    let temp_dir = if let Some(tp) = temp_dir_path {
        std::fs::create_dir_all(&tp)?;
        TempDir::new_in(tp)?
    } else {
        TempDir::new()?
    };
    let temp_db_root = temp_dir.path();

    if !original_db_path.exists() {
        return Err(anyhow::anyhow!(
            "Cells database not found at {}",
            original_db_path.display()
        ));
    }

    // Copy the database directory
    println!(
        "Copying cells database from {} to {}...",
        original_db_path.display(),
        temp_db_root.display()
    );
    copy_dir_recursive(&original_db_path, temp_db_root)?;

    // 2. Open the copied database and remove all states

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

    // Remove all states and their cells
    let cell_storage = shard_state_storage.cell_storage().clone();
    let mut bump = bumpalo::Bump::new();
    let mut total_states = 0;
    let mut total_cells = 0;

    // Collect all states to remove
    let states: Vec<_> = cells_db
        .shard_states
        .iterator(IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()?;
    for (key, value) in states {
        let root_hash = HashBytes::from_slice(&value);
        let cell = cell_storage.load_cell(&root_hash, 0)?;

        total_states += 1;

        traverse_cell((cell as Arc<DynCell>).as_ref());

        let (removed_cells, mut local_batch) = cell_storage.remove_cell(&bump, &root_hash)?;
        total_cells += removed_cells;

        local_batch.delete_cf(&cells_db.shard_states.cf(), key);
        cells_db
            .rocksdb()
            .write_opt(local_batch, cells_db.cells.write_config())?;
        println!("Removed cells {root_hash} done. Traversed: {removed_cells}",);

        bump.reset();
    }

    // Trigger compaction to remove tombstones
    cells_db.trigger_compaction().await;
    cells_db.trigger_compaction().await;

    // 3. Check if the cell storage is empty
    let cells_left = cells_db.cells.iterator(IteratorMode::Start).count();

    println!(
        "States removed: {}, Cells removed: {}, Cells left in database: {}",
        total_states, total_cells, cells_left
    );

    // Clean up temp directory
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

fn traverse_cell(cell: &DynCell) {
    for cell in cell.references() {
        traverse_cell(cell);
    }
}
