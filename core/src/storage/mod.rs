use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;
use tycho_storage::StorageContext;
use tycho_storage::kv::ApplyMigrations;
use tycho_types::models::ShardIdent;

pub use self::block::{
    ArchiveId, BlockGcStats, BlockStorage, BlockStorageConfig, MaybeExistingHandle, OpenStats,
    PackageEntryKey, PartialBlockId, StoreBlockResult,
};
pub use self::block_connection::{BlockConnection, BlockConnectionStorage};
pub use self::block_handle::{
    BlockFlags, BlockHandle, BlockHandleStorage, BlockMeta, HandleCreationStatus,
    KeyBlocksDirection, LoadedBlockMeta, NewBlockMeta, WeakBlockHandle,
};
pub use self::config::{
    ArchivesGcConfig, BlocksCacheConfig, BlocksGcConfig, BlocksGcType, CoreStorageConfig,
    StatesGcConfig,
};
pub use self::db::{CellsDb, CellsPartDb, CoreDb, CoreDbExt, CoreTables};
pub use self::node_state::{NodeStateStorage, NodeSyncState};
pub use self::persistent_state::{
    BriefBocHeader, PersistentStateInfo, PersistentStateKind, PersistentStateStorage,
    QueueDiffReader, QueueStateReader, QueueStateWriter, ShardStateReader, ShardStateWriter,
};
pub use self::shard_state::{
    ShardStateStorage, ShardStateStorageError, ShardStateStorageMetrics, ShardStateStoragePart,
    ShardStateStoragePartImpl, StoragePartsMap, StoreStateHint,
};

pub mod tables;

pub(crate) mod block;
mod block_connection;
mod block_handle;
mod config;
mod db;
mod node_state;
mod persistent_state;
mod shard_state;

mod util {
    pub use self::slot_subscriptions::*;
    pub use self::stored_value::*;

    mod slot_subscriptions;
    mod stored_value;
}

const CORE_DB_SUBDIR: &str = "core";
const CELLS_DB_SUBDIR: &str = "cells";

#[derive(Clone)]
#[repr(transparent)]
pub struct CoreStorage {
    inner: Arc<Inner>,
}

impl CoreStorage {
    pub async fn open(ctx: StorageContext, config: CoreStorageConfig) -> Result<Self> {
        let db: CoreDb = ctx.open_preconfigured(CORE_DB_SUBDIR)?;
        db.normalize_version()?;
        db.apply_migrations().await?;

        let cells_db: CellsDb = ctx.open_preconfigured(CELLS_DB_SUBDIR)?;
        cells_db.normalize_version()?;
        cells_db.apply_migrations().await?;

        let blocks_storage_config = BlockStorageConfig {
            blocks_cache: config.blocks_cache,
            blobs_root: ctx.root_dir().path().join("blobs"),
            blob_db_config: config.blob_db.clone(),
        };
        let block_handle_storage = Arc::new(BlockHandleStorage::new(db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(db.clone()));
        let block_storage = BlockStorage::new(
            db.clone(),
            blocks_storage_config,
            block_handle_storage.clone(),
            block_connection_storage.clone(),
        )
        .await?;
        let block_storage = Arc::new(block_storage);

        const SHARD_PARTITIONS_SPLIT_DEPTH: u8 = 3; // TODO: move to node config

        let mut shards = vec![];

        // TODO: make a helper, pass workchain id from outside
        struct SplitShardCx {
            shard: ShardIdent,
            remaining_split_depth: u8,
        }
        let mut split_queue = VecDeque::new();
        split_queue.push_back(SplitShardCx {
            shard: ShardIdent::new_full(0),
            remaining_split_depth: SHARD_PARTITIONS_SPLIT_DEPTH,
        });
        while let Some(shard_split_cx) = split_queue.pop_front() {
            if shard_split_cx.remaining_split_depth > 0
                && let Some((left, right)) = shard_split_cx.shard.split()
            {
                let remaining_split_depth = shard_split_cx.remaining_split_depth.saturating_sub(1);
                if remaining_split_depth > 0 {
                    split_queue.push_back(SplitShardCx {
                        shard: left,
                        remaining_split_depth,
                    });
                    split_queue.push_back(SplitShardCx {
                        shard: right,
                        remaining_split_depth,
                    });
                } else {
                    shards.push(left);
                    shards.push(right);
                }
            }
        }

        let mut storage_parts = StoragePartsMap::default();
        for shard in shards {
            let cells_part_db: CellsPartDb = ctx.open_preconfigured(format!(
                "cells-parts/cells-part-{0}",
                shard.to_string().replace(":", "_")
            ))?;
            cells_part_db.normalize_version()?;
            cells_part_db.apply_migrations().await?;
            storage_parts.insert(
                shard,
                Arc::new(ShardStateStoragePartImpl::new(
                    shard,
                    cells_part_db,
                    config.cells_cache_size,
                    config.drop_interval,
                )),
            );
        }

        let shard_state_storage = ShardStateStorage::new(
            cells_db.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            ctx.temp_files().clone(),
            config.cells_cache_size,
            config.drop_interval,
            SHARD_PARTITIONS_SPLIT_DEPTH,
            Arc::new(storage_parts),
        )?;
        let persistent_state_storage = PersistentStateStorage::new(
            cells_db.clone(),
            ctx.files_dir(),
            block_handle_storage.clone(),
            block_storage.clone(),
            shard_state_storage.clone(),
        )?;

        persistent_state_storage.preload().await?;

        let node_state_storage = NodeStateStorage::new(db.clone());

        Ok(Self {
            inner: Arc::new(Inner {
                ctx,
                db,
                cells_db,
                config,
                block_handle_storage,
                block_storage,
                shard_state_storage,
                persistent_state_storage,
                block_connection_storage,
                node_state_storage,
            }),
        })
    }

    pub fn context(&self) -> &StorageContext {
        &self.inner.ctx
    }

    pub fn db(&self) -> &CoreDb {
        &self.inner.db
    }

    pub fn cells_db(&self) -> &CellsDb {
        &self.inner.cells_db
    }

    pub fn config(&self) -> &CoreStorageConfig {
        &self.inner.config
    }

    pub fn persistent_state_storage(&self) -> &PersistentStateStorage {
        &self.inner.persistent_state_storage
    }

    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        &self.inner.block_handle_storage
    }

    pub fn block_storage(&self) -> &BlockStorage {
        &self.inner.block_storage
    }

    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.inner.block_connection_storage
    }

    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.inner.shard_state_storage
    }

    pub fn node_state(&self) -> &NodeStateStorage {
        &self.inner.node_state_storage
    }

    pub fn open_stats(&self) -> &OpenStats {
        self.inner.block_storage.open_stats()
    }
}

struct Inner {
    ctx: StorageContext,
    db: CoreDb,
    cells_db: CellsDb,
    config: CoreStorageConfig,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: Arc<ShardStateStorage>,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}
