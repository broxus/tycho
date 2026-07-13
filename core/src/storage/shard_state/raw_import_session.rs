use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::storage::CellsDb;
use crate::storage::shard_state::db_state;

/// Lazily owns one top-level raw-import marker.
#[derive(Clone)]
pub struct RawImportSession {
    cells_db: CellsDb,
    state: Arc<Mutex<RawImportSessionState>>,
}

#[derive(Clone, Copy)]
enum RawImportSessionState {
    Pending,
    Started,
    Finished,
}

impl RawImportSession {
    pub(crate) fn new(cells_db: CellsDb) -> Self {
        Self {
            cells_db,
            state: Arc::new(Mutex::new(RawImportSessionState::Pending)),
        }
    }

    /// Creates the marker once while the session is not finished.
    pub(crate) fn begin(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("raw import session lock poisoned: {e}"))?;
        match *state {
            RawImportSessionState::Pending => {
                Self::begin_raw_import(&self.cells_db)?;
                *state = RawImportSessionState::Started;
                Ok(())
            }
            RawImportSessionState::Started => Ok(()),
            RawImportSessionState::Finished => {
                anyhow::bail!("raw import session is already finished")
            }
        }
    }

    fn begin_raw_import(cells_db: &CellsDb) -> Result<()> {
        // Any restart before this marker is cleared must require a full re-sync.
        anyhow::ensure!(
            cells_db
                .state
                .get(db_state::CellsDbStateKey::RawImportInProgress)?
                .is_none(),
            "raw state import is already in progress; \
            local storage is poisoned and requires a full re-sync",
        );
        cells_db
            .state
            .insert(db_state::CellsDbStateKey::RawImportInProgress, [1u8])?;
        Ok(())
    }

    /// Completes the session and clears its marker when it was started.
    pub fn finish(self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("raw import session lock poisoned: {e}"))?;
        match *state {
            RawImportSessionState::Pending => {
                *state = RawImportSessionState::Finished;
                Ok(())
            }
            RawImportSessionState::Started => {
                Self::finish_raw_import(&self.cells_db)?;
                *state = RawImportSessionState::Finished;
                Ok(())
            }
            RawImportSessionState::Finished => {
                anyhow::bail!("raw import session is already finished")
            }
        }
    }

    fn finish_raw_import(cells_db: &CellsDb) -> Result<()> {
        // Keep the marker until metadata and persistent states are done.
        cells_db
            .state
            .remove(db_state::CellsDbStateKey::RawImportInProgress)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tycho_storage::{StorageConfig, StorageContext};

    use super::RawImportSession;
    use crate::storage::{CoreStorage, CoreStorageConfig};

    #[tokio::test]
    async fn storage_open_rejects_existing_raw_import_marker() -> Result<()> {
        // setup
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        // marker creation
        let cells_db = storage.shard_state_storage().cell_storage().db().clone();
        RawImportSession::begin_raw_import(&cells_db)?;
        let err = RawImportSession::begin_raw_import(&cells_db)
            .expect_err("second raw import must be rejected");
        assert!(err.to_string().contains("already in progress"));
        drop(cells_db);
        drop(storage);

        // restart/open attempt
        let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
        let err = match CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await {
            Ok(_) => panic!("storage open must reject an existing raw import marker"),
            Err(err) => err,
        };

        // final assertions
        assert!(err.to_string().contains("re-sync"));
        Ok(())
    }

    #[tokio::test]
    async fn finished_raw_import_marker_allows_storage_open() -> Result<()> {
        // setup
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        // raw import
        let raw_import = storage.shard_state_storage().create_raw_import_session();
        raw_import.begin()?;
        raw_import.finish()?;
        drop(storage);

        // restart/open attempt
        let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        // final assertions
        assert!(storage.node_state().load_init_mc_block_id().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn raw_import_session_is_lazy_idempotent() -> Result<()> {
        // setup
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;
        let shard_states = storage.shard_state_storage();

        // pending session completion
        let raw_import = shard_states.create_raw_import_session();
        let retained_activate = raw_import.clone();
        let retained_finish = raw_import.clone();
        raw_import.finish()?;
        let err = retained_activate
            .begin()
            .expect_err("finished pending session must not activate");
        assert!(err.to_string().contains("already finished"));
        drop(retained_activate);
        let err = retained_finish
            .finish()
            .expect_err("finished pending session must not finish again");
        assert!(err.to_string().contains("already finished"));

        // started session completion
        let raw_import = shard_states.create_raw_import_session();
        let retained_activate = raw_import.clone();
        let retained_finish = raw_import.clone();
        raw_import.begin()?;
        raw_import.begin()?;
        raw_import.finish()?;
        let err = retained_activate
            .begin()
            .expect_err("finished started session must not reactivate");
        assert!(err.to_string().contains("already finished"));
        drop(retained_activate);
        let err = retained_finish
            .finish()
            .expect_err("finished started session must not finish again");
        assert!(err.to_string().contains("already finished"));
        drop(storage);

        // restart/open attempt
        let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
        let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

        // final assertions
        assert!(storage.node_state().load_init_mc_block_id().is_none());
        Ok(())
    }
}
