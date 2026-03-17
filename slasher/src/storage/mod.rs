use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_slasher_traits::ValidationSessionId;
use tycho_storage::StorageContext;
use weedb::OwnedSnapshot;

use self::db::{SlasherDb, tables};
use self::models::{StoredBlocksBatch, StoredSessionPenaltyReport};
use crate::{BlocksBatch, SessionPenaltyReport};

pub mod db;
pub mod models;

const SLASHER_DB_SUBDIR: &str = "slasher";

#[derive(Clone)]
#[repr(transparent)]
pub struct SlasherStorage {
    inner: Arc<Inner>,
}

impl SlasherStorage {
    pub fn open(ctx: &StorageContext) -> Result<Self> {
        let db = ctx.open_preconfigured(SLASHER_DB_SUBDIR)?;

        Ok(Self {
            inner: Arc::new(Inner { db }),
        })
    }

    /// Creates a new snapshot.
    pub fn snapshot(&self) -> SlasherStorageSnapshot {
        SlasherStorageSnapshot {
            db: self.inner.db.clone(),
            snapshot: Arc::new(self.inner.db.owned_snapshot()),
        }
    }

    pub fn store_blocks_batch(
        &self,
        session_id: ValidationSessionId,
        validator_idx: u16,
        batch: &BlocksBatch,
    ) -> Result<Option<SessionPenaltyReport>> {
        let key = block_batches_key(session_id, validator_idx, batch.start_seqno);

        let value = tl_proto::serialize(StoredBlocksBatch::wrap(batch));

        self.inner.db.block_batches.insert(key.as_slice(), value)?;
        self.take_session_report(session_id)
    }

    pub fn store_session_report(&self, report: &SessionPenaltyReport) -> Result<()> {
        let key = session_key(report.session_id);
        let value = tl_proto::serialize(StoredSessionPenaltyReport::wrap(report));
        self.inner
            .db
            .session_reports
            .insert(key.as_slice(), value)?;
        Ok(())
    }

    pub fn load_session_report(
        &self,
        session_id: ValidationSessionId,
    ) -> Result<Option<SessionPenaltyReport>> {
        let table = &self.inner.db.session_reports;
        let key = session_key(session_id);
        let Some(value) = self
            .inner
            .db
            .rocksdb()
            .get_cf(&table.cf(), key.as_slice())?
        else {
            return Ok(None);
        };

        let report = tl_proto::deserialize::<StoredSessionPenaltyReport>(&value)
            .context("failed to deserialize slasher session report")?
            .0;
        Ok(Some(report))
    }

    fn take_session_report(
        &self,
        session_id: ValidationSessionId,
    ) -> Result<Option<SessionPenaltyReport>> {
        let report = self.load_session_report(session_id)?;
        if report.is_some() {
            let key = session_key(session_id);
            self.inner.db.rocksdb().delete_cf_opt(
                &self.inner.db.session_reports.cf(),
                key.as_slice(),
                self.inner.db.session_reports.write_config(),
            )?;
        }
        Ok(report)
    }
}

struct Inner {
    db: SlasherDb,
}

#[derive(Clone)]
pub struct SlasherStorageSnapshot {
    db: SlasherDb,
    snapshot: Arc<OwnedSnapshot>,
}

impl SlasherStorageSnapshot {
    pub fn load_latest_session_id(&self) -> Result<Option<ValidationSessionId>> {
        let table = &self.db.block_batches;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);
        iter.seek_to_last();

        match iter.key() {
            Some(key) => Ok(Some(parse_session_id_prefix(key))),
            None => {
                iter.status()?;
                Ok(None)
            }
        }
    }

    pub fn load_distinct_session_ids(&self) -> Result<Vec<ValidationSessionId>> {
        let table = &self.db.block_batches;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);
        iter.seek_to_first();

        let mut items = Vec::new();
        let mut prev = None;
        loop {
            let key = match iter.key() {
                Some(key) => key,
                None => {
                    iter.status()?;
                    break;
                }
            };

            let session_id = parse_session_id_prefix(key);
            if prev != Some(session_id) {
                items.push(session_id);
                prev = Some(session_id);
            }

            iter.next();
        }

        Ok(items)
    }

    pub fn load_batches_for_session(
        &self,
        session_id: ValidationSessionId,
    ) -> Result<Vec<BlocksBatch>> {
        let table = &self.db.block_batches;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let prefix = session_key(session_id);
        read_config.set_iterate_lower_bound(prefix.as_slice());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);
        iter.seek(prefix.as_slice());

        let mut items = Vec::new();
        while let Some((key, value)) = iter.item() {
            if &key[0..tables::Sessions::KEY_LEN] != prefix.as_slice() {
                break;
            }

            let batch = tl_proto::deserialize::<StoredBlocksBatch>(value)
                .context("failed to deserialize slasher blocks batch")?
                .0;
            items.push(batch);
            iter.next();
        }
        iter.status()?;

        Ok(items)
    }

    pub fn load_session_report(
        &self,
        session_id: ValidationSessionId,
    ) -> Result<Option<SessionPenaltyReport>> {
        let table = &self.db.session_reports;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let key = session_key(session_id);
        let Some(value) =
            self.db
                .rocksdb()
                .get_pinned_cf_opt(&table.cf(), key.as_slice(), &read_config)?
        else {
            return Ok(None);
        };

        let report = tl_proto::deserialize::<StoredSessionPenaltyReport>(value.as_ref())
            .context("failed to deserialize slasher session report")?
            .0;
        Ok(Some(report))
    }
}

fn session_key(session_id: ValidationSessionId) -> [u8; tables::SessionReports::KEY_LEN] {
    let mut key = [0u8; tables::SessionReports::KEY_LEN];
    key[0..4].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
    key[4..8].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
    key
}

fn block_batches_key(
    session_id: ValidationSessionId,
    validator_idx: u16,
    start_seqno: u32,
) -> [u8; tables::BlockBatches::KEY_LEN] {
    let mut key = [0u8; tables::BlockBatches::KEY_LEN];
    key[0..4].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
    key[4..8].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
    key[8..10].copy_from_slice(&validator_idx.to_be_bytes());
    key[10..14].copy_from_slice(&start_seqno.to_be_bytes());
    key
}

fn parse_session_id_prefix(key: &[u8]) -> ValidationSessionId {
    ValidationSessionId {
        catchain_seqno: u32::from_be_bytes(key[0..4].try_into().unwrap()),
        vset_switch_round: u32::from_be_bytes(key[4..8].try_into().unwrap()),
    }
}
