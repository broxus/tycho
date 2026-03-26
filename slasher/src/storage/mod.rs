use std::sync::Arc;

use anyhow::{Context, Result};
use tycho_slasher_traits::ValidationSessionId;
use tycho_storage::StorageContext;
use tycho_types::cell::HashBytes;
use weedb::OwnedSnapshot;

use self::db::{SlasherDb, tables};
use self::models::{
    StoredBlocksBatch, StoredSessionMeta, StoredSessionPenaltyReport, StoredVsetEpoch,
    StoredVsetPenaltyReport,
};
use crate::BlocksBatch;
use crate::analyzer::{
    ObservedBlocksBatch, SessionMeta, SessionPenaltyReport, VsetEpoch, VsetPenaltyReport,
};

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

    pub fn snapshot(&self) -> SlasherStorageSnapshot {
        SlasherStorageSnapshot {
            db: self.inner.db.clone(),
            snapshot: Arc::new(self.inner.db.owned_snapshot()),
        }
    }

    pub fn update_current_vset_epoch(
        &self,
        current_session_id: ValidationSessionId,
        current_vset_hash: HashBytes,
    ) -> Result<()> {
        let latest = self.load_latest_vset_epoch()?;

        match latest {
            // just same vset. do nothing
            Some(epoch) if epoch.vset_hash == current_vset_hash => Ok(()),
            // we have new session. old persists for analyze
            Some(mut epoch) => {
                if epoch.next_epoch_start_session_id.is_none() {
                    epoch.next_epoch_start_session_id = Some(current_session_id);
                    self.store_vset_epoch(&epoch)?;
                }

                self.store_vset_epoch(&VsetEpoch {
                    start_session_id: current_session_id,
                    vset_hash: current_vset_hash,
                    next_epoch_start_session_id: None,
                })
            }
            None => self.store_vset_epoch(&VsetEpoch {
                start_session_id: current_session_id,
                vset_hash: current_vset_hash,
                next_epoch_start_session_id: None,
            }),
        }
    }

    pub fn store_blocks_batch(
        &self,
        session_id: ValidationSessionId,
        observer_validator_idx: u16,
        batch: &BlocksBatch,
    ) -> Result<bool> {
        let Some(epoch) = self.find_epoch_for_session(session_id)? else {
            return Ok(false);
        };

        let key = block_batches_key(session_id, observer_validator_idx, batch.start_seqno);
        let value = tl_proto::serialize(StoredBlocksBatch::wrap(batch));
        self.inner.db.block_batches.insert(key.as_slice(), value)?;

        let mut validator_indices = batch
            .signatures_history
            .iter()
            .map(|item| item.validator_idx)
            .collect::<Vec<_>>();

        validator_indices.sort();
        validator_indices.dedup();

        // TODO: just upsert for now, maybe we can load and then save if absent
        self.store_session_meta(&SessionMeta {
            session_id,
            epoch_start_session_id: epoch.start_session_id,
            validator_indices,
        })?;

        self.clear_intermediate_data(&epoch, &session_id)?;

        Ok(true)
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

    pub fn store_vset_report(&self, report: &VsetPenaltyReport) -> Result<()> {
        let key = session_key(report.epoch_start_session_id);
        let value = tl_proto::serialize(StoredVsetPenaltyReport::wrap(report));
        self.inner.db.vset_reports.insert(key.as_slice(), value)?;
        Ok(())
    }

    fn store_vset_epoch(&self, epoch: &VsetEpoch) -> Result<()> {
        let key = session_key(epoch.start_session_id);
        let value = tl_proto::serialize(StoredVsetEpoch::wrap(epoch));
        self.inner.db.vset_epochs.insert(key.as_slice(), value)?;
        Ok(())
    }

    fn store_session_meta(&self, meta: &SessionMeta) -> Result<()> {
        let key = session_meta_key(meta.epoch_start_session_id, meta.session_id);
        let value = tl_proto::serialize(StoredSessionMeta::wrap(meta));
        self.inner.db.session_meta.insert(key.as_slice(), value)?;
        Ok(())
    }

    // todo: should we clean after each batch or just after session
    fn clear_intermediate_data(
        &self,
        epoch: &VsetEpoch,
        session_id: &ValidationSessionId,
    ) -> Result<()> {
        self.delete_session_report(*session_id)?;
        self.delete_vset_report(epoch.start_session_id)?;
        Ok(())
    }

    fn delete_session_report(&self, session_id: ValidationSessionId) -> Result<()> {
        self.delete_by_key(
            &self.inner.db.session_reports.cf(),
            session_key(session_id).as_slice(),
            self.inner.db.session_reports.write_config(),
        )
    }

    fn delete_vset_report(&self, epoch_start_session_id: ValidationSessionId) -> Result<()> {
        self.delete_by_key(
            &self.inner.db.vset_reports.cf(),
            session_key(epoch_start_session_id).as_slice(),
            self.inner.db.vset_reports.write_config(),
        )
    }

    fn delete_by_key(
        &self,
        cf: &impl weedb::rocksdb::AsColumnFamilyRef,
        key: &[u8],
        write_config: &weedb::rocksdb::WriteOptions,
    ) -> Result<()> {
        self.inner
            .db
            .rocksdb()
            .delete_cf_opt(cf, key, write_config)?;
        Ok(())
    }

    fn load_latest_vset_epoch(&self) -> Result<Option<VsetEpoch>> {
        let table = &self.inner.db.vset_epochs;
        let read_config = table.new_read_config();
        let cf = table.cf();
        let mut iter = self
            .inner
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&cf, read_config);
        iter.seek_to_last();

        let epoch = match iter.item() {
            Some((key, value)) => Some(parse_vset_epoch(key, value)?),
            None => {
                iter.status()?;
                None
            }
        };
        Ok(epoch)
    }

    fn find_epoch_for_session(&self, session_id: ValidationSessionId) -> Result<Option<VsetEpoch>> {
        let table = &self.inner.db.vset_epochs;
        let read_config = table.new_read_config();
        let cf = table.cf();
        let mut iter = self
            .inner
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&cf, read_config);
        let key = session_key(session_id);
        iter.seek_for_prev(key.as_slice());

        let epoch = match iter.item() {
            Some((key, value)) => Some(parse_vset_epoch(key, value)?),
            None => {
                iter.status()?;
                None
            }
        };
        Ok(epoch)
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
    pub fn load_closed_vset_epochs(&self) -> Result<Vec<VsetEpoch>> {
        let table = &self.db.vset_epochs;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);
        iter.seek_to_first();

        let mut items = Vec::new();
        while let Some((key, value)) = iter.item() {
            let epoch = parse_vset_epoch(key, value)?;
            if epoch.next_epoch_start_session_id.is_some() {
                items.push(epoch);
            }
            iter.next();
        }
        iter.status()?;

        Ok(items)
    }

    pub fn load_sessions_for_epoch(
        &self,
        epoch_start_session_id: ValidationSessionId,
    ) -> Result<Vec<SessionMeta>> {
        let table = &self.db.session_meta;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let prefix = session_key(epoch_start_session_id);
        read_config.set_iterate_lower_bound(prefix.as_slice());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);
        iter.seek(prefix.as_slice());

        let mut items = Vec::new();
        while let Some((key, value)) = iter.item() {
            if &key[..tables::VsetEpochs::KEY_LEN] != prefix.as_slice() {
                break;
            }

            items.push(parse_session_meta(key, value)?);
            iter.next();
        }
        iter.status()?;

        Ok(items)
    }

    pub fn load_observed_batches_for_session(
        &self,
        session_id: ValidationSessionId,
    ) -> Result<Vec<ObservedBlocksBatch>> {
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
            if &key[..tables::SessionReports::KEY_LEN] != prefix.as_slice() {
                break;
            }

            let batch = tl_proto::deserialize::<StoredBlocksBatch>(value)
                .context("failed to deserialize slasher blocks batch")?
                .0;
            items.push(ObservedBlocksBatch {
                observer_validator_idx: parse_observer_validator_idx(key),
                batch,
            });
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

    pub fn load_vset_report(
        &self,
        epoch_start_session_id: ValidationSessionId,
    ) -> Result<Option<VsetPenaltyReport>> {
        let table = &self.db.vset_reports;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(self.snapshot.as_ref());

        let key = session_key(epoch_start_session_id);
        let Some(value) =
            self.db
                .rocksdb()
                .get_pinned_cf_opt(&table.cf(), key.as_slice(), &read_config)?
        else {
            return Ok(None);
        };

        let report = tl_proto::deserialize::<StoredVsetPenaltyReport>(value.as_ref())
            .context("failed to deserialize slasher vset report")?
            .0;
        Ok(Some(report))
    }
}

fn session_key(session_id: ValidationSessionId) -> [u8; tables::SessionReports::KEY_LEN] {
    let mut key = [0u8; tables::SessionReports::KEY_LEN];
    key[..4].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
    key[4..8].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
    key
}

fn session_meta_key(
    epoch_start_session_id: ValidationSessionId,
    session_id: ValidationSessionId,
) -> [u8; tables::SessionMeta::KEY_LEN] {
    let mut key = [0u8; tables::SessionMeta::KEY_LEN];
    key[..8].copy_from_slice(&session_key(epoch_start_session_id));
    key[8..12].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
    key[12..16].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
    key
}

fn block_batches_key(
    session_id: ValidationSessionId,
    observer_validator_idx: u16,
    start_seqno: u32,
) -> [u8; tables::BlockBatches::KEY_LEN] {
    let mut key = [0u8; tables::BlockBatches::KEY_LEN];
    key[..8].copy_from_slice(&session_key(session_id));
    key[8..10].copy_from_slice(&observer_validator_idx.to_be_bytes());
    key[10..14].copy_from_slice(&start_seqno.to_be_bytes());
    key
}

fn parse_session_id_prefix(key: &[u8]) -> ValidationSessionId {
    ValidationSessionId {
        catchain_seqno: u32::from_be_bytes(key[..4].try_into().unwrap()),
        vset_switch_round: u32::from_be_bytes(key[4..8].try_into().unwrap()),
    }
}

fn parse_observer_validator_idx(key: &[u8]) -> u16 {
    u16::from_be_bytes(key[8..10].try_into().unwrap())
}

fn parse_vset_epoch(key: &[u8], value: &[u8]) -> Result<VsetEpoch> {
    let mut epoch = tl_proto::deserialize::<StoredVsetEpoch>(value)
        .context("failed to deserialize slasher vset epoch")?
        .0;
    epoch.start_session_id = parse_session_id_prefix(key);
    Ok(epoch)
}

fn parse_session_meta(key: &[u8], value: &[u8]) -> Result<SessionMeta> {
    let mut meta = tl_proto::deserialize::<StoredSessionMeta>(value)
        .context("failed to deserialize slasher session meta")?
        .0;
    meta.epoch_start_session_id = parse_session_id_prefix(&key[..8]);
    meta.session_id = ValidationSessionId {
        catchain_seqno: u32::from_be_bytes(key[8..12].try_into().unwrap()),
        vset_switch_round: u32::from_be_bytes(key[12..16].try_into().unwrap()),
    };
    Ok(meta)
}
