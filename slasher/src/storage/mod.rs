use std::sync::Arc;

use anyhow::Result;
use tycho_slasher_traits::ValidationSessionId;
use tycho_storage::StorageContext;
use tycho_storage::kv::ApplyMigrations;
use tycho_types::cell::HashBytes;
use weedb::{OwnedSnapshot, WeeDb, rocksdb};

use self::db::{SlasherDb, SlasherDbExt, tables};
use self::models::{StoredBlocksBatch, StoredVsetInfo, StoredVsetReport};
use crate::BlocksBatch;
use crate::storage::db::SlasherTables;

pub mod db;
pub mod models;

const SLASHER_DB_SUBDIR: &str = "slasher";

#[derive(Clone)]
#[repr(transparent)]
pub struct SlasherStorage {
    inner: Arc<Inner>,
}

impl SlasherStorage {
    pub async fn open(ctx: &StorageContext) -> Result<Self> {
        let db: WeeDb<SlasherTables> = ctx.open_preconfigured(SLASHER_DB_SUBDIR)?;

        db.normalize_version().await?;
        db.apply_migrations().await?;

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

    pub fn store_blocks_batch(
        &self,
        vset_hash: &HashBytes,
        validator_idx: u16,
        batch: &BlocksBatch,
    ) -> Result<(), rocksdb::Error> {
        let key = block_batches_key(vset_hash, validator_idx, batch.start_seqno);
        let value = tl_proto::serialize(StoredBlocksBatch::wrap(batch));
        self.inner.db.block_batches.insert(key, value)?;
        Ok(())
    }

    pub fn store_vset_session(
        &self,
        vset_hash: &HashBytes,
        session_id: ValidationSessionId,
        start_seqno: u32,
    ) -> Result<(), rocksdb::Error> {
        let key = vset_session_key(vset_hash, session_id);
        self.inner
            .db
            .vset_sessions
            .insert(key, start_seqno.to_le_bytes())?;
        Ok(())
    }

    pub fn load_vset_info(&self, vset_hash: &HashBytes) -> Result<Option<StoredVsetInfo>> {
        let table = &self.inner.db.vset_state;
        match table.get(vset_state_key(vset_hash, tables::VsetState::KEY_TYPE_INFO))? {
            Some(data) => Ok(Some(tl_proto::deserialize::<StoredVsetInfo>(&data)?)),
            None => Ok(None),
        }
    }

    pub fn contains_vset_info(&self, vset_hash: &HashBytes) -> Result<bool, rocksdb::Error> {
        self.inner
            .db
            .vset_state
            .contains_key(vset_state_key(vset_hash, tables::VsetState::KEY_TYPE_INFO))
    }

    pub fn begin_vset(&self, vset_hash: &HashBytes, info: &StoredVsetInfo) -> Result<()> {
        let db = &self.inner.db;

        let mut batch = rocksdb::WriteBatch::new();

        // Remove vset before the previous validator set.
        if let Some(prev_vset) = self.load_vset_info(HashBytes::wrap(&info.prev_vset_hash))? {
            let vset_to_remove = HashBytes::wrap(&prev_vset.prev_vset_hash);

            batch.delete_range_cf(
                &db.vset_state.cf(),
                vset_state_key(vset_to_remove, 0),
                vset_state_key(vset_to_remove, u8::MAX),
            );
            batch.delete_range_cf(
                &db.vset_sessions.cf(),
                vset_session_key(vset_to_remove, ZERO_SESSION),
                vset_session_key(vset_to_remove, MAX_SESSION),
            );
            batch.delete_range_cf(
                &db.block_batches.cf(),
                block_batches_key(vset_to_remove, 0, 0),
                block_batches_key(vset_to_remove, u16::MAX, u32::MAX),
            );
        }

        batch.put_cf(
            &db.vset_state.cf(),
            vset_state_key(vset_hash, tables::VsetState::KEY_TYPE_INFO),
            tl_proto::serialize(info),
        );

        db.rocksdb()
            .write_opt(batch, db.vset_state.write_config())?;
        Ok(())
    }

    pub fn store_vset_report(
        &self,
        vset_hash: &HashBytes,
        report: &StoredVsetReport,
    ) -> Result<(), rocksdb::Error> {
        self.inner.db.vset_state.insert(
            vset_state_key(vset_hash, tables::VsetState::KEY_TYPE_REPORT),
            tl_proto::serialize(report),
        )
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
    pub fn iter_sessions(&self, vset_hash: &HashBytes) -> VsetSessionsIter<'_> {
        let mut readopts = self.db.vset_sessions.new_read_config();
        readopts.set_snapshot(&self.snapshot);
        readopts.set_iterate_lower_bound(vset_session_key(vset_hash, ZERO_SESSION));
        readopts.set_iterate_upper_bound(vset_session_key(vset_hash, MAX_SESSION));

        let mut raw = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&self.db.vset_sessions.cf(), readopts);
        raw.seek_to_first();

        VsetSessionsIter { raw, broken: false }
    }

    pub fn iter_block_batches(&self, vset_hash: &HashBytes) -> BlockBatchesIter<'_> {
        let mut readopts = self.db.block_batches.new_read_config();
        readopts.set_snapshot(&self.snapshot);
        readopts.set_iterate_lower_bound(block_batches_key(vset_hash, 0, 0));
        readopts.set_iterate_upper_bound(block_batches_key(vset_hash, u16::MAX, u32::MAX));

        let mut raw = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&self.db.block_batches.cf(), readopts);
        raw.seek_to_first();

        BlockBatchesIter { raw, broken: false }
    }
}

pub struct VsetSessionsIter<'a> {
    raw: rocksdb::DBRawIterator<'a>,
    broken: bool,
}

impl Iterator for VsetSessionsIter<'_> {
    type Item = Result<VsetSession>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.broken {
            return None;
        }

        let (key, value) = match self.raw.item() {
            Some(item) => item,
            None => match self.raw.status() {
                Ok(()) => return None,
                Err(e) => {
                    self.broken = true;
                    return Some(Err(e.into()));
                }
            },
        };

        let id = ValidationSessionId {
            catchain_seqno: u32::from_be_bytes(key[32..36].try_into().unwrap()),
            vset_switch_round: u32::from_be_bytes(key[36..40].try_into().unwrap()),
        };
        let start_seqno = u32::from_le_bytes(value[0..4].try_into().unwrap());

        self.raw.next();
        Some(Ok(VsetSession { id, start_seqno }))
    }
}

pub struct BlockBatchesIter<'a> {
    raw: rocksdb::DBRawIterator<'a>,
    broken: bool,
}

impl Iterator for BlockBatchesIter<'_> {
    type Item = Result<(u16, BlocksBatch)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.broken {
            return None;
        }

        let (key, value) = match self.raw.item() {
            Some(item) => item,
            None => match self.raw.status() {
                Ok(()) => return None,
                Err(e) => {
                    self.broken = true;
                    return Some(Err(e.into()));
                }
            },
        };

        let validator_idx = u16::from_be_bytes(key[32..34].try_into().unwrap());
        let batch = match tl_proto::deserialize(value) {
            Ok(StoredBlocksBatch(batch)) => batch,
            Err(e) => {
                let start_seqno = u32::from_be_bytes(key[34..38].try_into().unwrap());
                self.broken = true;
                return Some(Err(anyhow::anyhow!(
                    "invalid stored blocks batch \
                    (validator_idx={validator_idx}, start_seqno={start_seqno}): {e:?}"
                )));
            }
        };

        self.raw.next();
        Some(Ok((validator_idx, batch)))
    }
}

#[derive(Debug, Clone)]
pub struct VsetSession {
    // NOTE: Might be needed later.
    #[expect(unused)]
    pub id: ValidationSessionId,
    pub start_seqno: u32,
}

fn vset_state_key(vset_hash: &HashBytes, ty: u8) -> [u8; tables::VsetState::KEY_LEN] {
    let mut key = [0u8; tables::VsetState::KEY_LEN];
    key[..32].copy_from_slice(vset_hash.as_slice());
    key[32] = ty;
    key
}

fn vset_session_key(
    vset_hash: &HashBytes,
    session_id: ValidationSessionId,
) -> [u8; tables::VsetSessions::KEY_LEN] {
    let mut key = [0u8; tables::VsetSessions::KEY_LEN];
    key[0..32].copy_from_slice(vset_hash.as_slice());
    key[32..36].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
    key[36..40].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
    key
}

fn block_batches_key(
    vset_hash: &HashBytes,
    validator_idx: u16,
    start_seqno: u32,
) -> [u8; tables::BlockBatches::KEY_LEN] {
    let mut key = [0u8; tables::BlockBatches::KEY_LEN];
    key[0..32].copy_from_slice(vset_hash.as_slice());
    key[32..34].copy_from_slice(&validator_idx.to_be_bytes());
    key[34..38].copy_from_slice(&start_seqno.to_be_bytes());
    key
}

const ZERO_SESSION: ValidationSessionId = ValidationSessionId {
    catchain_seqno: 0,
    vset_switch_round: 0,
};
const MAX_SESSION: ValidationSessionId = ValidationSessionId {
    catchain_seqno: u32::MAX,
    vset_switch_round: u32::MAX,
};
