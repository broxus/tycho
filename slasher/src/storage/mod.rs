use std::sync::Arc;

use anyhow::Result;
use tycho_slasher_traits::ValidationSessionId;
use tycho_storage::StorageContext;
use tycho_types::cell::HashBytes;
use weedb::{OwnedSnapshot, rocksdb};

use self::db::{SlasherDb, tables};
use self::models::StoredBlocksBatch;
use crate::BlocksBatch;

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

    pub fn store_blocks_batch(
        &self,
        vset_hash: &HashBytes,
        validator_idx: u16,
        batch: &BlocksBatch,
    ) -> Result<()> {
        let key = block_batches_key(vset_hash, validator_idx, batch.start_seqno);
        let value = tl_proto::serialize(StoredBlocksBatch::wrap(batch));
        self.inner.db.block_batches.insert(key, value)?;
        Ok(())
    }

    pub fn store_vset_session(
        &self,
        session_id: ValidationSessionId,
        vset_hash: &HashBytes,
        start_seqno: u32,
    ) -> Result<()> {
        let key = session_key(session_id);
        let value = session_value(vset_hash, start_seqno);
        self.inner.db.vset_sessions.insert(key, value)?;
        Ok(())
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
    pub fn load_vset_sessions(&self, vset_hash: &HashBytes) -> Result<Vec<VsetSession>> {
        let mut iter = self.snapshot.raw_iterator_cf_opt(
            &self.db.vset_sessions.cf(),
            self.db.vset_sessions.new_read_config(),
        );
        iter.seek_to_last();

        let mut result = Vec::new();
        let mut vset_seen = false;
        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => {
                    iter.status()?;
                    break;
                }
            };

            let session_vset_hash = HashBytes::from_slice(&value[0..32]);
            if &session_vset_hash == vset_hash {
                vset_seen = true;
                let session_id = ValidationSessionId {
                    catchain_seqno: u32::from_be_bytes(key[0..4].try_into().unwrap()),
                    vset_switch_round: u32::from_be_bytes(key[4..8].try_into().unwrap()),
                };

                let start_seqno = u32::from_le_bytes(value[32..36].try_into().unwrap());

                result.push(VsetSession {
                    id: session_id,
                    start_seqno,
                });
            } else if vset_seen {
                break;
            }

            iter.prev();
        }

        Ok(result)
    }

    pub fn iter_block_batches(&self, vset_hash: &HashBytes) -> BlockBatchesIter<'_> {
        let mut readopts = self.db.block_batches.new_read_config();
        readopts.set_snapshot(&self.snapshot);
        let mut key = block_batches_key(vset_hash, 0, 0);
        readopts.set_iterate_lower_bound(key);
        key[32..].fill(0xff);
        readopts.set_iterate_upper_bound(key);

        let mut raw = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&self.db.block_batches.cf(), readopts);
        raw.seek_to_first();

        BlockBatchesIter { raw, broken: false }
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
                let start_seqno = u32::from_be_bytes(key[34..48].try_into().unwrap());
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

fn session_key(session_id: ValidationSessionId) -> [u8; tables::VsetSessions::KEY_LEN] {
    let mut key = [0u8; tables::VsetSessions::KEY_LEN];
    key[..4].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
    key[4..8].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
    key
}

fn session_value(vset_hash: &HashBytes, start_seqno: u32) -> [u8; tables::VsetSessions::VALUE_LEN] {
    let mut value = [0u8; tables::VsetSessions::VALUE_LEN];
    value[0..32].copy_from_slice(vset_hash.as_slice());
    value[32..36].copy_from_slice(&start_seqno.to_le_bytes());
    value
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
