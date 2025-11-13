use std::sync::Arc;

use anyhow::{Context, Result};
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use weedb::rocksdb::{Direction, IteratorMode, MergeOperands, WriteBatch};

use crate::models::{Round, UnixTime};
use crate::moderator::{RecordBatch, RecordFull, RecordKey, RecordValue, RecordValueShort};
use crate::storage::{MempoolDb, POINT_KEY_LEN, fill_point_key};

#[derive(Clone)]
pub struct JournalStore(Arc<MempoolDb>);

impl JournalStore {
    pub fn new(mempool_db: Arc<MempoolDb>) -> Self {
        Self(mempool_db)
    }

    pub fn load_restore(&self, since: UnixTime) -> Result<Vec<(RecordKey, RecordValueShort)>> {
        fn filter_parse(k: &[u8], v: &[u8]) -> Result<Option<(RecordKey, RecordValueShort)>> {
            let short = RecordValueShort::read_from(&mut &v[..])?;
            if !short.is_ban_related {
                return Ok(None);
            }
            let key = RecordKey::read_from(&mut &k[..])?;
            Ok(Some((key, short)))
        }
        let mut record_key: [u8; RecordKey::MAX_TL_BYTES] = [0; _];
        record_key[..UnixTime::MAX_TL_BYTES].copy_from_slice(&since.millis().to_be_bytes());
        (self.0.db.journal)
            .iterator(IteratorMode::From(&record_key, Direction::Forward))
            .filter_map_ok(|(k, v)| filter_parse(&k, &v).transpose())
            .flatten()
            .try_collect()
    }

    pub fn load_records(&self, count: u16, page: u32, asc: bool) -> Result<Vec<RecordFull>> {
        let mode = if asc {
            IteratorMode::Start
        } else {
            IteratorMode::End
        };
        (self.0.db.journal)
            .iterator(mode)
            .skip(count as usize * page as usize)
            .take(count as usize)
            .map_ok(|(k, v)| {
                let key = RecordKey::read_from(&mut &k[..])?;
                let value = RecordValue::read_from(&mut &v[..])?;
                Ok(RecordFull { key, value })
            })
            .flatten()
            .try_collect()
    }

    pub fn delete(&self, millis: std::ops::Range<UnixTime>) -> Result<()> {
        self.0.wait_for_compact()?;
        self.0.clean_events(millis)?;
        self.0.wait_for_compact()?;
        Ok(())
    }

    pub fn store_records(&self, batch: RecordBatch<'_>, conf_point_max_bytes: usize) -> Result<()> {
        if batch.records.is_empty() {
            return Ok(());
        }

        let (max_record_bytes, all_record_bytes) = {
            let mut max = 0;
            let all = (batch.records.iter())
                .map(|(_, record_value)| {
                    let record_bytes =
                        RecordKey::MAX_TL_BYTES + TlWrite::max_size_hint(record_value);
                    max = max.max(record_bytes);
                    record_bytes
                })
                .sum::<usize>();
            (max, all)
        };

        let mut write = WriteBatch::with_capacity_bytes(
            all_record_bytes
                + ({ batch.keys.len() + batch.points.len() } * {
                    (POINT_KEY_LEN + conf_point_max_bytes) // copied points
                        + (Round::MAX_TL_SIZE + UnixTime::MAX_TL_BYTES) // round to time index
                }),
        );
        {
            // fill records and indices
            let journal_cf = self.0.db.journal.cf();
            let journal_round_time = self.0.db.journal_round_time.cf();
            let mut record_bytes = Vec::with_capacity(max_record_bytes);
            for (record_key, record_value) in &batch.records {
                record_bytes.clear();
                record_key.write_to(&mut record_bytes);
                record_value.write_to(&mut record_bytes);

                write.put_cf(
                    &journal_cf,
                    &record_bytes[..RecordKey::MAX_TL_BYTES],
                    &record_bytes[RecordKey::MAX_TL_BYTES..],
                );
                for point_key in &record_value.point_keys {
                    write.merge_cf(
                        &journal_round_time,
                        &point_key[..Round::MAX_TL_SIZE], // key starts with round
                        &record_bytes[..UnixTime::MAX_TL_BYTES], // key starts with time
                    );
                }
            }
        }

        let journal_points_cf = self.0.db.journal_points.cf();

        // full points are most likely not yet stored in DB, but keys are most likely still stored;
        // looks like we cannot totally avoid writing duplicates ...

        let mut point_key_buf = [0; POINT_KEY_LEN];
        {
            assert!(batch.keys.is_sorted(), "batch point keys must be sorted");
            let mut target_iter = self.0.db.journal_points.raw_iterator();
            let mut source_iter = self.0.db.points.raw_iterator();
            for (round, digest) in batch.keys {
                fill_point_key(round.0, digest.inner(), &mut point_key_buf);

                target_iter.seek(point_key_buf);
                target_iter.status().context("target iter after seek")?;

                if target_iter.key().is_some_and(|key| key == point_key_buf) {
                    continue; // skip already stored points; target is smaller than source
                }

                source_iter.seek(point_key_buf);
                source_iter.status().context("source iter after seek")?;

                let Some((f_key, value)) = source_iter.item() else {
                    break; // source iter is exhausted
                };
                if f_key == point_key_buf {
                    write.put_cf(&journal_points_cf, f_key, value);
                } // else: skip not found points, though they shouldn't be GCed yet
            }
        }

        for ((round, digest), point) in batch.points {
            fill_point_key(round.0, digest.inner(), &mut point_key_buf);
            write.put_cf(&journal_points_cf, point_key_buf, point.serialized());
        }

        let db = self.0.db.rocksdb();
        db.write(write).context("write record batch")?;
        Ok(())
    }
}

/// Merge `event_round_time` cf: simply take the max big-endian value.
pub(super) fn merge_max_value(
    _key: &[u8],
    stored: Option<&[u8]>,
    new_status_queue: &MergeOperands,
) -> Option<Vec<u8>> {
    stored
        .into_iter()
        .chain(new_status_queue)
        .max()
        .map(|a| a.to_vec())
}
