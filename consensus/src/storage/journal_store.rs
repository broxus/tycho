use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use weedb::rocksdb::{Direction, IteratorMode, WriteBatch};

use crate::models::{PointKey, Round, UnixTime};
use crate::moderator::{RecordBatch, RecordFull, RecordKey, RecordValue, RecordValueShort};
use crate::storage::MempoolDb;
use crate::storage::time_to_round::TimeToRound;

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

    pub fn get_point(&self, key: &PointKey) -> Result<Option<Bytes>> {
        let mut key_buf = [0; _];
        key.fill(&mut key_buf);

        let slice = self.0.db.journal_points.get_owned(&key_buf[..])?;
        Ok(slice.map(Bytes::from_owner))
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

        let mut write = WriteBatch::with_capacity_bytes(
            batch.all_record_bytes
                + ({ batch.keys.len() + batch.points.len() } * {
                    (PointKey::MAX_TL_BYTES + conf_point_max_bytes) // point size
                        + (Round::MAX_TL_SIZE + UnixTime::MAX_TL_BYTES) // round to time index
                }),
        );
        {
            // fill records and indices
            let journal_cf = self.0.db.journal.cf();
            let journal_time_to_round_cf = self.0.db.journal_time_to_round.cf();
            let mut record_bytes = Vec::with_capacity(batch.max_record_bytes);
            let mut ttr_buf = [0; _];
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
                    TimeToRound::new(record_key.created, point_key.round).fill(&mut ttr_buf);
                    write.put_cf(&journal_time_to_round_cf, ttr_buf, []);
                }
            }
        }

        let journal_points_cf = self.0.db.journal_points.cf();

        // full points are most likely not yet stored in DB, but keys are most likely still stored;
        // looks like we cannot totally avoid writing duplicates ...

        let mut point_key_buf = [0; PointKey::MAX_TL_BYTES];
        {
            assert!(batch.keys.is_sorted(), "batch point keys must be sorted");
            let mut target_iter = self.0.db.journal_points.raw_iterator();
            let mut source_iter = self.0.db.points.raw_iterator();
            for key in batch.keys {
                key.fill(&mut point_key_buf);

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

        for (key, point) in batch.points {
            key.fill(&mut point_key_buf);
            write.put_cf(&journal_points_cf, point_key_buf, point.serialized());
        }

        let db = self.0.db.rocksdb();
        db.write(write).context("write record batch")?;
        Ok(())
    }
}
