use std::sync::Arc;

use ahash::HashSetExt;
use anyhow::{Context, Result};
use bytes::{Buf, Bytes};
use itertools::Itertools;
use tl_proto::{RawBytes, TlRead, TlWrite};
use tycho_util::FastHashSet;
use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb::{Direction, IteratorMode, WriteBatch};

use crate::models::{PointKey, UnixTime};
use crate::moderator::storage::db::ModeratorDb;
use crate::moderator::{
    JournalPoint, JournalPointData, RecordBatch, RecordFull, RecordKey, RecordKind, RecordValue,
    RecordValueShort,
};
use crate::storage::{MempoolDb, meter_db_clean_error};

#[derive(Clone)]
pub struct JournalStore(Arc<JournalStoreInner>);

struct JournalStoreInner {
    this: ModeratorDb,
    main: Arc<MempoolDb>,
}

impl JournalStore {
    pub fn new(mempool_db: Arc<MempoolDb>) -> Result<Self> {
        let moderator_db = ModeratorDb::open(mempool_db.storage_context().clone())?;
        Ok(Self(Arc::new(JournalStoreInner {
            this: moderator_db,
            main: mempool_db,
        })))
    }

    pub async fn init(&self) -> Result<()> {
        self.0.this.apply_migrations().await
    }

    pub fn load_restore(
        &self,
        special_since: UnixTime,
        all_since: UnixTime,
    ) -> impl Iterator<Item = Result<(RecordKey, RecordValueShort)>> {
        fn filter_parse(
            k: &[u8],
            v: &[u8],
            all_since: UnixTime,
        ) -> Result<Option<(RecordKey, RecordValueShort)>> {
            let key = RecordKey::read_from(&mut &k[..])?;
            let short = RecordValueShort::read_from(&mut &v[..])?;
            Ok(if key.created < all_since {
                // have to load special records deeper than records of eny type;
                // now specials are only manual bans and unbans
                match short.kind {
                    RecordKind::Banned(_) | RecordKind::Unbanned => Some((key, short)),
                    RecordKind::NodeStarted | RecordKind::Event(_) => None,
                }
            } else if short.is_ban_related {
                Some((key, short))
            } else {
                None
            })
        }

        let mut min_key: [u8; RecordKey::MAX_TL_BYTES] = [0; _];
        min_key[..UnixTime::MAX_TL_BYTES]
            .copy_from_slice(&special_since.min(all_since).millis().to_be_bytes());

        (self.0.this.db.journal)
            .iterator(IteratorMode::From(&min_key, Direction::Forward))
            .filter_map_ok(move |(k, v)| filter_parse(&k, &v, all_since).transpose())
            .flatten()
    }

    pub fn load_records(&self, count: u16, page: u32, asc: bool) -> Result<Vec<RecordFull>> {
        let mode = if asc {
            IteratorMode::Start
        } else {
            IteratorMode::End
        };
        (self.0.this.db.journal)
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

        let Some(slice) = self.0.this.db.journal_points.get_owned(&key_buf[..])? else {
            return Ok(None);
        };
        let data_offset = match JournalPoint::read_from(&mut &slice[..])?.data {
            JournalPointData::Sub => return Ok(None),
            JournalPointData::Data(point) => slice.len() - point.into_inner().len(),
        };
        let mut point_bytes = Bytes::from_owner(slice);
        point_bytes.advance(data_offset);
        Ok(Some(point_bytes))
    }

    pub fn delete(&self, millis: std::ops::Range<UnixTime>) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_events_time");
        let range_string = || format!("{}..{}", millis.start, millis.end);
        match self.0.this.clean_events(millis.clone()) {
            Ok(()) => {}
            Err(e) => {
                meter_db_clean_error("journal");
                tracing::error!(
                    "delete range of mempool journal {} failed: {e}",
                    range_string()
                );
                return Err(e);
            }
        }
        match self.0.this.wait_for_compact() {
            Ok(()) => {}
            Err(e) => {
                meter_db_clean_error("compact_journal");
                tracing::error!(
                    "compact of mempool journal clean {} failed: {e}",
                    range_string()
                );
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn store_records(
        &self,
        mut batch: RecordBatch<'_>,
        journal_point_max_bytes: usize,
    ) -> Result<()> {
        if batch.records.is_empty() {
            return Ok(());
        }

        let mut found_keys = FastHashSet::with_capacity(batch.keys.len() + batch.points.len());

        let mut write = WriteBatch::with_capacity_bytes(
            batch.all_record_bytes
                + ({ batch.keys.len() + batch.points.len() } * {
                    PointKey::MAX_TL_BYTES + journal_point_max_bytes
                }),
        );

        let j_points_cf = self.0.this.db.journal_points.cf();
        let mut j_point_buf = Vec::with_capacity(journal_point_max_bytes);

        // full points are most likely not yet stored in DB, but keys are most likely still stored;
        // looks like we cannot totally avoid writing duplicates ...

        let mut point_key_buf = [0; PointKey::MAX_TL_BYTES];
        {
            assert!(batch.keys.is_sorted(), "batch point keys must be sorted");
            let mut target_iter = self.0.this.db.journal_points.raw_iterator();
            let mut source_iter = self.0.main.points_raw_iter();
            'keys: for (key, ref_count) in &batch.keys {
                key.fill(&mut point_key_buf);

                target_iter.seek(point_key_buf);
                target_iter.status().context("target iter after seek")?;

                let raw_bytes = if target_iter.key().is_some_and(|key| key == point_key_buf) {
                    let value = target_iter.value().context("target iter get value")?;
                    // re-store the same point because it may be concurrently deleted
                    let j_point: JournalPoint<'_> =
                        tl_proto::deserialize(value).context("extract point data")?;
                    match j_point.data {
                        JournalPointData::Sub => anyhow::bail!("only Sub stored for {key:?}"),
                        JournalPointData::Data(raw_bytes) => raw_bytes,
                    }
                } else {
                    source_iter.seek(point_key_buf);
                    source_iter.status().context("source iter after seek")?;

                    if source_iter.key().is_some_and(|key| key == point_key_buf) {
                        let bytes = source_iter.value().context("source iter get value")?;
                        RawBytes::new(bytes)
                    } else {
                        continue 'keys;
                    }
                };

                found_keys.insert(key);
                let data = JournalPointData::Data(raw_bytes);
                let ref_count = *ref_count;

                (JournalPoint { ref_count, data }).write_to(&mut j_point_buf);
                write.merge_cf(&j_points_cf, &point_key_buf[..], &j_point_buf[..]);
                j_point_buf.clear();
            }
        }

        for (key, (ref_count, point)) in &batch.points {
            key.fill(&mut point_key_buf);

            found_keys.insert(key);
            let data = JournalPointData::Data(RawBytes::new(point.serialized()));
            let ref_count = *ref_count;

            JournalPoint { ref_count, data }.write_to(&mut j_point_buf);
            write.merge_cf(&j_points_cf, &point_key_buf[..], &j_point_buf[..]);
            j_point_buf.clear();
        }

        // fill records and indices
        let journal_cf = self.0.this.db.journal.cf();
        let mut record_bytes = Vec::with_capacity(batch.max_record_bytes);
        for (record_key, record_value) in &mut batch.records {
            for point_ref in &mut record_value.point_refs {
                point_ref.is_stored = found_keys.contains(&point_ref.key);
            }

            record_bytes.clear();
            record_key.write_to(&mut record_bytes);
            record_value.write_to(&mut record_bytes);

            write.put_cf(
                &journal_cf,
                &record_bytes[..RecordKey::MAX_TL_BYTES],
                &record_bytes[RecordKey::MAX_TL_BYTES..],
            );
        }

        let db = self.0.this.db.rocksdb();
        db.write(write).context("write record batch")?;
        Ok(())
    }
}
