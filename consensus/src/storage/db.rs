use std::collections::hash_map;
use std::sync::Arc;

use anyhow::Context;
use tl_proto::TlRead;
use tycho_storage::StorageContext;
use tycho_storage::kv::NamedTables;
use tycho_util::FastHashMap;
use tycho_util::metrics::HistogramGuard;
use weedb::WeeDb;
use weedb::rocksdb::{IteratorMode, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::models::{PointKey, Round, UnixTime};
use crate::moderator::{JournalPoint, RecordKey, RecordValue};
use crate::storage::tables::MempoolTables;

pub(super) const DB_CLEAN_ERRORS: &str = "tycho_mempool_db_clean_error_count";

pub struct MempoolDb {
    #[allow(unused, reason = "context must have at least the lifetime of the DB")]
    ctx: StorageContext,
    pub(super) db: WeeDb<MempoolTables>,
}

impl MempoolDb {
    /// Opens an existing or creates a new mempool `RocksDB` instance.
    pub fn open(ctx: StorageContext) -> anyhow::Result<Arc<Self>> {
        let db = ctx.open_preconfigured(MempoolTables::NAME)?;

        // TODO: Add migrations here if needed. However, it might require making this method `async`.

        Ok(Arc::new(Self { db, ctx }))
    }

    #[cfg(any(test, feature = "test"))]
    pub fn file_storage(&self) -> anyhow::Result<tycho_storage::fs::Dir> {
        self.ctx.root_dir().create_subdir("mempool_files")
    }

    /// delete all stored data up to provided value (exclusive);
    /// returns range of logically deleted keys
    pub(super) fn clean_points(
        &self,
        up_to_exclusive: &[u8; PointKey::MAX_TL_BYTES],
    ) -> anyhow::Result<Option<(Round, Round)>> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_points_time");
        let zero = [0; PointKey::MAX_TL_BYTES];
        let none = None::<[u8; PointKey::MAX_TL_BYTES]>;

        let status_cf = self.db.tables().points_status.cf();
        let info_cf = self.db.tables().points_info.cf();
        let points_cf = self.db.tables().points.cf();
        let rocksdb = self.db.rocksdb();

        let mut opt = ReadOptions::default();
        opt.set_iterate_upper_bound(up_to_exclusive);
        let mut iter = rocksdb.raw_iterator_cf_opt(&status_cf, opt);
        iter.status()?;
        iter.seek_to_first();
        iter.status()?;
        let first = iter.key().map(|first_key| {
            PointKey::parse_prefix(first_key).unwrap_or_else(|| {
                tracing::error!(
                    "mempool lower clean bound will be shown as 0: {}",
                    PointKey::format_loose(first_key)
                );
                Round::BOTTOM
            })
        });
        iter.status()?;
        iter.seek_to_last();
        iter.status()?;
        let last = iter
            .key()
            .map(|last_key| {
                PointKey::parse_prefix(last_key).unwrap_or_else(|| {
                    tracing::error!(
                        "mempool upper clean bound will be shown as 0: {}",
                        PointKey::format_loose(last_key)
                    );
                    Round::BOTTOM
                })
            })
            .or(first);
        iter.status()?;
        drop(iter);

        // in case we'll return to `db.delete_file_in_range_cf()`:
        // * at first delete status, as their absense prevents incorrect access to points data
        // * then delete info, as it leaves points usable only for upload
        // * at last delete points safely

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&status_cf, &zero, up_to_exclusive);
        batch.delete_range_cf(&info_cf, &zero, up_to_exclusive);
        batch.delete_range_cf(&points_cf, &zero, up_to_exclusive);
        rocksdb.write(batch)?;

        rocksdb.compact_range_cf(&status_cf, none, Some(up_to_exclusive));
        rocksdb.compact_range_cf(&info_cf, none, Some(up_to_exclusive));
        rocksdb.compact_range_cf(&points_cf, none, Some(up_to_exclusive));

        Ok(first.zip(last))
    }

    /// delete all stored event data up to provided time (exclusive);
    /// Note: deletes all points with rounds in time range, so some records are left without points
    pub(super) fn clean_events(&self, range: std::ops::Range<UnixTime>) -> anyhow::Result<()> {
        fn min_max<const N: usize>(prefix: usize, min: &[u8], max: &[u8]) -> ([u8; N], [u8; N]) {
            let (mut temp_min, mut temp_max) = ([0; N], [0; N]);
            temp_min[0..prefix].copy_from_slice(min);
            temp_max[0..prefix].copy_from_slice(max);
            (temp_min, temp_max)
        }

        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_events_time");

        let (record_key_min_incl, record_key_max_excl) = {
            let min_time = range.start.millis().to_be_bytes();
            let max_time = range.end.millis().to_be_bytes();
            min_max::<{ RecordKey::MAX_TL_BYTES }>(UnixTime::MAX_TL_BYTES, &min_time, &max_time)
        };

        let rocksdb = self.db.rocksdb();
        let journal_cf = self.db.tables().journal.cf();

        let mut point_refs_sub = FastHashMap::<PointKey, u32>::default();
        let (point_key_min_incl, point_key_max_excl) = {
            let mut ro = ReadOptions::default();
            ro.set_iterate_lower_bound(&record_key_min_incl[..]);
            ro.set_iterate_upper_bound(&record_key_max_excl[..]);
            let mut max_round = Round::BOTTOM;
            let mut min_round = Round(u32::MAX);
            for item in rocksdb.iterator_cf_opt(&journal_cf, ro, IteratorMode::Start) {
                let (_, v) = item.context("iter get journal record")?;
                let value =
                    RecordValue::read_from(&mut &v[..]).context("iter parse journal record")?;
                for point_ref in value.point_refs {
                    if point_ref.is_stored {
                        min_round = min_round.min(point_ref.key.round);
                        max_round = max_round.max(point_ref.key.round);
                        match point_refs_sub.entry(point_ref.key) {
                            hash_map::Entry::Occupied(mut exist) => *exist.get_mut() += 1,
                            hash_map::Entry::Vacant(empty) => _ = empty.insert(1),
                        }
                    }
                }
            }
            max_round = max_round.next(); // make exclusive
            min_round = min_round.min(max_round); // in case iter was empty
            let (min_round, max_round) = (min_round.0.to_be_bytes(), max_round.0.to_be_bytes());
            min_max::<{ PointKey::MAX_TL_BYTES }>(Round::MAX_TL_SIZE, &min_round, &max_round)
        };

        let j_points_cf = self.db.tables().journal_points.cf();

        let mut batch = WriteBatch::with_capacity_bytes(RecordKey::MAX_TL_BYTES * 2);
        batch.delete_range_cf(&journal_cf, &record_key_min_incl, &record_key_max_excl);

        {
            let mut j_point_bytes = [0; _];
            let mut ro = ReadOptions::default();
            ro.set_iterate_lower_bound(&point_key_min_incl[..]);
            ro.set_iterate_upper_bound(&point_key_max_excl[..]);
            for item in rocksdb.iterator_cf_opt(&j_points_cf, ro, IteratorMode::Start) {
                let (k, v) = item.context("iter get journal point")?;
                let key =
                    PointKey::read_from(&mut &k[..]).context("iter parse journal point key")?;
                let value = JournalPoint::read_from(&mut &v[..])
                    .context("iter parse journal point value")?;
                if let Some(ref_count) = point_refs_sub.remove(&key) {
                    if ref_count == value.ref_count {
                        batch.delete_cf(&j_points_cf, k);
                    } else {
                        JournalPoint::fill_sub(ref_count, &mut j_point_bytes);
                        batch.merge_cf(&j_points_cf, k, j_point_bytes);
                    }
                } else {
                    if value.ref_count == 0 {
                        batch.delete_cf(&j_points_cf, k);
                    }
                }
            }
        }

        rocksdb.write(batch)?;

        rocksdb.compact_range_cf(
            &journal_cf,
            Some(&record_key_min_incl),
            Some(&record_key_max_excl),
        );
        rocksdb.compact_range_cf(
            &j_points_cf,
            Some(&point_key_min_incl),
            Some(&point_key_max_excl),
        );

        Ok(())
    }

    /// Use when no reads/writes are possible, and this should finish prior other ops
    pub(super) fn wait_for_compact(&self) -> anyhow::Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_db_wait_for_compact_time");
        let mut opt = WaitForCompactOptions::default();
        opt.set_flush(true);
        self.db.rocksdb().wait_for_compact(&opt)?;
        Ok(())
    }

    /// returns `true` if only one value existed and was equal to provided arg
    ///
    /// if returns `false` - should drop all data and wait for compactions to finish
    pub(super) fn has_compatible_data(&self, data_version: &[u8]) -> anyhow::Result<bool> {
        let default_cf = self.db.rocksdb();

        let mut has_same = false;
        let mut other_values = Vec::<Box<[u8]>>::new();

        for result in default_cf.iterator(IteratorMode::Start) {
            let (key, _) = result?;
            if &*key == data_version {
                has_same = true;
            } else {
                other_values.push(key);
            }
        }

        if has_same & other_values.is_empty() {
            return Ok(true);
        }

        let mut batch = WriteBatch::default();

        for other in other_values {
            batch.delete(other);
        }
        if !has_same {
            batch.put(data_version, []);
        }
        default_cf.write(batch)?;

        default_cf.compact_range(None::<&[u8]>, None::<&[u8]>);

        Ok(false)
    }
}
