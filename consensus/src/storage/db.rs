use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use tycho_storage::StorageContext;
use tycho_storage::kv::NamedTables;
use tycho_util::metrics::HistogramGuard;
use weedb::WeeDb;
use weedb::rocksdb::{IteratorMode, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::models::{PointKey, Round, UnixTime};
use crate::moderator::RecordKey;
use crate::storage::tables::MempoolTables;
use crate::storage::time_to_round::TimeToRound;

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
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_events_time");

        let min_time = range.start.millis().to_be_bytes();
        let max_time = range.end.millis().to_be_bytes();

        let (min_ttr, max_ttr) = {
            let (mut min_ttr, mut max_ttr) = ([0; _], [0; _]);
            TimeToRound::new(range.start, Round(0)).fill(&mut min_ttr);
            TimeToRound::new(range.end, Round(0)).fill(&mut max_ttr);
            (min_ttr, max_ttr)
        };

        let journal_time_to_round_cf = self.db.tables().journal_time_to_round.cf();

        let (min_round, max_round) = {
            let mut ro = ReadOptions::default();
            ro.set_iterate_lower_bound(&min_ttr[..]);
            ro.set_iterate_upper_bound(&max_ttr[..]);
            (self.db.rocksdb())
                .iterator_cf_opt(&journal_time_to_round_cf, ro, IteratorMode::Start)
                .filter_map_ok(|(key, _)| TimeToRound::read_round(&key))
                .fold_ok((Round(u32::MAX), Round(0)), |(min, max), round| {
                    (min.min(round), max.max(round))
                })
                .map(|(min, max)| (min.min(max).0.to_be_bytes(), max.next().0.to_be_bytes()))
                .context("time to round iter fold")?
        };

        fn min_max<const N: usize>(prefix: usize, min: &[u8], max: &[u8]) -> ([u8; N], [u8; N]) {
            let (mut temp_min, mut temp_max) = ([0; N], [0; N]);
            temp_min[0..prefix].copy_from_slice(min);
            temp_max[0..prefix].copy_from_slice(max);
            (temp_min, temp_max)
        }

        let (record_key_min_incl, record_key_max_excl) =
            min_max::<{ RecordKey::MAX_TL_BYTES }>(UnixTime::MAX_TL_BYTES, &min_time, &max_time);

        let (point_key_min_incl, point_key_max_excl) =
            min_max::<{ PointKey::MAX_TL_BYTES }>(Round::MAX_TL_SIZE, &min_round, &max_round);

        let journal_cf = self.db.tables().journal.cf();
        let journal_points_cf = self.db.tables().journal_points.cf();
        let rocksdb = self.db.rocksdb();

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&journal_cf, &record_key_min_incl, &record_key_max_excl);
        batch.delete_range_cf(&journal_points_cf, &point_key_min_incl, &point_key_max_excl);
        batch.delete_range_cf(&journal_time_to_round_cf, &min_ttr, &max_ttr);
        rocksdb.write(batch)?;

        rocksdb.compact_range_cf(
            &journal_cf,
            Some(&record_key_min_incl),
            Some(&record_key_max_excl),
        );
        rocksdb.compact_range_cf(
            &journal_points_cf,
            Some(&point_key_min_incl),
            Some(&point_key_max_excl),
        );
        rocksdb.compact_range_cf(&journal_time_to_round_cf, Some(min_ttr), Some(max_ttr));

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
