use std::collections::hash_map;

use anyhow::{Context, Result};
use tl_proto::TlRead;
use tycho_storage::StorageContext;
use tycho_storage::kv::{ApplyMigrations, NamedTables};
use tycho_util::FastHashMap;
use tycho_util::metrics::HistogramGuard;
use weedb::WeeDb;
use weedb::rocksdb::{IteratorMode, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::models::{PointKey, Round, UnixTime};
use crate::moderator::storage::tables::ModeratorTables;
use crate::moderator::{JournalPoint, RecordKey, RecordValue};

pub struct ModeratorDb {
    #[allow(unused, reason = "context must have at least the lifetime of the DB")]
    ctx: StorageContext,
    pub(super) db: WeeDb<ModeratorTables>,
}

impl ModeratorDb {
    /// Opens an existing or creates a new mempool `RocksDB` instance.
    pub fn open(ctx: StorageContext) -> Result<Self> {
        let db = ctx.open_preconfigured(ModeratorTables::NAME)?;
        Ok(Self { db, ctx })
    }

    pub async fn apply_migrations(&self) -> Result<()> {
        self.db.apply_migrations().await?;
        Ok(())
    }

    /// delete all stored event data up to provided time (exclusive);
    /// Note: deletes all points with rounds in time range, so some records are left without points
    pub fn clean_events(&self, range: std::ops::Range<UnixTime>) -> Result<()> {
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
    pub fn wait_for_compact(&self) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_db_wait_for_compact_time");
        let mut opt = WaitForCompactOptions::default();
        opt.set_flush(true);
        self.db.rocksdb().wait_for_compact(&opt)?;
        Ok(())
    }
}
