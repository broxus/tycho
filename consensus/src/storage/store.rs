use std::cmp;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use tycho_network::OverlayId;
use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb::{DBRawIterator, IteratorMode, ReadOptions, WriteBatch};

use crate::effects::AltFormat;
use crate::models::{
    Point, PointInfo, PointKey, PointRestore, PointRestoreSelect, PointStatusStored,
    PointStatusStoredRef, Round,
};
use crate::storage::{MempoolDb, StatusFlags};

#[derive(Clone)]
pub struct MempoolStore(Arc<dyn MempoolStoreImpl>);

trait MempoolStoreImpl: Send + Sync {
    fn insert_point(&self, point: &Point, status: PointStatusStoredRef<'_>) -> Result<()>;

    fn set_status(&self, key: &PointKey, status: PointStatusStoredRef<'_>) -> Result<()>;

    fn get_point(&self, key: &PointKey) -> Result<Option<Point>>;

    fn multi_get_info(&self, keys: &[PointKey]) -> Result<Vec<PointInfo>>;

    fn get_point_raw(&self, key: &PointKey) -> Result<Option<Bytes>>;

    fn get_info(&self, key: &PointKey) -> Result<Option<PointInfo>>;

    fn get_status(&self, key: &PointKey) -> Result<Option<PointStatusStored>>;

    fn last_round(&self) -> Result<Option<Round>>;

    fn reset_statuses(&self, range: &RangeInclusive<Round>) -> Result<()>;

    fn load_restore(&self, range: &RangeInclusive<Round>) -> Result<Vec<PointRestoreSelect>>;

    fn init_storage(&self, overlay_id: &OverlayId) -> Result<()>;
}

impl MempoolStore {
    pub fn new(mempool_db: Arc<MempoolDb>) -> Self {
        Self(mempool_db)
    }

    #[cfg(any(feature = "test", test))]
    pub fn no_read_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn insert_point(&self, point: &Point, status: PointStatusStoredRef<'_>) {
        self.0
            .insert_point(point, status)
            .with_context(|| format!("id {:?}", point.info().id().alt()))
            .expect("DB insert point full");
    }

    pub fn set_status(&self, key: &PointKey, status: PointStatusStoredRef<'_>) {
        self.0
            .set_status(key, status)
            .with_context(|| key.alt().to_string())
            .expect("DB set point status");
    }

    pub fn get_point(&self, key: &PointKey) -> Option<Point> {
        self.0
            .get_point(key)
            .with_context(|| key.alt().to_string())
            .expect("DB get point")
    }

    pub fn get_point_raw(&self, key: &PointKey) -> Option<Bytes> {
        self.0
            .get_point_raw(key)
            .with_context(|| key.alt().to_string())
            .expect("DB get point raw")
    }

    pub fn multi_get_info(&self, keys: &[PointKey]) -> Vec<PointInfo> {
        self.0.multi_get_info(keys).expect("DB multi get points")
    }

    #[allow(dead_code, reason = "idiomatic getter may come in useful")]
    pub fn get_info(&self, key: &PointKey) -> Option<PointInfo> {
        self.0
            .get_info(key)
            .with_context(|| key.alt().to_string())
            .expect("DB get point info")
    }

    pub fn get_status(&self, key: &PointKey) -> Option<PointStatusStored> {
        self.0
            .get_status(key)
            .with_context(|| key.alt().to_string())
            .expect("DB get point status")
    }

    pub fn last_round(&self) -> Option<Round> {
        self.0.last_round().expect("DB load last round")
    }

    pub fn reset_statuses(&self, range: &RangeInclusive<Round>) {
        self.0
            .reset_statuses(range)
            .with_context(|| format!("range [{}..={}]", range.start().0, range.end().0))
            .expect("DB reset statuses");
    }

    pub fn load_restore(&self, range: &RangeInclusive<Round>) -> Vec<PointRestoreSelect> {
        self.0
            .load_restore(range)
            .with_context(|| format!("range [{}..={}]", range.start().0, range.end().0))
            .expect("DB load restore")
    }

    pub fn init_storage(&self, overlay_id: &OverlayId) {
        self.0
            .init_storage(overlay_id)
            .with_context(|| format!("new overlay id {overlay_id}"))
            .expect("DB drop all data");
    }
}

impl MempoolStoreImpl for MempoolDb {
    fn insert_point(&self, point: &Point, status: PointStatusStoredRef<'_>) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_insert_point_time");
        let mut key_buf = [0; _];
        point.info().key().fill(&mut key_buf);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();
        let info_cf = self.db.points_info.cf();
        let status_cf = self.db.points_status.cf();

        // transaction not needed as there is no concurrent puts for the same key,
        // as they occur inside DAG futures whose uniqueness is protected by dash map;
        // in contrast, status are written from random places, but only via `merge_cf()`
        let mut batch =
            WriteBatch::with_capacity_bytes(point.serialized().len() + PointInfo::MAX_BYTE_SIZE);

        let mut buffer = Vec::<u8>::with_capacity(PointInfo::MAX_BYTE_SIZE);

        batch.put_cf(&points_cf, &key_buf[..], point.serialized());

        point.info().write_to(&mut buffer);
        batch.put_cf(&info_cf, &key_buf[..], &buffer);

        buffer.clear();

        status.write_to(&mut buffer);

        batch.merge_cf(&status_cf, &key_buf[..], &buffer);

        Ok(db.write(batch)?)
    }

    fn set_status(&self, key: &PointKey, status: PointStatusStoredRef<'_>) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_status_time");
        let mut key_buf = [0; _];
        key.fill(&mut key_buf);

        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();

        Ok(db.merge_cf(&status_cf, &key_buf[..], status.encode())?)
    }

    fn get_point(&self, key: &PointKey) -> Result<Option<Point>> {
        metrics::counter!("tycho_mempool_store_get_point_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_time");
        let mut key_buf = [0; _];
        key.fill(&mut key_buf);

        let points = &self.db.points;
        points
            .get(&key_buf[..])
            .context("db get")?
            .map(|a| Point::from_bytes(a.to_vec()).context("deserialize db point"))
            .transpose()
    }

    fn multi_get_info(&self, keys: &[PointKey]) -> Result<Vec<PointInfo>> {
        let key_bytes = {
            let mut b_keys = Vec::with_capacity(keys.len());
            let mut key_buf = [0; _];
            for key in keys {
                key.fill(&mut key_buf);
                b_keys.push(key_buf);
            }
            b_keys
        };

        anyhow::ensure!(key_bytes.is_sorted(), "key bytes must be sorted");

        let mut infos = Vec::with_capacity(keys.len());
        for (result_option_bytes, key) in (self.db.points_info)
            .batched_multi_get(&key_bytes, true)
            .into_iter()
            .zip_eq(keys)
        {
            let option_bytes =
                result_option_bytes.with_context(|| format!("result for {key:?}"))?;
            let bytes = option_bytes.with_context(|| format!("not found {key:?}"))?;
            let info = tl_proto::deserialize::<PointInfo>(&bytes)
                .with_context(|| format!("deserialize db point info {key:?}"))?;
            anyhow::ensure!(info.key() == *key, "found {:?} instead {key:?}", info.key());
            infos.push(info);
        }
        Ok(infos)
    }

    fn get_point_raw(&self, key: &PointKey) -> Result<Option<Bytes>> {
        metrics::counter!("tycho_mempool_store_get_point_raw_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_raw_time");
        let mut key_buf = [0; _];
        key.fill(&mut key_buf);

        let points = &self.db.points;
        let point_bytes = points.get_owned(&key_buf[..])?;
        Ok(point_bytes.map(Bytes::from_owner))
    }

    fn get_info(&self, key: &PointKey) -> Result<Option<PointInfo>> {
        metrics::counter!("tycho_mempool_store_get_info_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_info_time");
        let mut key_buf = [0; _];
        key.fill(&mut key_buf);

        let table = &self.db.points_info;
        table
            .get(&key_buf[..])?
            .map(|a| tl_proto::deserialize::<PointInfo>(&a).context("deserialize point info"))
            .transpose()
    }

    fn get_status(&self, key: &PointKey) -> Result<Option<PointStatusStored>> {
        metrics::counter!("tycho_mempool_store_get_status_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_status_time");
        let mut key_buf = [0; _];
        key.fill(&mut key_buf);

        let table = &self.db.points_status;
        table
            .get(&key_buf[..])
            .context("db get point status")?
            .as_deref()
            .map(PointStatusStored::decode)
            .transpose()
    }

    fn last_round(&self) -> Result<Option<Round>> {
        let Some((last_key, _)) = (self.db.points_status)
            .iterator(IteratorMode::End)
            .next()
            .transpose()?
        else {
            return Ok(None);
        };

        let round = PointKey::parse_prefix(&last_key).context("bad round bytes")?;

        anyhow::ensure!(round.0 > 0, "key with zero round");

        Ok(Some(round))
    }

    fn reset_statuses(&self, range: &RangeInclusive<Round>) -> Result<()> {
        let status_t = &self.db.tables().points_status;
        let db = self.db.rocksdb();

        let mut start = [0; _];
        PointKey::fill_prefix(*range.start(), &mut start);

        let mut end_excl = [0; _];
        PointKey::fill_prefix(range.end().next(), &mut end_excl);

        let mut conf = status_t.new_read_config();
        conf.set_iterate_lower_bound(start);
        conf.set_iterate_upper_bound(end_excl);
        let iter = db.iterator_cf_opt(&status_t.cf(), conf, IteratorMode::Start);

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&status_t.cf(), start, end_excl);
        for kv in iter {
            let (k, v) = kv.context("status iter next")?;
            let parsed_flags =
                StatusFlags::try_from_stored(&v).with_context(|| PointKey::format_loose(&k))?;
            let new_v = match parsed_flags {
                Some(flags) if !flags.contains(StatusFlags::Found) => &*v,
                _ => &[],
            };
            batch.put_cf(&status_t.cf(), k, new_v);
        }

        db.write(batch)?;

        db.compact_range_cf(&status_t.cf(), Some(start), Some(end_excl));

        self.wait_for_compact()
    }

    fn load_restore(&self, range: &RangeInclusive<Round>) -> Result<Vec<PointRestoreSelect>> {
        fn opts(range: &RangeInclusive<Round>) -> ReadOptions {
            let mut opts = ReadOptions::default();
            let mut key_buf = [0; _];
            PointKey::fill_prefix(*range.start(), &mut key_buf);
            opts.set_iterate_lower_bound(key_buf);
            PointKey::fill_prefix(range.end().next(), &mut key_buf);
            opts.set_iterate_upper_bound(key_buf);
            opts
        }

        fn get_value<T>(iter: &mut DBRawIterator<'_>, key: &[u8]) -> Result<T>
        where
            for<'b> T: TlRead<'b>,
        {
            iter.status().context("before seek")?;
            iter.seek(key);
            iter.status().context("after seek")?;
            let (f_key, value) = iter.item().context("iter exhausted")?;
            match key.cmp(f_key) {
                cmp::Ordering::Less => {
                    let f_key_str = PointKey::format_loose(f_key);
                    anyhow::bail!("iter did not seek, found key {f_key_str}")
                }
                cmp::Ordering::Equal => {
                    Ok(tl_proto::deserialize::<T>(value).context("deserialize")?)
                }
                cmp::Ordering::Greater => {
                    let f_key_str = PointKey::format_loose(f_key);
                    anyhow::bail!("no record found, next key {f_key_str}")
                }
            }
        }

        let mut result = Vec::new();

        let status_iter = (self.db.rocksdb()).iterator_cf_opt(
            &self.db.points_status.cf(),
            opts(range),
            IteratorMode::Start,
        );
        let mut info_iter =
            (self.db.rocksdb()).raw_iterator_cf_opt(&self.db.points_info.cf(), opts(range));

        for item in status_iter {
            let (key_bytes, status_bytes) = item.context("get point status")?;
            let status = PointStatusStored::decode(&status_bytes)?;

            let key = PointKey::read_from(&mut &key_bytes[..])
                .with_context(|| PointKey::format_loose(&key_bytes))?;

            match status {
                PointStatusStored::Exists => {
                    result.push(PointRestoreSelect::NeedsVerify(key));
                }
                PointStatusStored::NotFound(status) => {
                    let ready = PointRestore::NotFound(key, status);
                    result.push(PointRestoreSelect::Ready(ready));
                }
                PointStatusStored::Validated(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key_bytes)
                        .with_context(|| format!("table point info, status {status} {key:?}"))?;
                    let ready = PointRestore::Validated(info, status);
                    result.push(PointRestoreSelect::Ready(ready));
                }
                PointStatusStored::IllFormed(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key_bytes)
                        .with_context(|| format!("table point info, status {status} {key:?}"))?;
                    let ready = PointRestore::IllFormed(info.id(), status);
                    result.push(PointRestoreSelect::Ready(ready));
                }
            }
        }

        Ok(result)
    }

    fn init_storage(&self, overlay_id: &OverlayId) -> Result<()> {
        if !self.has_compatible_data(overlay_id.as_bytes())? {
            match self.clean_points(&[u8::MAX; _])? {
                Some((Round(first), Round(last))) => {
                    tracing::info!("mempool DB cleaned on init, rounds: [{first}..{last}]");
                }
                None => {
                    tracing::info!("mempool DB was empty on init");
                }
            };
            self.wait_for_compact()?;
        }
        Ok(())
    }
}

#[cfg(any(feature = "test", test))]
impl MempoolStoreImpl for () {
    fn insert_point(&self, _: &Point, _: PointStatusStoredRef<'_>) -> Result<()> {
        Ok(())
    }

    fn set_status(&self, _: &PointKey, _: PointStatusStoredRef<'_>) -> Result<()> {
        Ok(())
    }

    fn get_point(&self, _: &PointKey) -> Result<Option<Point>> {
        anyhow::bail!("should not be used in tests")
    }

    fn multi_get_info(&self, _: &[PointKey]) -> Result<Vec<PointInfo>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_point_raw(&self, _: &PointKey) -> Result<Option<Bytes>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_info(&self, _: &PointKey) -> Result<Option<PointInfo>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_status(&self, _: &PointKey) -> Result<Option<PointStatusStored>> {
        anyhow::bail!("should not be used in tests")
    }

    fn last_round(&self) -> Result<Option<Round>> {
        anyhow::bail!("should not be used in tests")
    }

    fn reset_statuses(&self, _: &RangeInclusive<Round>) -> Result<()> {
        anyhow::bail!("should not be used in tests")
    }

    fn load_restore(&self, _: &RangeInclusive<Round>) -> Result<Vec<PointRestoreSelect>> {
        anyhow::bail!("should not be used in tests")
    }

    fn init_storage(&self, _: &OverlayId) -> Result<()> {
        Ok(())
    }
}
