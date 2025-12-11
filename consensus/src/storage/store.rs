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

use super::{POINT_KEY_LEN, fill_point_key, fill_point_prefix, format_point_key};
use crate::effects::AltFormat;
use crate::models::{
    Digest, Point, PointInfo, PointRestore, PointRestoreSelect, PointStatusStored,
    PointStatusStoredRef, Round,
};
use crate::storage::{MempoolDb, StatusFlags};

#[derive(Clone)]
pub struct MempoolStore(Arc<dyn MempoolStoreImpl>);

trait MempoolStoreImpl: Send + Sync {
    fn insert_point(&self, point: &Point, status: PointStatusStoredRef<'_>) -> Result<()>;

    fn set_status(
        &self,
        round: Round,
        digest: &Digest,
        status: PointStatusStoredRef<'_>,
    ) -> Result<()>;

    fn get_point(&self, round: Round, digest: &Digest) -> Result<Option<Point>>;

    fn multi_get_info(&self, keys: &[(Round, Digest)]) -> Result<Vec<PointInfo>>;

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<Bytes>>;

    fn get_info(&self, round: Round, digest: &Digest) -> Result<Option<PointInfo>>;

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatusStored>>;

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

    pub fn set_status(&self, round: Round, digest: &Digest, status: PointStatusStoredRef<'_>) {
        self.0
            .set_status(round, digest, status)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB set point status");
    }

    pub fn get_point(&self, round: Round, digest: &Digest) -> Option<Point> {
        self.0
            .get_point(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point")
    }

    pub fn get_point_raw(&self, round: Round, digest: &Digest) -> Option<Bytes> {
        self.0
            .get_point_raw(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point raw")
    }

    pub fn multi_get_info(&self, keys: &[(Round, Digest)]) -> Vec<PointInfo> {
        self.0.multi_get_info(keys).expect("DB multi get points")
    }

    #[allow(dead_code, reason = "idiomatic getter may come in useful")]
    pub fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo> {
        self.0
            .get_info(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point info")
    }

    pub fn get_status(&self, round: Round, digest: &Digest) -> Option<PointStatusStored> {
        self.0
            .get_status(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
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
        let mut key = [0_u8; POINT_KEY_LEN];
        fill_point_key(
            point.info().round().0,
            point.info().digest().inner(),
            &mut key,
        );

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

        batch.put_cf(&points_cf, key.as_slice(), point.serialized());

        point.info().write_to(&mut buffer);
        batch.put_cf(&info_cf, key.as_slice(), &buffer);

        buffer.clear();

        status.write_to(&mut buffer);

        batch.merge_cf(&status_cf, key.as_slice(), &buffer);

        Ok(db.write(batch)?)
    }

    fn set_status(
        &self,
        round: Round,
        digest: &Digest,
        status: PointStatusStoredRef<'_>,
    ) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_status_time");
        let mut key = [0_u8; POINT_KEY_LEN];
        fill_point_key(round.0, digest.inner(), &mut key);

        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();

        Ok(db.merge_cf(&status_cf, key.as_slice(), status.encode())?)
    }

    fn get_point(&self, round: Round, digest: &Digest) -> Result<Option<Point>> {
        metrics::counter!("tycho_mempool_store_get_point_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_time");
        let mut key = [0_u8; POINT_KEY_LEN];
        fill_point_key(round.0, digest.inner(), &mut key);

        let points = &self.db.points;
        points
            .get(key.as_slice())
            .context("db get")?
            .map(|a| Point::from_bytes(a.to_vec()).context("deserialize db point"))
            .transpose()
    }

    fn multi_get_info(&self, keys: &[(Round, Digest)]) -> Result<Vec<PointInfo>> {
        let key_bytes = {
            let mut b_keys = Vec::with_capacity(keys.len());
            let mut buf = [0_u8; POINT_KEY_LEN];
            for (round, digest) in keys {
                fill_point_key(round.0, digest.inner(), &mut buf);
                b_keys.push(buf);
            }
            b_keys
        };

        anyhow::ensure!(key_bytes.is_sorted(), "key bytes must be sorted");

        let mut infos = Vec::with_capacity(keys.len());
        for (result_option_bytes, (round, digest)) in (self.db.points_info)
            .batched_multi_get(&key_bytes, true)
            .into_iter()
            .zip_eq(keys)
        {
            let option_bytes =
                result_option_bytes.with_context(|| format!("result for {round:?} {digest:?}"))?;
            let bytes = option_bytes.with_context(|| format!("not found {round:?} {digest:?}"))?;
            let point = tl_proto::deserialize::<PointInfo>(&bytes)
                .context("deserialize db point")
                .with_context(|| format!("deserialize point info {round:?} {digest:?}"))?;
            infos.push(point);
        }
        Ok(infos)
    }

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<Bytes>> {
        metrics::counter!("tycho_mempool_store_get_point_raw_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_raw_time");
        let mut key = [0_u8; POINT_KEY_LEN];
        fill_point_key(round.0, digest.inner(), &mut key);

        let points = &self.db.points;
        let point = points.get_owned(key.as_slice()).context("db get")?;
        Ok(point.map(Bytes::from_owner))
    }

    fn get_info(&self, round: Round, digest: &Digest) -> Result<Option<PointInfo>> {
        metrics::counter!("tycho_mempool_store_get_info_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_info_time");
        let mut key = [0_u8; POINT_KEY_LEN];
        fill_point_key(round.0, digest.inner(), &mut key);

        let table = &self.db.points_info;
        table
            .get(key.as_slice())
            .context("db get")?
            .map(|a| tl_proto::deserialize::<PointInfo>(&a).context("deserialize point info"))
            .transpose()
    }

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatusStored>> {
        metrics::counter!("tycho_mempool_store_get_status_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_status_time");
        let mut key = [0_u8; POINT_KEY_LEN];
        fill_point_key(round.0, digest.inner(), &mut key);

        let table = &self.db.points_status;
        table
            .get(key.as_slice())
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

        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&last_key[..4]);

        let round = u32::from_be_bytes(bytes);
        anyhow::ensure!(round > 0, "key with zero round");

        Ok(Some(Round(round)))
    }

    fn reset_statuses(&self, range: &RangeInclusive<Round>) -> Result<()> {
        let status_t = &self.db.tables().points_status;
        let db = self.db.rocksdb();

        let mut start = [0_u8; POINT_KEY_LEN];
        fill_point_prefix(range.start().0, &mut start);

        let mut end_excl = [0_u8; POINT_KEY_LEN];
        fill_point_prefix(range.end().next().0, &mut end_excl);

        let mut conf = status_t.new_read_config();
        conf.set_iterate_lower_bound(start);
        conf.set_iterate_upper_bound(end_excl);
        let iter = db.iterator_cf_opt(&status_t.cf(), conf, IteratorMode::Start);

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&status_t.cf(), start, end_excl);
        for kv in iter {
            let (k, v) = kv.context("status iter next")?;
            let new_v =
                match StatusFlags::try_from_stored(&v).with_context(|| format_point_key(&k))? {
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
            let mut buf = [0; POINT_KEY_LEN];
            fill_point_prefix(range.start().0, &mut buf);
            opts.set_iterate_lower_bound(buf);
            fill_point_prefix(range.end().next().0, &mut buf);
            opts.set_iterate_upper_bound(buf);
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
                    anyhow::bail!("iter did not seek, found key {}", format_point_key(f_key),)
                }
                cmp::Ordering::Equal => {
                    Ok(tl_proto::deserialize::<T>(value).context("deserialize")?)
                }
                cmp::Ordering::Greater => {
                    anyhow::bail!("no record found, next key {}", format_point_key(f_key),)
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

        let mut round_buf = [0_u8; 4];
        let mut digest_buf = [0_u8; 32];
        for item in status_iter {
            let (key, status_bytes) = item.context("get point status")?;
            anyhow::ensure!(
                key.len() == POINT_KEY_LEN,
                "unexpected key len {}",
                key.len()
            );
            let status = PointStatusStored::decode(&status_bytes)?;

            round_buf.copy_from_slice(&key[..4]);
            digest_buf.copy_from_slice(&key[4..]);
            let round = Round(u32::from_be_bytes(round_buf));
            let digest = Digest::wrap(&digest_buf);

            match status {
                PointStatusStored::Exists => {
                    result.push(PointRestoreSelect::NeedsVerify(round, *digest));
                }
                PointStatusStored::NotFound(status) => {
                    let ready = PointRestore::NotFound(round, *digest, status);
                    result.push(PointRestoreSelect::Ready(ready));
                }
                PointStatusStored::Validated(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key).with_context(|| {
                        format!("table point info, status {status} {round:?} {digest:?}")
                    })?;
                    let ready = PointRestore::Validated(info, status);
                    result.push(PointRestoreSelect::Ready(ready));
                }
                PointStatusStored::IllFormed(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key).with_context(|| {
                        format!("table point info, status {status} {round:?} {digest:?}")
                    })?;
                    let ready = PointRestore::IllFormed(info.id(), status);
                    result.push(PointRestoreSelect::Ready(ready));
                }
            }
        }

        Ok(result)
    }

    fn init_storage(&self, overlay_id: &OverlayId) -> Result<()> {
        if !self.has_compatible_data(overlay_id.as_bytes())? {
            match self.clean_points(&[u8::MAX; POINT_KEY_LEN])? {
                Some((first, last)) => {
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

    fn set_status(&self, _: Round, _: &Digest, _: PointStatusStoredRef<'_>) -> Result<()> {
        Ok(())
    }

    fn get_point(&self, _: Round, _: &Digest) -> Result<Option<Point>> {
        anyhow::bail!("should not be used in tests")
    }

    fn multi_get_info(&self, _: &[(Round, Digest)]) -> Result<Vec<PointInfo>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_point_raw(&self, _: Round, _: &Digest) -> Result<Option<Bytes>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_info(&self, _: Round, _: &Digest) -> Result<Option<PointInfo>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_status(&self, _: Round, _: &Digest) -> Result<Option<PointStatusStored>> {
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
