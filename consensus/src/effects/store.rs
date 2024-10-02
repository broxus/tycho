use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use tycho_storage::point_status::PointStatus;
use tycho_storage::MempoolStorage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{DBPinnableSlice, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::effects::AltFormat;
use crate::engine::round_watch::{Commit, Consensus, RoundWatch, RoundWatcher, TopKnownAnchor};
use crate::engine::MempoolConfig;
use crate::models::{Digest, Point, PointInfo, Round, ShortPoint};

#[derive(Clone)]
pub struct MempoolAdapterStore {
    pub inner: MempoolStorage,
    commit_finished: RoundWatch<Commit>,
}

#[derive(Clone)]
pub struct MempoolStore(Arc<dyn MempoolStoreImpl>);

trait MempoolStoreImpl: Send + Sync {
    fn insert_point(&self, point: &Point, status: &PointStatus) -> Result<()>;

    fn set_status(&self, round: Round, digest: &Digest, status: &PointStatus) -> Result<()>;

    fn set_committed(&self, anchor: &PointInfo, history: &[PointInfo]) -> Result<()>;

    fn get_point(&self, round: Round, digest: &Digest) -> Result<Option<Point>>;

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<DBPinnableSlice>>;

    fn get_info(&self, round: Round, digest: &Digest) -> Result<Option<PointInfo>>;

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatus>>;

    fn expand_anchor_history(&self, history: &[PointInfo]) -> Result<Vec<Bytes>>;

    fn latest_round(&self) -> Result<Round>;

    fn load_rounds(&self, first: Round, last: Round) -> Result<Vec<(PointInfo, PointStatus)>>;

    fn drop_all_data_before_start(&self) -> Result<()>;
}

impl MempoolAdapterStore {
    pub fn new(inner: MempoolStorage, commit_finished: RoundWatch<Commit>) -> Self {
        MempoolAdapterStore {
            inner,
            commit_finished,
        }
    }

    pub fn set_committed(&self, anchor: &PointInfo, history: &[PointInfo]) {
        self.inner
            .set_committed(anchor, history)
            .with_context(|| {
                format!(
                    "anchor {:?} history points {} rounds [{}..{}]",
                    anchor.id().alt(),
                    history.first().map(|p| p.round().0).unwrap_or_default(),
                    history.last().map(|p| p.round().0).unwrap_or_default(),
                    history.len()
                )
            })
            .expect("DB set committed");
        // commit is finished when history payloads is read from DB and marked committed,
        // so that data may be removed consistently with any settings
        self.commit_finished.set_max(anchor.round());
    }

    pub fn expand_anchor_history(&self, history: &[PointInfo]) -> Vec<Bytes> {
        self.inner
            .expand_anchor_history(history)
            .with_context(|| {
                format!(
                    "history points {} rounds [{}..{}]",
                    history.first().map(|p| p.round().0).unwrap_or_default(),
                    history.last().map(|p| p.round().0).unwrap_or_default(),
                    history.len()
                )
            })
            .expect("DB expand anchor history")
    }
}

impl MempoolStore {
    pub fn new(
        mempool_adapter_store: &MempoolAdapterStore,
        consensus_round: RoundWatcher<Consensus>,
        top_known_anchor: RoundWatcher<TopKnownAnchor>,
    ) -> Self {
        Self::clean_task(
            mempool_adapter_store.inner.clone(),
            consensus_round,
            mempool_adapter_store.commit_finished.receiver(),
            top_known_anchor,
        );

        Self(Arc::new(mempool_adapter_store.inner.clone()))
    }

    #[cfg(feature = "test")]
    pub fn no_read_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn insert_point(&self, point: &Point, status: &PointStatus) {
        self.0
            .insert_point(point, status)
            .with_context(|| format!("id {:?} {status:?}", point.id().alt()))
            .expect("DB insert point full");
    }

    pub fn set_status(&self, round: Round, digest: &Digest, status: &PointStatus) {
        self.0
            .set_status(round, digest, status)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB set point status");
    }

    pub fn get_point(&self, round: Round, digest: &Digest) -> Option<Point> {
        self.0
            .get_point(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point full")
    }

    pub fn get_point_raw(&self, round: Round, digest: Digest) -> Option<DBPinnableSlice> {
        self.0
            .get_point_raw(round, &digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point full")
    }

    pub fn get_info(&self, round: Round, digest: Digest) -> Option<PointInfo> {
        self.0
            .get_info(round, &digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point info")
    }

    pub fn get_status(&self, round: Round, digest: &Digest) -> Option<PointStatus> {
        self.0
            .get_status(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point status")
    }

    pub fn latest_round(&self) -> Round {
        self.0.latest_round().expect("DB latest round")
    }

    pub fn load_rounds(&self, first: Round, last: Round) -> Vec<(PointInfo, PointStatus)> {
        self.0
            .load_rounds(first, last)
            .with_context(|| format!("first {} last {}", first.0, last.0))
            .expect("DB load rounds")
    }

    pub fn drop_all_data_before_start(&self) {
        self.0
            .drop_all_data_before_start()
            .expect("DB drop all data");
    }

    fn clean_task(
        inner: MempoolStorage,
        mut consensus_round: RoundWatcher<Consensus>,
        mut committed_round: RoundWatcher<Commit>,
        mut top_known_anchor: RoundWatcher<TopKnownAnchor>,
    ) {
        fn least_to_keep(consensus: Round, committed: Round) -> Round {
            Round(
                // do not clean history that it can be requested by other peers
                Consensus::history_bottom(consensus)
                    // do not clean history until commit is finished (to reproduce after restart)
                    .min(Commit::stored_history_bottom(committed))
                    .0 // clean history no matter if top known anchor is far behind
                    .saturating_div(MempoolConfig::CLEAN_ROCKS_PERIOD as u32)
                    .saturating_mul(MempoolConfig::CLEAN_ROCKS_PERIOD as u32),
            )
        }

        tokio::spawn(async move {
            let mut consensus = consensus_round.get();
            let mut committed = committed_round.get();
            let mut top_known = top_known_anchor.get(); // for metrics only
            let mut prev_least_to_keep = least_to_keep(consensus, committed);
            loop {
                tokio::select! {
                    new_consensus = consensus_round.next() => {
                        consensus = new_consensus;
                        metrics::gauge!("tycho_mempool_consensus_current_round")
                            .set(consensus.0);
                    },
                    new_committed = committed_round.next() => committed = new_committed,
                    new_top_known = top_known_anchor.next() => top_known = new_top_known,
                }

                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_top_known")
                    .set((consensus.0 as f64) - (top_known.0 as f64));
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_committed")
                    .set((consensus.0 as f64) - (committed.0 as f64));
                metrics::gauge!("tycho_mempool_rounds_committed_ahead_top_known")
                    .set((committed.0 as f64) - (top_known.0 as f64));

                let new_least_to_keep = least_to_keep(consensus, committed);
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_storage_round")
                    .set((consensus.0 as f64) - (new_least_to_keep.0 as f64));

                if new_least_to_keep > prev_least_to_keep {
                    let inner = inner.clone();
                    let task = tokio::task::spawn_blocking(move || {
                        let mut up_to_exclusive = [0_u8; MempoolStorage::KEY_LEN];
                        MempoolStorage::fill_prefix(new_least_to_keep.0, &mut up_to_exclusive);

                        match inner.clean(&up_to_exclusive) {
                            Ok(()) => {}
                            Err(e) => {
                                tracing::error!("delete range of mempool data failed: {e}");
                            }
                        }
                    });
                    match task.await {
                        Ok(()) => prev_least_to_keep = new_least_to_keep,
                        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                        Err(cancelled) => {
                            tracing::error!("clean mempool storage task: {cancelled:?}");
                            // keep prev value unchanged to retry
                        }
                    }
                }
            }
        });
    }
}

impl MempoolStoreImpl for MempoolStorage {
    fn insert_point(&self, point: &Point, status: &PointStatus) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_insert_point_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(point.round().0, point.digest().inner(), &mut key);

        let value = PointInfo::serializable_from(point);
        let mut info = BytesMut::with_capacity(value.max_size_hint());
        value.write_to(&mut info);

        // new
        let mut point_bytes = BytesMut::with_capacity(point.max_size_hint());
        point.write_to(&mut point_bytes);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();
        let info_cf = self.db.points_info.cf();
        let status_cf = self.db.points_status.cf();

        // transaction not needed as there is no concurrent puts for the same key,
        // as they occur inside DAG futures whose uniqueness is protected by dash map;
        // in contrast, status are written from random places, but only via `merge_cf()`
        let mut batch = WriteBatch::default();
        batch.put_cf(&points_cf, key.as_slice(), point_bytes.freeze());
        batch.put_cf(&info_cf, key.as_slice(), info.freeze());
        batch.merge_cf(&status_cf, key.as_slice(), status.encode().as_slice());

        Ok(db.write(batch)?)
    }

    fn set_status(&self, round: Round, digest: &Digest, status: &PointStatus) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_status_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();

        Ok(db.merge_cf(&status_cf, key.as_slice(), status.encode().as_slice())?)
    }

    fn set_committed(&self, anchor: &PointInfo, history: &[PointInfo]) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_committed_status_time");

        let mut buf = [0_u8; MempoolStorage::KEY_LEN];

        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();
        let mut batch = WriteBatch::default();

        let status_encoded = PointStatus {
            committed_at_round: Some(anchor.round().0),
            ..Default::default()
        }
        .encode();

        for info in history {
            MempoolStorage::fill_key(info.round().0, info.digest().inner(), &mut buf);
            batch.merge_cf(&status_cf, buf.as_slice(), status_encoded.as_slice());
        }

        Ok(db.write(batch)?)
    }

    fn get_point(&self, round: Round, digest: &Digest) -> Result<Option<Point>> {
        metrics::counter!("tycho_mempool_store_get_point_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let points = &self.db.points;
        points
            .get(key.as_slice())
            .context("db get")?
            .map(|a| <Point>::read_from(&a, &mut 0).context("deserialize db point"))
            .transpose()
    }

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<DBPinnableSlice>> {
        metrics::counter!("tycho_mempool_store_get_point_raw_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_raw_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let points = &self.db.points;
        let point = points.get(key.as_slice()).context("db get")?;
        Ok(point)
    }

    fn get_info(&self, round: Round, digest: &Digest) -> Result<Option<PointInfo>> {
        metrics::counter!("tycho_mempool_store_get_info_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_info_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let table = &self.db.points_info;
        table
            .get(key.as_slice())
            .context("db get")?
            .map(|a| <PointInfo>::read_from(&a, &mut 0).context("deserialize point info"))
            .transpose()
    }

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatus>> {
        metrics::counter!("tycho_mempool_store_get_status_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_status_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let table = &self.db.points_status;
        table
            .get(key.as_slice())
            .context("db get point status")?
            .as_deref()
            .map(PointStatus::decode)
            .transpose()
    }

    fn expand_anchor_history(&self, history: &[PointInfo]) -> Result<Vec<Bytes>> {
        let _call_duration =
            HistogramGuard::begin("tycho_mempool_store_expand_anchor_history_time");
        let mut buf = [0_u8; MempoolStorage::KEY_LEN];
        let mut keys = history
            .iter()
            .map(|info| {
                MempoolStorage::fill_key(info.round().0, info.digest().inner(), &mut buf);
                buf.to_vec().into_boxed_slice()
            })
            .collect::<FastHashSet<_>>();
        buf.fill(0);

        let mut opt = ReadOptions::default();

        let first = history
            .first()
            .context("anchor history must not be empty")?;
        MempoolStorage::fill_prefix(first.round().0, &mut buf);
        opt.set_iterate_lower_bound(buf);

        let last = history.last().context("anchor history must not be empty")?;
        MempoolStorage::fill_prefix(last.round().next().0, &mut buf);
        opt.set_iterate_upper_bound(buf);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();

        let mut found = FastHashMap::with_capacity(history.len());
        let mut iter = db.raw_iterator_cf_opt(&points_cf, opt);
        iter.seek_to_first();

        let mut total_payload_items = 0;
        while iter.valid() {
            let key = iter.key().context("history iter invalidated on key")?;
            if keys.remove(key) {
                let bytes = iter.value().context("history iter invalidated on value")?;
                let point = <ShortPoint>::read_from(bytes, &mut 0).context("deserialize point")?;

                total_payload_items += point.payload().len();
                if found
                    .insert(key.to_vec().into_boxed_slice(), point)
                    .is_some()
                {
                    // we panic thus we don't care about performance
                    let full_point =
                        <Point>::read_from(bytes, &mut 0).context("deserialize point")?;
                    panic!("iter read non-unique point {:?}", full_point.id())
                }
            }
            if keys.is_empty() {
                break;
            }
            iter.next();
        }
        iter.status().context("anchor history iter is not ok")?;
        drop(iter);

        anyhow::ensure!(
            keys.is_empty(),
            "some history points were not found id db:\n {}",
            keys.iter()
                .map(|key| MempoolStorage::format_key(key))
                .join(",\n")
        );
        anyhow::ensure!(found.len() == history.len(), "stored point key collision");

        let mut result = Vec::with_capacity(total_payload_items);
        for info in history {
            MempoolStorage::fill_key(info.round().0, info.digest().inner(), &mut buf);
            let point = found
                .remove(buf.as_slice())
                .with_context(|| MempoolStorage::format_key(&buf))
                .context("key was searched in db but was not found")?;
            result.extend_from_slice(point.payload());
        }

        Ok(result)
    }

    fn latest_round(&self) -> Result<Round> {
        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();

        let mut iter = db.raw_iterator_cf(&status_cf);
        iter.seek_to_last();

        let last_key = iter
            .key()
            .context("db is empty, at least last genesis must be supplied")?;
        iter.status().context("iter is not ok")?;

        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&last_key[..4]);

        let round = u32::from_be_bytes(bytes);
        anyhow::ensure!(round > 0, "key with zero round");

        Ok(Round(round))
    }

    fn load_rounds(&self, first: Round, last: Round) -> Result<Vec<(PointInfo, PointStatus)>> {
        fn opts(first: Round, last: Round, buf: &mut [u8; MempoolStorage::KEY_LEN]) -> ReadOptions {
            let mut opt = ReadOptions::default();
            MempoolStorage::fill_prefix(first.0, buf);
            opt.set_iterate_lower_bound(&buf[..]);
            MempoolStorage::fill_prefix(last.next().0, buf);
            opt.set_iterate_upper_bound(&buf[..]);
            opt
        }

        let mut buf = [0_u8; MempoolStorage::KEY_LEN];
        let db = self.db.rocksdb();
        let info_cf = self.db.points_info.cf();
        let status_cf = self.db.points_status.cf();

        let opt = opts(first, last, &mut buf);
        let mut iter = db.raw_iterator_cf_opt(&status_cf, opt);
        let mut statuses = FastHashMap::default();
        iter.seek_to_first();

        while iter.valid() {
            let Some((key, bytes)) = iter.item() else {
                break;
            };
            statuses.insert(
                Vec::from(key).into_boxed_slice(),
                PointStatus::decode(bytes).with_context(|| MempoolStorage::format_key(key))?,
            );
            iter.next();
        }
        iter.status().context("point status iter is not ok")?;
        drop(iter);

        let opt = opts(first, last, &mut buf);
        let mut iter = db.raw_iterator_cf_opt(&info_cf, opt);
        iter.seek_to_first();

        let mut result = Vec::with_capacity(statuses.len());
        while iter.valid() {
            let Some((key, info)) = iter.item() else {
                break;
            };
            let info = <PointInfo>::read_from(info, &mut 0).context("db deserialize point info")?;
            if let Some(status) = statuses.remove(key) {
                result.push((info, status));
            } else {
                anyhow::bail!("point stored without status: {:?}", info.id().alt())
            }
            iter.next();
        }
        iter.status().context("point info iter is not ok")?;
        drop(iter);

        if !statuses.is_empty() {
            let keys = statuses
                .keys()
                .map(|key| MempoolStorage::format_key(key))
                .collect::<Vec<_>>();
            anyhow::bail!("point info stored without point status: {keys:?}");
        }

        Ok(result)
    }

    fn drop_all_data_before_start(&self) -> Result<()> {
        self.clean(&[u8::MAX; Self::KEY_LEN])?;
        // no reads/writes yet possible, and should finish prior other ops
        let mut opt = WaitForCompactOptions::default();
        opt.set_flush(true);
        self.db.rocksdb().wait_for_compact(&opt)?;
        Ok(())
    }
}

#[cfg(feature = "test")]
impl MempoolStoreImpl for () {
    fn insert_point(&self, _: &Point, _: &PointStatus) -> Result<()> {
        Ok(())
    }

    fn set_status(&self, _: Round, _: &Digest, _: &PointStatus) -> Result<()> {
        Ok(())
    }

    fn set_committed(&self, _: &PointInfo, _: &[PointInfo]) -> Result<()> {
        Ok(())
    }

    fn get_point(&self, _: Round, _: &Digest) -> Result<Option<Point>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<DBPinnableSlice>> {
        Ok(None)
    }

    fn get_info(&self, _: Round, _: &Digest) -> Result<Option<PointInfo>> {
        anyhow::bail!("should not be used in tests")
    }

    fn get_status(&self, _: Round, _: &Digest) -> Result<Option<PointStatus>> {
        anyhow::bail!("should not be used in tests")
    }

    fn expand_anchor_history(&self, _: &[PointInfo]) -> Result<Vec<Bytes>> {
        anyhow::bail!("should not be used in tests")
    }

    fn latest_round(&self) -> Result<Round> {
        anyhow::bail!("should not be used in tests")
    }

    fn load_rounds(&self, _: Round, _: Round) -> Result<Vec<(PointInfo, PointStatus)>> {
        anyhow::bail!("should not be used in tests")
    }

    fn drop_all_data_before_start(&self) -> Result<()> {
        Ok(())
    }
}
