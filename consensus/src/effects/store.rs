use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result};
use bytes::Bytes;
use itertools::Itertools;
use tycho_storage::point_status::PointStatus;
use tycho_storage::MempoolStorage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{ReadOptions, WriteBatch};

use crate::effects::AltFormat;
use crate::engine::outer_round::{Collator, Commit, Consensus, OuterRound, OuterRoundRecv};
use crate::engine::MempoolConfig;
use crate::models::{Digest, Point, PointInfo, Round};

#[derive(Clone)]
pub struct MempoolAdapterStore {
    pub inner: MempoolStorage,
    commit_finished: OuterRound<Commit>,
}

#[derive(Clone)]
pub struct MempoolStore(Arc<dyn MempoolStoreImpl>);

trait MempoolStoreImpl: Send + Sync {
    fn insert_point(&self, point: &Point, status: &PointStatus) -> Result<()>;

    fn set_status(&self, round: Round, digest: &Digest, status: &PointStatus) -> Result<()>;

    fn set_committed(&self, anchor: &PointInfo, history: &[PointInfo]) -> Result<()>;

    fn get_point(&self, round: Round, digest: &Digest) -> Result<Option<Point>>;

    fn get_info(&self, round: Round, digest: &Digest) -> Result<Option<PointInfo>>;

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatus>>;

    fn expand_anchor_history(&self, history: &[PointInfo]) -> Result<Vec<Bytes>>;

    fn latest_round(&self) -> Result<Round>;

    fn load_rounds(&self, first: Round, last: Round) -> Result<Vec<(PointInfo, PointStatus)>>;
}

impl MempoolAdapterStore {
    pub fn new(inner: MempoolStorage, commit_finished: OuterRound<Commit>) -> Self {
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
        consensus_round: OuterRoundRecv<Consensus>,
        collator_round: OuterRoundRecv<Collator>,
    ) -> Self {
        Self::clean_task(
            mempool_adapter_store.inner.clone(),
            consensus_round,
            mempool_adapter_store.commit_finished.receiver(),
            collator_round,
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

    pub fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo> {
        self.0
            .get_info(round, digest)
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

    fn clean_task(
        inner: MempoolStorage,
        mut consensus_round: OuterRoundRecv<Consensus>,
        mut committed_round: OuterRoundRecv<Commit>,
        mut collator_round: OuterRoundRecv<Collator>,
    ) {
        fn least_to_keep(consensus: Round, committed: Round, collated: Round) -> Round {
            // enough to handle acceptable collator lag
            let behind_consensus = (consensus.0)
                // before silent mode
                .saturating_sub(MempoolConfig::MAX_ANCHOR_DISTANCE as u32)
                // before top known block in silent mode
                .saturating_sub(MempoolConfig::ACCEPTABLE_COLLATOR_LAG as u32)
                // oldest data to collate as unique
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as u32) // data to collate
                .saturating_sub(MempoolConfig::DEDUPLICATE_ROUNDS as u32); // as unique

            // oldest data to collate that is validatable and unique
            let behind_committed = (committed.0)
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as u32) // data to collate
                .saturating_sub(
                    (MempoolConfig::MAX_ANCHOR_DISTANCE as u32) // validatable by other peers
                        .max(MempoolConfig::DEDUPLICATE_ROUNDS as u32), // unique
                );
            // oldest unique data to collate (including latest collated round)
            let behind_collated = (collated.0)
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as u32)
                .saturating_sub(MempoolConfig::DEDUPLICATE_ROUNDS as u32);
            Round(
                (MempoolConfig::genesis_round().0)
                    .max(behind_consensus)
                    .max(behind_committed.min(behind_collated))
                    .saturating_div(MempoolConfig::CLEAN_ROCKS_PERIOD as u32)
                    .saturating_mul(MempoolConfig::CLEAN_ROCKS_PERIOD as u32),
            )
        }

        tokio::spawn(async move {
            let mut consensus = consensus_round.get();
            let mut committed = committed_round.get();
            let mut collated = collator_round.get();
            let mut prev_least_to_keep = least_to_keep(consensus, committed, collated);
            loop {
                tokio::select! {
                    new_consensus = consensus_round.next() => {
                        consensus = new_consensus;
                        metrics::gauge!("tycho_mempool_consensus_current_round")
                            .set(consensus.0);
                    },
                    new_committed = committed_round.next() => committed = new_committed,
                    new_collated = collator_round.next() => collated = new_collated,
                }

                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_collated")
                    .set((consensus.0 as f64) - (collated.0 as f64));
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_committed")
                    .set((consensus.0 as f64) - (committed.0 as f64));
                metrics::gauge!("tycho_mempool_rounds_committed_ahead_collated")
                    .set((committed.0 as f64) - (collated.0 as f64));

                let new_least_to_keep = least_to_keep(consensus, committed, collated);
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_storage_round")
                    .set((consensus.0 as f64) - (new_least_to_keep.0 as f64));

                if new_least_to_keep > prev_least_to_keep {
                    let inner = inner.clone();
                    let task =
                        tokio::task::spawn_blocking(move || Self::clean(&inner, new_least_to_keep));
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

    /// delete all stored data up to provided value (exclusive)
    fn clean(inner: &MempoolStorage, least_to_keep: Round) {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_clean_time");
        let zero = [0_u8; MempoolStorage::KEY_LEN];
        let mut up_to_exclusive = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_prefix(least_to_keep.0, &mut up_to_exclusive);

        let db = inner.db.rocksdb();
        let points_cf = inner.db.points.cf();
        let info_cf = inner.db.points_info.cf();
        let status_cf = inner.db.points_status.cf();

        // in case we'll return to `db.delete_file_in_range_cf()`:
        // * at first delete status, as their absense prevents incorrect access to points data
        // * then delete info, as it leaves points usable only for upload
        // * at last delete points safely

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&status_cf, zero.as_slice(), up_to_exclusive.as_slice());
        batch.delete_range_cf(&info_cf, zero.as_slice(), up_to_exclusive.as_slice());
        batch.delete_range_cf(&points_cf, zero.as_slice(), up_to_exclusive.as_slice());

        match db.write(batch) {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("delete range of mempool data failed: {e}");
                return;
            }
        }

        let none = None::<[u8; MempoolStorage::KEY_LEN]>;
        db.compact_range_cf(&status_cf, none, Some(up_to_exclusive.as_slice()));
        db.compact_range_cf(&info_cf, none, Some(up_to_exclusive.as_slice()));
        db.compact_range_cf(&points_cf, none, Some(up_to_exclusive.as_slice()));
    }
}

impl MempoolStoreImpl for MempoolStorage {
    fn insert_point(&self, point: &Point, status: &PointStatus) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_insert_point_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(point.round().0, point.digest().inner(), &mut key);

        let info = bincode::serialize(&PointInfo::serializable_from(point))
            .context("serialize db point info equivalent from point")?;
        let point = bincode::serialize(point)?;

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();
        let info_cf = self.db.points_info.cf();
        let status_cf = self.db.points_status.cf();

        // transaction not needed as there is no concurrent puts for the same key,
        // as they occur inside DAG futures whose uniqueness is protected by dash map;
        // in contrast, status are written from random places, but only via `merge_cf()`
        let mut batch = WriteBatch::default();
        batch.put_cf(&points_cf, key.as_slice(), point.as_slice());
        batch.put_cf(&info_cf, key.as_slice(), info.as_slice());
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
            .map(|a| bincode::deserialize(&a).context("deserialize db point"))
            .transpose()
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
            .map(|a| bincode::deserialize(&a).context("deserialize point info"))
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
                let point = bincode::deserialize::<Point>(bytes).context("deserialize point")?;

                total_payload_items += point.payload().len();
                if let Some(prev_duplicate) = found.insert(key.to_vec().into_boxed_slice(), point) {
                    panic!("iter read non-unique point {:?}", prev_duplicate.id())
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
            let info =
                bincode::deserialize::<PointInfo>(info).context("db deserialize point info")?;
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
}
