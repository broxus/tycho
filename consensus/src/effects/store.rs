use std::cmp;
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::{Context, Result};
use bytes::Bytes;
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use tycho_network::OverlayId;
use tycho_storage::MempoolStorage;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{DBRawIterator, IteratorMode, ReadOptions, WaitForCompactOptions, WriteBatch};

use crate::effects::AltFormat;
use crate::engine::round_watch::{Commit, Consensus, RoundWatch, RoundWatcher, TopKnownAnchor};
use crate::engine::{CachedConfig, ConsensusConfigExt, Genesis};
use crate::models::{
    Digest, Point, PointInfo, PointRestore, PointStatus, PointStatusStored, PointStatusStoredRef,
    PointStatusValid, Round,
};

#[derive(Clone)]
pub struct MempoolAdapterStore {
    storage: MempoolStorage,
    commit_finished: RoundWatch<Commit>,
}

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

    fn set_committed(&self, anchor: &PointInfo, history: &[PointInfo]) -> Result<()>;

    fn get_point(&self, round: Round, digest: &Digest) -> Result<Option<Point>>;

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<Bytes>>;

    fn get_info(&self, round: Round, digest: &Digest) -> Result<Option<PointInfo>>;

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatusStored>>;

    fn expand_anchor_history(&self, history: &[PointInfo]) -> Result<Vec<Bytes>>;

    fn last_round(&self) -> Result<Round>;

    fn load_restore(&self, bottom: Round, top: Round) -> Result<Vec<PointRestore>>;

    fn init_storage(&self, overlay_id: &OverlayId) -> Result<()>;
}

impl MempoolAdapterStore {
    pub fn new(storage: MempoolStorage, commit_finished: RoundWatch<Commit>) -> Self {
        MempoolAdapterStore {
            storage,
            commit_finished,
        }
    }

    /// allows to remove no more needed data before sync and store of newly created dag part
    pub fn report_new_start(&self, next_expected_anchor: u32) {
        // set as committed because every anchor is repeatable by stored history (if it exists)
        self.commit_finished.set_max_raw(next_expected_anchor);
    }

    pub fn expand_anchor_history(&self, anchor: &PointInfo, history: &[PointInfo]) -> Vec<Bytes> {
        fn context(anchor: &PointInfo, history: &[PointInfo]) -> String {
            format!(
                "anchor {:?} history {} points rounds [{}..{}]",
                anchor.id().alt(),
                history.len(),
                history.first().map(|p| p.round().0).unwrap_or_default(),
                history.last().map(|p| p.round().0).unwrap_or_default()
            )
        }

        let payloads = if history.is_empty() {
            // history is checked at the end of DAG commit, leave traces in case its broken
            tracing::warn!(
                "anchor {:?} has empty history, it's ok only for anchor at DAG bottom round \
                 immediately after an unrecoverable gap",
                anchor.id().alt()
            );
            Vec::new()
        } else {
            self.storage
                .expand_anchor_history(history)
                .with_context(|| context(anchor, history))
                .expect("DB expand anchor history")
        };
        // may skip expand part, but never skip set committed - let it write what it should
        self.storage
            .set_committed(anchor, history)
            .with_context(|| context(anchor, history))
            .expect("DB set committed");
        // commit is finished when history payloads is read from DB and marked committed,
        // so that data may be removed consistently with any settings
        self.commit_finished.set_max(anchor.round());
        payloads
    }
}

impl MempoolStore {
    pub fn new(mempool_adapter_store: &MempoolAdapterStore) -> Self {
        Self(Arc::new(mempool_adapter_store.storage.clone()))
    }

    #[cfg(feature = "test")]
    pub fn no_read_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn insert_point(&self, point: &Point, status: PointStatusStoredRef<'_>) {
        self.0
            .insert_point(point, status)
            .with_context(|| format!("id {:?}", point.id().alt()))
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
            .expect("DB get point full")
    }

    pub fn get_point_raw(&self, round: Round, digest: &Digest) -> Option<Bytes> {
        self.0
            .get_point_raw(round, digest)
            .with_context(|| format!("round {} digest {}", round.0, digest.alt()))
            .expect("DB get point full")
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

    pub fn last_round(&self) -> Round {
        self.0.last_round().expect("DB load last round")
    }

    pub fn load_restore(&self, bottom: Round, top: Round) -> Vec<PointRestore> {
        self.0
            .load_restore(bottom, top)
            .with_context(|| format!("range [{}..{}]", bottom.0, top.0))
            .expect("DB load restore")
    }

    pub fn init_storage(&self, overlay_id: &OverlayId) {
        self.0
            .init_storage(overlay_id)
            .with_context(|| format!("new overlay id {}", overlay_id))
            .expect("DB drop all data");
    }
}

pub struct DbCleaner {
    storage: MempoolStorage,
    committed_round: RoundWatch<Commit>,
}

impl DbCleaner {
    pub fn new(adapter_store: &MempoolAdapterStore) -> Self {
        Self {
            storage: adapter_store.storage.clone(),
            committed_round: adapter_store.commit_finished.clone(),
        }
    }

    fn least_to_keep(consensus: Round, committed: Round, top_known_anchor: Round) -> Round {
        // If the node is not scheduled, then it's paused and does not receive broadcasts:
        // mempool receives fresher TKA (via validator sync)
        // while BcastFilter has outdated consensus round.
        // In such case top DAG round follows TKA while Engine round does not advance,
        // DAG eventually shrinks by advancing its bottom and reports it to MempoolAdapter.

        // So `committed` follows both consensus and TKA, and does not stall while Engine can.

        let least_to_keep = (committed - CachedConfig::get().consensus.reset_rounds()).min(
            // consensus for general work and sync:  collator may observe a gap if lags too far
            // TKA for deep sync: consensus round is stalled, history not needed for others
            consensus.max(top_known_anchor) - CachedConfig::get().consensus.max_total_rounds(),
        );
        let remainder =
            least_to_keep.0 % CachedConfig::get().node.clean_db_period_rounds.get() as u32;
        Genesis::id().round.max(least_to_keep - remainder)
    }

    pub fn new_task(
        &self,
        mut consensus_round: RoundWatcher<Consensus>,
        mut top_known_anchor: RoundWatcher<TopKnownAnchor>,
    ) -> JoinTask<()> {
        let storage = self.storage.clone();
        let mut committed_round = self.committed_round.receiver();

        JoinTask::new(async move {
            let mut consensus = consensus_round.get();
            let mut committed = committed_round.get();
            let mut top_known = top_known_anchor.get();
            let mut prev_least_to_keep = Self::least_to_keep(consensus, committed, top_known);
            loop {
                tokio::select! {
                    biased;
                    new_consensus = consensus_round.next() => consensus = new_consensus,
                    new_committed = committed_round.next() => committed = new_committed,
                    new_top_known = top_known_anchor.next() => top_known = new_top_known,
                }

                metrics::gauge!("tycho_mempool_consensus_current_round").set(consensus.0);
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_top_known")
                    .set(consensus - top_known);
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_committed")
                    .set(consensus - committed);
                metrics::gauge!("tycho_mempool_rounds_committed_ahead_top_known")
                    .set(committed - top_known);

                let new_least_to_keep = Self::least_to_keep(consensus, committed, top_known);
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_storage_round")
                    .set(consensus - new_least_to_keep);

                if new_least_to_keep > prev_least_to_keep {
                    let storage = storage.clone();
                    let task = tokio::task::spawn_blocking(move || {
                        let mut up_to_exclusive = [0_u8; MempoolStorage::KEY_LEN];
                        MempoolStorage::fill_prefix(new_least_to_keep.0, &mut up_to_exclusive);

                        match storage.clean(&up_to_exclusive) {
                            Ok(Some((first, last))) => {
                                const CLEANED: &str = "tycho_mempool_rounds_db_cleaned";
                                metrics::gauge!(CLEANED, "kind" => "lower").set(first);
                                metrics::gauge!(CLEANED, "kind" => "upper").set(last);
                                tracing::info!(
                                    "mempool DB cleaned for rounds [{first}..{last}] before {}",
                                    new_least_to_keep.0
                                );
                            }
                            Ok(None) => {
                                tracing::info!(
                                    "mempool DB is already clean before {}",
                                    new_least_to_keep.0
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    "delete range of mempool data before round {} failed: {e}",
                                    new_least_to_keep.0
                                );
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
        })
    }
}

impl MempoolStoreImpl for MempoolStorage {
    fn insert_point(&self, point: &Point, status: PointStatusStoredRef<'_>) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_insert_point_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(point.round().0, point.digest().inner(), &mut key);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();
        let info_cf = self.db.points_info.cf();
        let status_cf = self.db.points_status.cf();

        // transaction not needed as there is no concurrent puts for the same key,
        // as they occur inside DAG futures whose uniqueness is protected by dash map;
        // in contrast, status are written from random places, but only via `merge_cf()`
        let mut batch = WriteBatch::default();

        let mut buffer = Vec::<u8>::with_capacity(CachedConfig::get().point_max_bytes);
        point.write_to(&mut buffer);
        batch.put_cf(&points_cf, key.as_slice(), &buffer);

        buffer.clear();

        let value = PointInfo::serializable_from(point);
        value.write_to(&mut buffer);
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
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();

        Ok(db.merge_cf(&status_cf, key.as_slice(), status.encode())?)
    }

    fn set_committed(&self, anchor: &PointInfo, history: &[PointInfo]) -> Result<()> {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_committed_status_time");

        let mut buf = [0_u8; MempoolStorage::KEY_LEN];

        let db = self.db.rocksdb();
        let status_cf = self.db.points_status.cf();
        let mut batch = WriteBatch::default();

        let mut status = PointStatusValid::default();
        status.committed_at_round = Some(anchor.round().0);
        let status_encoded = status.encode();

        for info in history {
            MempoolStorage::fill_key(info.round().0, info.digest().inner(), &mut buf);
            batch.merge_cf(&status_cf, buf.as_slice(), &status_encoded);
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
            .map(|a| tl_proto::deserialize::<Point>(&a).context("deserialize db point"))
            .transpose()
    }

    fn get_point_raw(&self, round: Round, digest: &Digest) -> Result<Option<Bytes>> {
        metrics::counter!("tycho_mempool_store_get_point_raw_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_raw_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let points = &self.db.points;
        let point = points.get_owned(key.as_slice()).context("db get")?;
        Ok(point.map(Bytes::from_owner))
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
            .map(|a| tl_proto::deserialize::<PointInfo>(&a).context("deserialize point info"))
            .transpose()
    }

    fn get_status(&self, round: Round, digest: &Digest) -> Result<Option<PointStatusStored>> {
        metrics::counter!("tycho_mempool_store_get_status_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_status_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        MempoolStorage::fill_key(round.0, digest.inner(), &mut key);

        let table = &self.db.points_status;
        table
            .get(key.as_slice())
            .context("db get point status")?
            .as_deref()
            .map(PointStatusStored::decode)
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
                let point = Point::short_point_from_bytes(bytes).context("deserialize point")?;

                total_payload_items += point.payload().len();
                if found
                    .insert(key.to_vec().into_boxed_slice(), point)
                    .is_some()
                {
                    // we panic thus we don't care about performance
                    let full_point =
                        tl_proto::deserialize::<Point>(bytes).context("deserialize point")?;
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
            "{} history points were not found id db:\n{}",
            keys.len(),
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

    fn last_round(&self) -> Result<Round> {
        let (last_key, _) = self
            .db
            .points_status
            .iterator(IteratorMode::End)
            .next()
            .context("db is empty, at least last genesis must be provided")??;

        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&last_key[..4]);

        let round = u32::from_be_bytes(bytes);
        anyhow::ensure!(round > 0, "key with zero round");

        Ok(Round(round))
    }

    fn load_restore(&self, bottom: Round, top: Round) -> Result<Vec<PointRestore>> {
        fn opts(bottom: Round, top: Round) -> ReadOptions {
            let mut opts = ReadOptions::default();
            let mut buf = [0; MempoolStorage::KEY_LEN];
            MempoolStorage::fill_prefix(bottom.0, &mut buf);
            opts.set_iterate_lower_bound(buf);
            MempoolStorage::fill_prefix(top.next().0, &mut buf);
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
                    anyhow::bail!(
                        "iter did not seek, found key {}",
                        MempoolStorage::format_key(f_key),
                    )
                }
                cmp::Ordering::Equal => {
                    Ok(tl_proto::deserialize::<T>(value).context("deserialize")?)
                }
                cmp::Ordering::Greater => {
                    anyhow::bail!(
                        "no record found, next key {}",
                        MempoolStorage::format_key(f_key),
                    )
                }
            }
        }

        let mut result = Vec::new();

        let status_iter = (self.db.rocksdb()).iterator_cf_opt(
            &self.db.points_status.cf(),
            opts(bottom, top),
            IteratorMode::Start,
        );
        let mut info_iter =
            (self.db.rocksdb()).raw_iterator_cf_opt(&self.db.points_info.cf(), opts(bottom, top));
        let mut point_iter =
            (self.db.rocksdb()).raw_iterator_cf_opt(&self.db.points.cf(), opts(bottom, top));

        let mut round_buf = [0_u8; 4];
        let mut digest_buf = [0_u8; 32];
        for item in status_iter {
            let (key, status_bytes) = item.context("get point status")?;
            anyhow::ensure!(
                key.len() == MempoolStorage::KEY_LEN,
                "unexpected key len {}",
                key.len()
            );
            let status = PointStatusStored::decode(&status_bytes)?;

            match status {
                PointStatusStored::NotFound(status) => {
                    round_buf.copy_from_slice(&key[..4]);
                    digest_buf.copy_from_slice(&key[4..]);
                    let round = Round(u32::from_be_bytes(round_buf));
                    let digest = Digest::wrap(digest_buf);
                    result.push(PointRestore::NotFound(round, digest, status));
                }
                PointStatusStored::Valid(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key).with_context(|| {
                        format!(
                            "table point info, status {status}, key {}",
                            MempoolStorage::format_key(&key)
                        )
                    })?;
                    result.push(PointRestore::Valid(info, status));
                }
                PointStatusStored::Invalid(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key).with_context(|| {
                        format!(
                            "table point info, status {status}, key {}",
                            MempoolStorage::format_key(&key)
                        )
                    })?;
                    result.push(PointRestore::Invalid(info, status));
                }
                PointStatusStored::IllFormed(status) => {
                    let info = get_value::<PointInfo>(&mut info_iter, &key).with_context(|| {
                        format!(
                            "table point info, status {status}, key {}",
                            MempoolStorage::format_key(&key)
                        )
                    })?;
                    result.push(PointRestore::IllFormed(info.id(), status));
                }
                status @ PointStatusStored::Exists => {
                    let point = get_value::<Point>(&mut point_iter, &key).with_context(|| {
                        format!(
                            "table point, status {status}, key {}",
                            MempoolStorage::format_key(&key)
                        )
                    })?;
                    result.push(PointRestore::Exists(
                        PointInfo::from(&point),
                        point.prev_proof(),
                    ));
                }
            }
        }

        Ok(result)
    }

    fn init_storage(&self, overlay_id: &OverlayId) -> Result<()> {
        if !self.has_compatible_data(overlay_id.as_bytes())? {
            match self.clean(&[u8::MAX; Self::KEY_LEN])? {
                Some((first, last)) => {
                    tracing::info!("mempool DB cleaned on init, rounds: [{first}..{last}]");
                }
                None => {
                    tracing::info!("mempool DB was empty on init");
                }
            };
            // no reads/writes yet possible, and should finish prior other ops
            let mut opt = WaitForCompactOptions::default();
            opt.set_flush(true);
            self.db.rocksdb().wait_for_compact(&opt)?;
        }
        Ok(())
    }
}

#[cfg(feature = "test")]
impl MempoolStoreImpl for () {
    fn insert_point(&self, _: &Point, _: PointStatusStoredRef<'_>) -> Result<()> {
        Ok(())
    }

    fn set_status(&self, _: Round, _: &Digest, _: PointStatusStoredRef<'_>) -> Result<()> {
        Ok(())
    }

    fn set_committed(&self, _: &PointInfo, _: &[PointInfo]) -> Result<()> {
        Ok(())
    }

    fn get_point(&self, _: Round, _: &Digest) -> Result<Option<Point>> {
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

    fn expand_anchor_history(&self, _: &[PointInfo]) -> Result<Vec<Bytes>> {
        anyhow::bail!("should not be used in tests")
    }

    fn last_round(&self) -> Result<Round> {
        anyhow::bail!("should not be used in tests")
    }

    fn load_restore(&self, _: Round, _: Round) -> Result<Vec<PointRestore>> {
        anyhow::bail!("should not be used in tests")
    }

    fn init_storage(&self, _: &OverlayId) -> Result<()> {
        Ok(())
    }
}
