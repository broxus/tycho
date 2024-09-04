use std::sync::Arc;

use ahash::HashMapExt;
use tycho_storage::{MempoolStorage, PointFlags};
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use crate::engine::outer_round::{Collator, Commit, Consensus, OuterRoundRecv};
use crate::engine::MempoolConfig;
use crate::models::{Digest, Point, PointInfo, Round};

#[derive(Clone)]
pub struct MempoolStore(Arc<dyn MempoolStoreImpl>);

trait MempoolStoreImpl: Send + Sync {
    fn insert_point(&self, point: &Point, flags: &PointFlags);

    fn set_flags(&self, round: Round, digest: &Digest, flags: &PointFlags);

    fn get_point(&self, round: Round, digest: &Digest) -> Option<Point>;

    fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo>;

    fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags;

    fn expand_anchor_history(&self, history: &[PointInfo]) -> Vec<Point>;
}

impl MempoolStore {
    pub fn new(
        inner: MempoolStorage,
        consensus_round: OuterRoundRecv<Consensus>,
        committed_round: OuterRoundRecv<Commit>,
        collator_round: OuterRoundRecv<Collator>,
    ) -> Self {
        Self::clean_task(
            inner.clone(),
            consensus_round,
            committed_round,
            collator_round,
        );

        Self(Arc::new(inner.clone()))
    }

    #[cfg(feature = "test")]
    pub fn no_read_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn insert_point(&self, point: &Point, flags: &PointFlags) {
        self.0.insert_point(point, flags);
    }

    pub fn set_flags(&self, round: Round, digest: &Digest, flags: &PointFlags) {
        self.0.set_flags(round, digest, flags);
    }

    pub fn get_point(&self, round: Round, digest: &Digest) -> Option<Point> {
        self.0.get_point(round, digest)
    }

    pub fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo> {
        self.0.get_info(round, digest)
    }

    pub fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags {
        self.0.get_flags(round, digest)
    }

    pub fn expand_anchor_history(&self, history: &[PointInfo]) -> Vec<Point> {
        self.0.expand_anchor_history(history)
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
        fill_prefix(least_to_keep, &mut up_to_exclusive);

        let db = inner.db.rocksdb();
        let points_cf = inner.db.points.cf();
        let info_cf = inner.db.points_info.cf();
        let flags_cf = inner.db.points_flags.cf();

        // in case we'll return to `db.delete_file_in_range_cf()`:
        // * at first delete flags, as their absense prevents incorrect access to points data
        // * then delete info, as it leaves points usable only for upload
        // * at last delete points safely

        let mut batch = MempoolStorage::write_batch();
        batch.delete_range_cf(&flags_cf, zero.as_slice(), up_to_exclusive.as_slice());
        batch.delete_range_cf(&info_cf, zero.as_slice(), up_to_exclusive.as_slice());
        batch.delete_range_cf(&points_cf, zero.as_slice(), up_to_exclusive.as_slice());

        db.write(batch).expect("batch delete range of mempool data");

        let none = Option::<[u8; MempoolStorage::KEY_LEN]>::None;
        db.compact_range_cf(&flags_cf, none, Some(up_to_exclusive.as_slice()));
        db.compact_range_cf(&info_cf, none, Some(up_to_exclusive.as_slice()));
        db.compact_range_cf(&points_cf, none, Some(up_to_exclusive.as_slice()));
    }
}

fn fill_key(round: Round, digest: &Digest, key: &mut [u8; MempoolStorage::KEY_LEN]) {
    fill_prefix(round, key);
    key[4..].copy_from_slice(&digest.inner()[..]);
}
fn fill_prefix(round: Round, key: &mut [u8; MempoolStorage::KEY_LEN]) {
    key[..4].copy_from_slice(&round.0.to_be_bytes()[..]);
}

impl MempoolStoreImpl for MempoolStorage {
    fn insert_point(&self, point: &Point, flags: &PointFlags) {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_insert_point_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(point.round(), point.digest(), &mut key);

        let info = bincode::serialize(&PointInfo::serializable_from(point))
            .expect("serialize db point info equivalent from point");
        let point = bincode::serialize(point).expect("serialize db point");
        let flags = PointFlags::encode(flags);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();
        let info_cf = self.db.points_info.cf();
        let flags_cf = self.db.points_flags.cf();

        // transaction not needed as there is no concurrent puts for the same key,
        // as they occur inside DAG futures whose uniqueness is protected by dash map;
        // in contrast, flags are written from random places, but only via `merge_cf()`
        let mut batch = MempoolStorage::write_batch();
        batch.put_cf(&points_cf, key.as_slice(), point.as_slice());
        batch.put_cf(&info_cf, key.as_slice(), info.as_slice());
        batch.merge_cf(&flags_cf, key.as_slice(), flags.as_slice());

        db.write(batch).expect("db batch insert point, info, flags");
    }

    fn set_flags(&self, round: Round, digest: &Digest, flags: &PointFlags) {
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_set_flags_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let db = self.db.rocksdb();
        let flags_cf = self.db.points_flags.cf();

        let result = db.merge_cf(&flags_cf, key.as_slice(), flags.encode().as_slice());
        result.expect("db merge point flag");
    }

    fn get_point(&self, round: Round, digest: &Digest) -> Option<Point> {
        metrics::counter!("tycho_mempool_store_get_point_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_point_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let table = &self.db.points;
        let found = table.get(key.as_slice()).expect("db get point");

        found.map(|a| bincode::deserialize(&a).expect("deserialize db point"))
    }

    fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo> {
        metrics::counter!("tycho_mempool_store_get_info_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_info_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let table = &self.db.points_info;
        let found = table.get(key.as_slice()).expect("db get point info");

        found.map(|a| bincode::deserialize(&a).expect("deserialize db point info"))
    }

    fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags {
        metrics::counter!("tycho_mempool_store_get_flags_count").increment(1);
        let _call_duration = HistogramGuard::begin("tycho_mempool_store_get_flags_time");
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let table = &self.db.points_flags;

        let found = table.get(key.as_slice()).expect("db get point flags");
        found.as_deref().map(PointFlags::decode).unwrap_or_default()
    }

    fn expand_anchor_history(&self, history: &[PointInfo]) -> Vec<Point> {
        let _call_duration =
            HistogramGuard::begin("tycho_mempool_store_expand_anchor_history_time");
        let mut buf = [0_u8; MempoolStorage::KEY_LEN];
        let mut keys = history
            .iter()
            .map(|info| {
                fill_key(info.round(), info.digest(), &mut buf);
                buf.to_vec()
            })
            .collect::<FastHashSet<Vec<u8>>>();
        buf.fill(0);

        let mut opt = MempoolStorage::read_options();

        let first = history.first().expect("anchor history mut not be empty");
        fill_prefix(first.round(), &mut buf);
        opt.set_iterate_lower_bound(buf);

        let last = history.last().expect("anchor history mut not be empty");
        fill_prefix(last.round().next(), &mut buf);
        opt.set_iterate_upper_bound(buf);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();

        let mut found = FastHashMap::with_capacity(history.len());
        let mut iter = db.raw_iterator_cf_opt(&points_cf, opt);
        iter.seek_to_first();

        while iter.valid() {
            if keys.remove(iter.key().expect("history iter invalidated on key")) {
                let bytes = iter.value().expect("history iter invalidated on value");
                let point = bincode::deserialize::<Point>(bytes).expect("deserialize point");
                let key = (point.round(), point.digest().clone());
                assert!(
                    found.insert(key, point).is_none(),
                    "iter read non-unique point"
                );
            }
            if keys.is_empty() {
                break;
            }
            iter.next();
        }
        iter.status().expect("anchor history iter is not ok");

        let mut result = Vec::with_capacity(history.len());
        for info in history {
            let key = (info.round(), info.digest().clone());
            let point = found
                .remove(&key)
                .expect("key was searched in db but was not found");
            result.push(point);
        }
        assert_eq!(result.len(), history.len(), "stored point key collision");

        result
    }
}

#[cfg(feature = "test")]
impl MempoolStoreImpl for () {
    fn insert_point(&self, _: &Point, _: &PointFlags) {}

    fn set_flags(&self, _: Round, _: &Digest, _: &PointFlags) {}

    fn get_point(&self, _: Round, _: &Digest) -> Option<Point> {
        panic!("should not be used in tests")
    }

    fn get_info(&self, _: Round, _: &Digest) -> Option<PointInfo> {
        panic!("should not be used in tests")
    }

    fn get_flags(&self, _: Round, _: &Digest) -> PointFlags {
        panic!("should not be used in tests")
    }

    fn expand_anchor_history(&self, _: &[PointInfo]) -> Vec<Point> {
        panic!("should not be used in tests")
    }
}
