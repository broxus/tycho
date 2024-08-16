use std::sync::Arc;

use tycho_storage::{MempoolStorage, PointFlags};

use crate::models::{Digest, PointInfo, Round};
use crate::outer_round::{Collator, Commit, Consensus, OuterRoundRecv};
use crate::{MempoolConfig, Point};

#[derive(Clone)]
pub struct MempoolStore(Arc<dyn MempoolStoreImpl>);

trait MempoolStoreImpl: Send + Sync {
    fn insert_point(&self, point: &Point, flags: Option<&PointFlags>);

    fn set_flags(&self, round: Round, digest: &Digest, flags: &PointFlags);

    fn get_point(&self, round: Round, digest: &Digest) -> Option<Point>;

    fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo>;

    fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags;
}

impl MempoolStore {
    pub(crate) fn new(
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
    pub fn new_stub() -> Self {
        Self(Arc::new(()))
    }

    pub fn insert_point(&self, point: &Point, flags: Option<&PointFlags>) {
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
                (MempoolConfig::GENESIS_ROUND.0)
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
                    new_consensus = consensus_round.next() => consensus = new_consensus,
                    new_committed = committed_round.next() => committed = new_committed,
                    new_collated = collator_round.next() => collated = new_collated,
                }
                let new_least_to_keep = least_to_keep(consensus, committed, collated);
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
        let zero = [0_u8; MempoolStorage::KEY_LEN];
        let mut up_to_exclusive = [0_u8; MempoolStorage::KEY_LEN];
        up_to_exclusive[..4].copy_from_slice(&least_to_keep.0.to_be_bytes()[..]);

        let db = inner.db.rocksdb();
        let points_cf = inner.db.points.cf();
        let info_cf = inner.db.points_info.cf();
        let flags_cf = inner.db.points_flags.cf();

        // in case we'll return to `db.delete_file_in_range_cf()`:
        // * at first delete flags, as their absense prevents incorrect access to points data
        // * then delete info, as it leaves points usable only for upload
        // * at last delete points safely

        let mut batch = MempoolStorage::new_batch();
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
    key[..4].copy_from_slice(&round.0.to_be_bytes()[..]);
    key[4..].copy_from_slice(&digest.inner()[..]);
}

impl MempoolStoreImpl for MempoolStorage {
    fn insert_point(&self, point: &Point, flags: Option<&PointFlags>) {
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(point.round(), point.digest(), &mut key);

        let info = bincode::serialize(&PointInfo::serializable_from(point))
            .expect("serialize db point info equivalent from point");
        let point = bincode::serialize(point).expect("serialize db point");
        let flags = flags.map(PointFlags::encode);

        let db = self.db.rocksdb();
        let points_cf = self.db.points.cf();
        let info_cf = self.db.points_info.cf();

        // transaction not needed as there is no concurrent puts for the same key,
        // as they occur inside DAG futures whose uniqueness is protected by dash map;
        // in contrast, flags are written from random places, but only via `merge_cf()`
        let mut batch = MempoolStorage::new_batch();
        batch.put_cf(&points_cf, key.as_slice(), point.as_slice());
        batch.put_cf(&info_cf, key.as_slice(), info.as_slice());

        if let Some(flags) = flags {
            let flags_cf = self.db.points_flags.cf();
            batch.merge_cf(&flags_cf, key.as_slice(), flags.as_slice());
        }

        db.write(batch).expect("db batch insert point, info, flags");
    }

    fn set_flags(&self, round: Round, digest: &Digest, flags: &PointFlags) {
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let db = self.db.rocksdb();
        let flags_cf = self.db.points_flags.cf();

        let result = db.merge_cf(&flags_cf, key.as_slice(), flags.encode().as_slice());
        result.expect("db merge point flag");
    }

    fn get_point(&self, round: Round, digest: &Digest) -> Option<Point> {
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let table = &self.db.points;
        let found = table.get(key.as_slice()).expect("db get point");

        found.map(|a| bincode::deserialize(&a).expect("deserialize db point"))
    }

    fn get_info(&self, round: Round, digest: &Digest) -> Option<PointInfo> {
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let table = &self.db.points_info;
        let found = table.get(key.as_slice()).expect("db get point info");

        found.map(|a| bincode::deserialize(&a).expect("deserialize db point info"))
    }

    fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags {
        let mut key = [0_u8; MempoolStorage::KEY_LEN];
        fill_key(round, digest, &mut key);

        let table = &self.db.points_flags;

        let found = table.get(key.as_slice()).expect("db get point flags");
        found.as_deref().map(PointFlags::decode).unwrap_or_default()
    }
}

#[cfg(feature = "test")]
impl MempoolStoreImpl for () {
    fn insert_point(&self, _: &Point, _: Option<&PointFlags>) {}

    fn set_flags(&self, _: Round, _: &Digest, _: &PointFlags) {}

    fn get_point(&self, _: Round, _: &Digest) -> Option<Point> {
        None
    }

    fn get_info(&self, _: Round, _: &Digest) -> Option<PointInfo> {
        None
    }

    fn get_flags(&self, _: Round, _: &Digest) -> PointFlags {
        PointFlags::default()
    }
}
