use tycho_storage::{tables, MempoolStorage, PointFlags};

use crate::models::{Digest, Round};
use crate::outer_round::{Collator, Commit, Consensus, OuterRoundRecv};
use crate::{MempoolConfig, Point};

#[derive(Clone)]
pub struct MempoolStore {
    inner: MempoolStorage,
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
        Self { inner }
    }

    fn point_key(round: Round, digest: &Digest, key: &mut [u8; tables::Points::KEY_LEN]) {
        key[..4].copy_from_slice(&round.0.to_be_bytes()[..]);
        key[4..].copy_from_slice(&digest.inner()[..]);
    }

    pub fn insert_point(&self, point: &Point) {
        let mut key = [0_u8; tables::Points::KEY_LEN];
        Self::point_key(point.body().round, point.digest(), &mut key);

        let data = bincode::serialize(point).expect("serialize db point");
        let result = self.inner.db.points.insert(key.as_slice(), data.as_slice());
        result.expect("db insert point");
    }

    pub fn get_point(&self, round: Round, digest: &Digest) -> Option<Point> {
        let mut key = [0_u8; tables::Points::KEY_LEN];
        Self::point_key(round, digest, &mut key);

        let table = &self.inner.db.points;
        let found = table.get(key.as_slice()).expect("db get point");
        found.map(|a| bincode::deserialize(&a).expect("deserialize db point"))
    }

    pub fn set_flags(&self, round: Round, digest: &Digest, flags: &PointFlags) {
        let mut key = [0_u8; tables::Points::KEY_LEN];
        Self::point_key(round, digest, &mut key);

        let table = &self.inner.db.point_flags;
        let result = table
            .db()
            .merge_cf(&table.cf(), key.as_slice(), flags.encode().as_slice());
        result.expect("db merge point flag");
    }

    pub fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags {
        let mut key = [0_u8; tables::Points::KEY_LEN];
        Self::point_key(round, digest, &mut key);

        let table = &self.inner.db.point_flags;
        let found = table.get(key.as_slice()).expect("db get point flags");
        found.as_deref().map(PointFlags::decode).unwrap_or_default()
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

        // don't use `spawn_blocking()` here: it doesn't look like an IO intensive task
        tokio::spawn(async move {
            let mut consensus = consensus_round.get();
            let mut committed = committed_round.get();
            let mut collated = collator_round.get();
            let mut prev_least_to_keep = least_to_keep(consensus, committed, collated);
            loop {
                // keep this the only `.await` to cancel properly on runtime shutdown
                tokio::select! {
                    new_consensus = consensus_round.next() => consensus = new_consensus,
                    new_committed = committed_round.next() => committed = new_committed,
                    new_collated = collator_round.next() => collated = new_collated,
                }
                // delete stored data up to this value (exclusive)
                let new_least_to_keep = least_to_keep(consensus, committed, collated);
                if new_least_to_keep > prev_least_to_keep {
                    Self::clean(&inner, new_least_to_keep);
                    prev_least_to_keep = new_least_to_keep;
                }
            }
        });
    }

    fn clean(inner: &MempoolStorage, least_to_keep: Round) {
        let zero = [0_u8; tables::Points::KEY_LEN];
        let mut up_to_exclusive = [0_u8; tables::Points::KEY_LEN];
        up_to_exclusive[..4].copy_from_slice(&least_to_keep.0.to_be_bytes()[..]);

        // at first delete flags, as their removal prevents incorrect usage of points data
        let table = &inner.db.point_flags;
        // manual flush: as table grows slowly it prevents shared WALs with blobs from deletion
        table.db().flush_cf(&table.cf()).expect("flush point flags");
        table
            .db()
            .delete_file_in_range_cf(&table.cf(), zero.as_slice(), up_to_exclusive.as_slice())
            .expect("db delete range of point flags");

        let table = &inner.db.points;
        table
            .db()
            .delete_file_in_range_cf(&table.cf(), zero.as_slice(), up_to_exclusive.as_slice())
            .expect("db delete range of points");
    }
}
