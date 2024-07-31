use tycho_storage::{tables, MempoolStorage, PointFlags};

use crate::models::{Digest, Round};
use crate::outer_round::{Consensus, OuterRoundRecv};
use crate::{MempoolConfig, Point};

#[derive(Clone)]
pub struct MempoolStore {
    inner: MempoolStorage,
}

impl MempoolStore {
    pub fn new(inner: MempoolStorage, consensus_round: OuterRoundRecv<Consensus>) -> Self {
        Self::clean_task(inner.clone(), consensus_round);
        Self { inner }
    }

    fn point_key(round: Round, digest: &Digest, key: &mut [u8; tables::Points::KEY_LEN]) {
        key[..4].copy_from_slice(&round.0.to_be_bytes()[..]);
        key[4..].copy_from_slice(&digest.inner()[..]);
    }

    pub fn insert_point(&self, point: &Point) {
        let mut key = [0_u8; tables::Points::KEY_LEN];
        Self::point_key(point.body().location.round, point.digest(), &mut key);

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

    fn clean_task(inner: MempoolStorage, mut consensus_round: OuterRoundRecv<Consensus>) {
        // don't use `spawn_blocking()` here: it doesn't look like an IO intensive task
        tokio::spawn(async move {
            loop {
                // keep this the only `.await` to cancel properly on runtime shutdown
                let consensus_round = consensus_round.next().await.0;
                // delete stored data up to this value (exclusive)
                let least_to_keep = consensus_round
                    .saturating_div(MempoolConfig::TOMBSTONE_PERIOD_ROUNDS as u32)
                    .saturating_mul(MempoolConfig::TOMBSTONE_PERIOD_ROUNDS as u32)
                    .saturating_sub(MempoolConfig::PERSISTED_HISTORY_ROUNDS as u32);

                Self::clean(&inner, Round(least_to_keep));
            }
        });
    }

    fn clean(inner: &MempoolStorage, least_to_keep: Round) {
        let zero = [0_u8; tables::Points::KEY_LEN];
        let mut up_to_exclusive = [0_u8; tables::Points::KEY_LEN];
        up_to_exclusive[..4].copy_from_slice(&least_to_keep.0.to_be_bytes()[..]);

        // at first delete flags, as their removal prevents incorrect usage of points data
        let table = &inner.db.point_flags;
        table
            .db()
            .delete_range_cf(&table.cf(), zero.as_slice(), up_to_exclusive.as_slice())
            .expect("db delete range of point flags");

        let table = &inner.db.points;
        table
            .db()
            .delete_range_cf(&table.cf(), zero.as_slice(), up_to_exclusive.as_slice())
            .expect("db delete range of points");
    }
}
