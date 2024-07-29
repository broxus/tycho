use tycho_storage::{tables, MempoolStorage, PointFlags};

use crate::models::{Digest, Round};
use crate::Point;

#[derive(Clone)]
pub struct MempoolStore {
    inner: MempoolStorage,
}

impl MempoolStore {
    pub fn new(inner: MempoolStorage) -> Self {
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
        let mut key = [0_u8; tables::PointFlags::KEY_LEN];
        Self::point_key(round, digest, &mut key);

        let table = &self.inner.db.point_flags;
        let result = table
            .db()
            .merge_cf(&table.cf(), key.as_slice(), flags.encode().as_slice());
        result.expect("db merge point flag");
    }

    pub fn get_flags(&self, round: Round, digest: &Digest) -> PointFlags {
        let mut key = [0_u8; tables::PointFlags::KEY_LEN];
        Self::point_key(round, digest, &mut key);

        let table = &self.inner.db.point_flags;
        let found = table.get(key.as_slice()).expect("db get point flags");
        found.as_deref().map(PointFlags::decode).unwrap_or_default()
    }
}
