use everscale_crypto::ed25519::{KeyPair, SecretKey};

use crate::engine::MempoolConfig;
use crate::models::{Link, Point, PointBody, PointId, UnixTime};

const GENESIS_SECRET_KEY_BYTES: [u8; 32] = [0xAE; 32];
const GENESIS_MILLIS: u64 = 1713225727398;

// TODO this must be passed via config file
pub fn genesis_point_id() -> PointId {
    genesis().id()
}

pub fn genesis() -> Point {
    let genesis_keys = KeyPair::from(&SecretKey::from_bytes(GENESIS_SECRET_KEY_BYTES));

    Point::new(&genesis_keys, PointBody {
        author: genesis_keys.public_key.into(),
        round: MempoolConfig::GENESIS_ROUND,
        time: UnixTime::from_millis(GENESIS_MILLIS),
        payload: vec![],
        proof: None,
        includes: Default::default(),
        witness: Default::default(),
        anchor_trigger: Link::ToSelf,
        anchor_proof: Link::ToSelf,
        anchor_time: UnixTime::from_millis(GENESIS_MILLIS),
    })
}
