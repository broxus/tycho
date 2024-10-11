use std::sync::OnceLock;

use everscale_crypto::ed25519::{KeyPair, SecretKey};
use tycho_network::OverlayId;

use crate::dag::WAVE_ROUNDS;
use crate::engine::MempoolConfig;
use crate::models::{Link, Point, PointData, PointId, Round, UnixTime};

static GENESIS: OnceLock<PointId> = OnceLock::new();

pub struct Genesis;

impl Genesis {
    pub fn id() -> &'static PointId {
        GENESIS.get().expect("GENESIS_ID")
    }

    pub fn round() -> Round {
        GENESIS.get().expect("GENESIS_ROUND").round
    }

    pub fn init(start_round: Round, time: UnixTime) -> (Point, OverlayId) {
        // Must be (divisible by 4)+1, ie 1,5,9 etc., see `crate::dag::AnchorStage::of()`
        let aligned_start = ((start_round.0 + 2) / WAVE_ROUNDS) * WAVE_ROUNDS + 1;
        assert!(
            aligned_start > Round::BOTTOM.0,
            "invalid config: aligned genesis round is too low and will make code panic"
        );

        let mut hasher = blake3::Hasher::new();
        hasher.update(&aligned_start.to_be_bytes());
        hasher.update(&time.as_u64().to_be_bytes());
        hasher.update(&MempoolConfig::CLOCK_SKEW.as_u64().to_be_bytes());
        hasher.update(&MempoolConfig::COMMIT_DEPTH.to_be_bytes());
        hasher.update(&MempoolConfig::PAYLOAD_BATCH_BYTES.to_be_bytes());
        hasher.update(&MempoolConfig::DEDUPLICATE_ROUNDS.to_be_bytes());
        hasher.update(&MempoolConfig::MAX_ANCHOR_DISTANCE.to_be_bytes());
        let overlay_id = hasher.finalize().into();

        let genesis_keys = KeyPair::from(&SecretKey::from_bytes(overlay_id));

        let genesis = Point::new(
            &genesis_keys,
            Round(aligned_start),
            Default::default(),
            Default::default(),
            PointData {
                author: genesis_keys.public_key.into(),
                time,
                includes: Default::default(),
                witness: Default::default(),
                anchor_trigger: Link::ToSelf,
                anchor_proof: Link::ToSelf,
                anchor_time: time,
            },
        );

        GENESIS.set(genesis.id()).ok();
        assert_eq!(
            *Genesis::id(),
            genesis.id(),
            "genesis is not properly initialized"
        );

        (genesis, OverlayId(overlay_id))
    }
}
