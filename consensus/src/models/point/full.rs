use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tycho_network::PeerId;

use crate::models::point::inner::PointInner;
use crate::models::{AnchorStageRole, Digest, Link, PointBody, PointId, Round, Signature};

#[derive(Clone)]
pub struct Point(Arc<PointInner>);

impl Serialize for Point {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Point {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Point(Arc::new(PointInner::deserialize(deserializer)?)))
    }
}

impl Debug for Point {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Point")
            .field("body", self.body())
            .field("digest", self.digest())
            .field("signature", self.signature())
            .finish()
    }
}

impl Point {
    pub fn new(local_keypair: &KeyPair, point_body: PointBody) -> Self {
        assert_eq!(
            point_body.author,
            PeerId::from(local_keypair.public_key),
            "produced point author must match local key pair"
        );
        let digest = Digest::new(&point_body);
        Self(Arc::new(PointInner {
            body: point_body,
            signature: Signature::new(local_keypair, &digest),
            digest,
        }))
    }

    pub fn body(&self) -> &'_ PointBody {
        &self.0.body
    }

    pub fn digest(&self) -> &'_ Digest {
        &self.0.digest
    }

    pub fn signature(&self) -> &'_ Signature {
        &self.0.signature
    }

    pub fn id(&self) -> PointId {
        self.0.id()
    }

    pub fn prev_id(&self) -> Option<PointId> {
        self.0.prev_id()
    }

    /// Failed integrity means the point may be created by someone else.
    /// blame every dependent point author and the sender of this point,
    /// do not use the author from point's body
    pub fn is_integrity_ok(&self) -> bool {
        self.0.is_integrity_ok()
    }

    /// blame author and every dependent point's author
    /// must be checked right after integrity, before any manipulations with the point
    pub fn is_well_formed(&self) -> bool {
        self.0.is_well_formed()
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
        self.0.anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        self.0.anchor_round(link_field)
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        self.0.anchor_id(link_field)
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_id(&self, link_field: AnchorStageRole) -> PointId {
        self.0.anchor_link_id(link_field)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PrevPoint {
    // until weak links are supported,
    // any node may proof its vertex@r-1 with its point@r+0 only
    // pub round: Round,
    pub digest: Digest,
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter;
    /// point author is excluded: everyone must use the proven point to validate its proof
    pub evidence: BTreeMap<PeerId, Signature>,
}

impl PrevPoint {
    pub fn signatures_match(&self) -> bool {
        // according to the rule of thumb to yield every 0.01-0.1 ms,
        // and that each signature check takes near 0.03 ms,
        // every check deserves its own async task - delegate to rayon
        self.evidence
            .par_iter()
            .all(|(peer, sig)| sig.verifies(peer, &self.digest))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use bytes::Bytes;
    use everscale_crypto::ed25519::SecretKey;
    use rand::{thread_rng, RngCore};
    use tycho_util::sync::rayon_run;

    use super::*;
    use crate::models::UnixTime;

    const PEERS: usize = 100;
    const MSG_COUNT: usize = 1000;
    const MSG_BYTES: usize = 2 * 1000;

    fn new_key_pair() -> KeyPair {
        let mut secret_bytes: [u8; 32] = [0; 32];
        thread_rng().fill_bytes(&mut secret_bytes);
        KeyPair::from(&SecretKey::from_bytes(secret_bytes))
    }

    fn point_body(key_pair: &KeyPair) -> PointBody {
        let mut payload = Vec::with_capacity(MSG_COUNT);
        for _ in 0..MSG_COUNT {
            let mut data = vec![0; MSG_BYTES];
            thread_rng().fill_bytes(data.as_mut_slice());
            payload.push(Bytes::from(data));
        }

        let mut includes = BTreeMap::default();
        for _ in 0..PEERS {
            let key_pair = new_key_pair();
            let peer_id = PeerId::from(key_pair.public_key);
            let digest = Digest::from(*key_pair.secret_key.nonce());
            includes.insert(peer_id, digest);
        }

        PointBody {
            author: PeerId::from(key_pair.public_key),
            round: Round(thread_rng().next_u32()),
            time: UnixTime::now(),
            payload,
            proof: None,
            includes,
            witness: Default::default(),
            anchor_trigger: Link::ToSelf,
            anchor_proof: Link::ToSelf,
            anchor_time: UnixTime::now(),
        }
    }
    fn sig_data() -> (Digest, Vec<(PeerId, Signature)>) {
        let digest = Digest::from([12; 32]);
        let mut data = Vec::with_capacity(PEERS);
        for _ in 0..PEERS {
            let key_pair = new_key_pair();
            let sig = Signature::new(&key_pair, &digest);
            let peer_id = PeerId::from(key_pair.public_key);
            data.push((peer_id, sig));
        }
        (digest, data)
    }

    #[test]
    pub fn check_sig() {
        let (digest, data) = sig_data();

        let timer = Instant::now();
        for (peer_id, sig) in &data {
            sig.verifies(peer_id, &digest);
        }
        let elapsed = timer.elapsed();
        println!(
            "check {PEERS} sigs took {}",
            humantime::format_duration(elapsed)
        );

        let timer = Instant::now();
        assert!(
            data.par_iter()
                .all(|(peer_id, sig)| sig.verifies(peer_id, &digest)),
            "invalid signature"
        );
        let elapsed = timer.elapsed();
        println!(
            "check {PEERS} sigs in par iter took {}",
            humantime::format_duration(elapsed)
        );
    }

    #[tokio::test]
    pub async fn check_sig_on_rayon() {
        let (digest, data) = sig_data();

        let timer = Instant::now();
        rayon_run(|| ()).await;
        let elapsed = timer.elapsed();
        println!("init rayon took {}", humantime::format_duration(elapsed));

        let timer = Instant::now();
        rayon_run(move || {
            assert!(
                data.par_iter()
                    .all(|(peer_id, sig)| sig.verifies(peer_id, &digest)),
                "invalid signature"
            )
        })
        .await;
        let elapsed = timer.elapsed();
        println!(
            "check {PEERS} sigs on rayon in par iter took {}",
            humantime::format_duration(elapsed)
        );
    }

    #[test]
    pub fn check_new_point() {
        let point_key_pair = new_key_pair();
        let point_body = point_body(&point_key_pair);
        let point = Point::new(&point_key_pair, point_body.clone());

        let timer = Instant::now();
        let body = bincode::serialize(&point_body).expect("shouldn't happen");
        let bincode_elapsed = timer.elapsed();

        let timer = Instant::now();
        let digest = Digest::new(point.body());
        let sha_elapsed = timer.elapsed();
        assert_eq!(&digest, point.digest(), "point digest");

        let timer = Instant::now();
        let sig = Signature::new(&point_key_pair, &digest);
        let sig_elapsed = timer.elapsed();
        assert_eq!(&sig, point.signature(), "point signature");

        println!(
            "bincode {} bytes of point with {} bytes payload took {}",
            body.len(),
            point_body
                .payload
                .iter()
                .fold(0, |acc, bytes| acc + bytes.len()),
            humantime::format_duration(bincode_elapsed)
        );
        println!("hash took {}", humantime::format_duration(sha_elapsed));
        println!("sig took {}", humantime::format_duration(sig_elapsed));
        println!(
            "total {}",
            humantime::format_duration(bincode_elapsed + sha_elapsed + sig_elapsed)
        )
    }
}
