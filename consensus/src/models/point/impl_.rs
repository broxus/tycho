use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelRefIterator;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tycho_network::PeerId;

use crate::models::point::body::PointBody;
use crate::models::point::{AnchorStageRole, Digest, Link, PointData, PointId, Round, Signature};

#[derive(Clone)]
pub struct Point(Arc<PointInner>);

#[derive(Serialize, Deserialize, Debug)]
struct PointInner {
    // hash of everything except signature
    digest: Digest,
    // author's signature for the digest
    signature: Signature,
    body: PointBody,
}

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
            .field("body", self.data())
            .field("digest", self.digest())
            .field("signature", self.signature())
            .finish()
    }
}

impl PointInner {
    fn is_integrity_ok(&self) -> bool {
        self.signature
            .verifies(&self.body.data.author, &self.digest)
            && self.digest == self.body.make_digest()
    }
}

impl Point {
    pub fn new(
        local_keypair: &KeyPair,
        round: Round,
        evidence: Option<BTreeMap<PeerId, Signature>>,
        payload: Vec<Bytes>,
        data: PointData,
    ) -> Self {
        assert_eq!(
            data.author,
            PeerId::from(local_keypair.public_key),
            "produced point author must match local key pair"
        );
        let body = PointBody {
            round,
            data,
            evidence,
            payload,
        };
        let digest = body.make_digest();
        Self(Arc::new(PointInner {
            signature: Signature::new(local_keypair, &digest),
            digest,
            body,
        }))
    }

    pub fn digest(&self) -> &'_ Digest {
        &self.0.digest
    }

    pub fn signature(&self) -> &'_ Signature {
        &self.0.signature
    }

    pub fn round(&self) -> Round {
        self.0.body.round
    }

    pub fn data(&self) -> &PointData {
        &self.0.body.data
    }

    pub fn evidence(&self) -> Option<&BTreeMap<PeerId, Signature>> {
        self.0.body.evidence.as_ref()
    }

    pub fn payload(&self) -> &Vec<Bytes> {
        &self.0.body.payload
    }

    pub fn id(&self) -> PointId {
        PointId {
            author: self.0.body.data.author,
            round: self.0.body.round,
            digest: self.0.digest.clone(),
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        Some(PointId {
            author: self.0.body.data.author,
            round: self.0.body.round.prev(),
            digest: self.0.body.data.prev_digest.as_ref()?.clone(),
        })
    }

    pub fn prev_proof(&self) -> Option<PrevPoint> {
        Some(PrevPoint {
            digest: self.0.body.data.prev_digest.as_ref()?.clone(),
            evidence: self.0.body.evidence.as_ref()?.clone(),
        })
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
        self.0.body.is_well_formed()
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
        self.0.body.data.anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        self.0.body.data.anchor_round(link_field, self.0.body.round)
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        self.0
            .body
            .data
            .anchor_id(link_field, self.0.body.round)
            .unwrap_or(self.id())
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_id(&self, link_field: AnchorStageRole) -> PointId {
        self.0
            .body
            .data
            .anchor_link_id(link_field, self.0.body.round)
            .unwrap_or(self.id())
    }
}

#[derive(Debug)]
pub struct PrevPoint {
    pub digest: Digest,
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
    use crate::models::{PointInfo, Through, UnixTime};

    const PEERS: usize = 100;
    const MSG_COUNT: usize = 120;
    const MSG_BYTES: usize = 64 * 100;

    fn new_key_pair() -> KeyPair {
        let mut secret_bytes: [u8; 32] = [0; 32];
        thread_rng().fill_bytes(&mut secret_bytes);
        KeyPair::from(&SecretKey::from_bytes(secret_bytes))
    }

    fn point_body(key_pair: &KeyPair) -> PointBody {
        let mut payload = Vec::with_capacity(MSG_COUNT);
        let mut bytes = vec![0; MSG_BYTES];
        for _ in 0..MSG_COUNT {
            thread_rng().fill_bytes(bytes.as_mut_slice());
            payload.push(Bytes::copy_from_slice(&bytes));
        }

        let prev_digest = Digest::new(&[42]);
        let mut includes = BTreeMap::default();
        let mut evidence = BTreeMap::default();
        for _ in 0..PEERS {
            let key_pair = new_key_pair();
            let peer_id = PeerId::from(key_pair.public_key);
            thread_rng().fill_bytes(bytes.as_mut_slice());
            let digest = Digest::new(&bytes);
            includes.insert(peer_id, digest);
            evidence.insert(peer_id, Signature::new(&key_pair, &prev_digest));
        }

        PointBody {
            round: Round(thread_rng().next_u32()),
            data: PointData {
                author: PeerId::from(key_pair.public_key),
                time: UnixTime::now(),
                prev_digest: Some(prev_digest),
                includes,
                witness: BTreeMap::from([
                    (PeerId([1; 32]), Digest::new(&[1])),
                    (PeerId([2; 32]), Digest::new(&[2])),
                ]),
                anchor_trigger: Link::Direct(Through::Witness(PeerId([1; 32]))),
                anchor_proof: Link::Indirect {
                    to: PointId {
                        author: PeerId([122; 32]),
                        round: Round(852),
                        digest: Digest::new(&[2]),
                    },
                    path: Through::Witness(PeerId([2; 32])),
                },
                anchor_time: UnixTime::now(),
            },
            evidence: Some(evidence),
            payload,
        }
    }
    fn sig_data() -> (Digest, Vec<(PeerId, Signature)>) {
        let mut bytes = vec![0; MSG_BYTES];
        thread_rng().fill_bytes(bytes.as_mut_slice());
        let digest = Digest::new(&bytes);
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
    pub fn check_serialize() {
        let point_key_pair = new_key_pair();
        let point_body = point_body(&point_key_pair);
        let digest = point_body.make_digest();
        let point = Point(Arc::new(PointInner {
            signature: Signature::new(&point_key_pair, &digest),
            digest,
            body: point_body.clone(),
        }));
        let info = PointInfo::from(&point);
        let ser_info = bincode::serialize(&info).expect("serialize point");
        let ser_ref =
            bincode::serialize(&PointInfo::serializable_from(&point)).expect("serialize point");

        assert_eq!(
            info,
            bincode::deserialize::<PointInfo>(&ser_ref).expect("deserialize point info from ref"),
        );
        assert_eq!(
            Digest::new(&ser_info),
            Digest::new(&ser_ref),
            "compare serialized bytes"
        );
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
            );
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
        let digest = point_body.make_digest();
        let point = Point(Arc::new(PointInner {
            signature: Signature::new(&point_key_pair, &digest),
            digest,
            body: point_body.clone(),
        }));

        let timer = Instant::now();
        let body = bincode::serialize(&point_body).expect("shouldn't happen");
        let bincode_elapsed = timer.elapsed();

        let timer = Instant::now();
        let digest = Digest::new(body.as_slice());
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
        );
    }
}
