use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use everscale_crypto::ed25519::KeyPair;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelRefIterator;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::models::point::body::{PointBody, ShortPointBody};
use crate::models::point::{AnchorStageRole, Digest, Link, PointData, PointId, Round, Signature};

#[derive(Clone, TlWrite, TlRead)]
pub struct Point(Arc<PointInner>);

#[derive(Clone, TlWrite, TlRead)]
pub struct ShortPoint(Arc<ShortPointInner>);

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "consensus.pointInner", scheme = "proto.tl")]
struct PointInner {
    // hash of everything except signature
    digest: Digest,
    // author's signature for the digest
    signature: Signature,
    body: PointBody,
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "consensus.shortPointInner", scheme = "proto.tl")]
struct ShortPointInner {
    // hash of everything except signature
    digest: Digest,
    // author's signature for the digest
    signature: Signature,
    body: ShortPointBody,
}

impl ShortPoint {
    pub fn payload(&self) -> &Vec<Bytes> {
        &self.0.body.payload
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
    }
}

impl Point {
    pub const TL_ID: u32 = tl_proto::id!("consensus.pointInner", scheme = "proto.tl");
    pub fn new(
        local_keypair: &KeyPair,
        round: Round,
        evidence: BTreeMap<PeerId, Signature>,
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

    pub fn evidence(&self) -> &BTreeMap<PeerId, Signature> {
        &self.0.body.evidence
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
            digest: self.0.body.data.prev_digest()?.clone(),
        })
    }

    pub fn prev_proof(&self) -> Option<PrevPoint> {
        Some(PrevPoint {
            digest: self.0.body.data.prev_digest()?.clone(),
            evidence: self.0.body.evidence.clone(),
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

    use bytes::{Bytes, BytesMut};
    use everscale_crypto::ed25519::SecretKey;
    use rand::{thread_rng, RngCore};
    use tycho_util::sync::rayon_run;

    use super::*;
    use crate::models::{PointInfo, Through, UnixTime};

    const PEERS: usize = 100;
    const MSG_COUNT: usize = 48;
    const MSG_BYTES: usize = 16266; // 64 * 100;

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
            evidence,
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
        let mut data = BytesMut::with_capacity(info.max_size_hint());
        info.write_to(&mut data);
        let ref_info = PointInfo::serializable_from(&point);
        let mut ref_data = BytesMut::with_capacity(info.max_size_hint());
        ref_info.write_to(&mut ref_data);

        assert_eq!(
            info,
            <PointInfo>::read_from(&ref_data, &mut 0).expect("deserialize point info from ref"),
        );
        assert_eq!(
            Digest::new(data.freeze().as_ref()),
            Digest::new(ref_data.freeze().as_ref()),
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
        let mut data = BytesMut::with_capacity(point_body.max_size_hint());
        point_body.write_to(&mut data);
        let bincode_elapsed = timer.elapsed();

        let bytes = data.freeze();

        let timer = Instant::now();
        let digest = Digest::new(bytes.as_ref());
        let sha_elapsed = timer.elapsed();
        assert_eq!(&digest, point.digest(), "point digest");

        let timer = Instant::now();
        let sig = Signature::new(&point_key_pair, &digest);
        let sig_elapsed = timer.elapsed();
        assert_eq!(&sig, point.signature(), "point signature");

        println!(
            "tl {} bytes of point with {} bytes payload took {}",
            bytes.len(),
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

    #[test]
    pub fn massive_point_serde() {
        let point_key_pair = new_key_pair();
        let timer = Instant::now();
        let mut point_payload = MSG_COUNT * MSG_BYTES;
        let mut byte_size = 0;

        let point_body = point_body(&point_key_pair);
        let digest = point_body.make_digest();
        let point = Point(Arc::new(PointInner {
            signature: Signature::new(&point_key_pair, &digest),
            digest,
            body: point_body.clone(),
        }));
        const POINTS_LEN: u32 = 100;
        for i in 0..POINTS_LEN {
            let point = point.clone();
            let mut data = BytesMut::with_capacity(1 << 20);
            point.write_to(&mut data);
            byte_size = data.len();
            // data.freeze();
        }

        let elapsed = timer.elapsed();
        println!(
            "tl write of {POINTS_LEN} point os size {byte_size} bytes of point with {point_payload} bytes payload took {}",
            humantime::format_duration(elapsed)
        );
    }
}
