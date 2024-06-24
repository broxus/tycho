use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Sub};
use std::sync::Arc;

use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as Sha2Digest, Sha256};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Digest([u8; 32]);

impl Display for Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(32);
        for byte in self.0.iter().take(len) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Debug for Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Digest(")?;
        Display::fmt(self, f)?;
        f.write_str(")")
    }
}

impl Digest {
    fn new(point_body: &PointBody) -> Self {
        let body = bincode::serialize(&point_body).expect("shouldn't happen");
        let mut hasher = Sha256::new();
        hasher.update(body.as_slice());
        Self(hasher.finalize().into())
    }
    pub fn inner(&self) -> &'_ [u8; 32] {
        &self.0
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct Signature(Bytes);

impl Display for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = f.precision().unwrap_or(64);
        for byte in self.0.iter().take(len) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl Debug for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Signature(")?;
        Display::fmt(self, f)?;
        f.write_str(")")
    }
}

impl Signature {
    pub fn new(local_keypair: &KeyPair, digest: &Digest) -> Self {
        let sig = local_keypair.sign_raw(digest.0.as_slice());
        Self(Bytes::from(sig.to_vec()))
    }

    pub fn verifies(&self, signer: &PeerId, digest: &Digest) -> bool {
        let sig_raw: Result<[u8; 64], _> = self.0.to_vec().try_into();
        sig_raw
            .ok()
            .zip(signer.as_public_key())
            .map_or(false, |(sig_raw, pub_key)| {
                pub_key.verify_raw(digest.0.as_slice(), &sig_raw)
            })
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Round(pub u32);

impl Round {
    /// stub that cannot be used even by genesis round
    pub const BOTTOM: Self = Self(0);
    pub fn prev(&self) -> Self {
        self.0
            .checked_sub(1)
            .map(Round)
            .expect("DAG round number underflow, fix dag initial configuration")
    }
    pub fn next(&self) -> Self {
        self.0
            .checked_add(1)
            .map(Round)
            .expect("DAG round number overflow, inner type exhausted")
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct UnixTime(u64);

impl UnixTime {
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }
    pub fn now() -> Self {
        Self(
            u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("current time since unix epoch")
                    .as_millis(),
            )
            .expect("current Unix time in millis as u64"),
        )
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Add for UnixTime {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl Sub for UnixTime {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl Display for UnixTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Location {
    pub round: Round,
    pub author: PeerId,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct PointId {
    pub location: Location,
    pub digest: Digest,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PrevPoint {
    // until weak links are supported,
    // any node may proof its vertex@r-1 with its point@r+0 only
    // pub round: Round,
    pub digest: Digest,
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter;
    /// point author is excluded: everyone must use the proven point to validate its proof
    // Note: bincode may be non-stable on (de)serializing HashMap due to different local order
    pub evidence: BTreeMap<PeerId, Signature>,
}
impl PrevPoint {
    pub fn signatures_match(&self) -> bool {
        for (peer, sig) in &self.evidence {
            if !sig.verifies(peer, &self.digest) {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum Through {
    Witness(PeerId),
    Includes(PeerId),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum Link {
    ToSelf,
    Direct(Through),
    Indirect { to: PointId, path: Through },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PointBody {
    pub location: Location, // let it be @ r+0
    pub time: UnixTime,
    pub payload: Vec<Bytes>,
    /// by the same author
    pub proof: Option<PrevPoint>,
    /// `>= 2F+1` points @ r-1,
    /// signed by author @ r-1 with some additional points just mentioned;
    /// mandatory includes author's own vertex iff proof is given.
    /// Repeatable order on every node is needed for commit; map is used during validation
    pub includes: BTreeMap<PeerId, Digest>,
    /// `>= 0` points @ r-2, signed by author @ r-1
    /// Repeatable order on every node needed for commit; map is used during validation
    pub witness: BTreeMap<PeerId, Digest>,
    /// last included by author; defines author's last committed anchor
    pub anchor_trigger: Link,
    /// last included by author; maintains anchor chain linked without explicit DAG traverse
    pub anchor_proof: Link,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

/// Just a field accessor
#[derive(Clone, Copy)]
pub enum LinkField {
    Trigger,
    Proof,
}

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
            point_body.location.author,
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

    pub fn anchor_link(&self, link_field: LinkField) -> &'_ Link {
        self.0.anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: LinkField) -> Round {
        self.0.anchor_round(link_field)
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: LinkField) -> PointId {
        self.0.anchor_id(link_field)
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_id(&self, link_field: LinkField) -> PointId {
        self.0.anchor_link_id(link_field)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct PointInner {
    body: PointBody,
    // hash of the point's body (includes author peer id)
    digest: Digest,
    // author's signature for the digest
    signature: Signature,
}

impl PointInner {
    fn id(&self) -> PointId {
        PointId {
            location: self.body.location,
            digest: self.digest.clone(),
        }
    }

    fn prev_id(&self) -> Option<PointId> {
        let digest = self.body.proof.as_ref().map(|p| &p.digest)?;
        Some(PointId {
            location: Location {
                round: self.body.location.round.prev(),
                author: self.body.location.author,
            },
            digest: digest.clone(),
        })
    }

    fn is_integrity_ok(&self) -> bool {
        self.signature
            .verifies(&self.body.location.author, &self.digest)
            && self.digest == Digest::new(&self.body)
    }

    fn is_well_formed(&self) -> bool {
        // any genesis is suitable, round number may be taken from configs
        let author = &self.body.location.author;
        let is_special_ok = match self.body.location.round {
            MempoolConfig::GENESIS_ROUND => {
                self.body.includes.is_empty()
                    && self.body.witness.is_empty()
                    && self.body.payload.is_empty()
                    && self.body.proof.is_none()
                    && self.body.time == self.body.anchor_time
                    && self.body.anchor_proof == Link::ToSelf
                    && self.body.anchor_trigger == Link::ToSelf
            }
            round if round > MempoolConfig::GENESIS_ROUND => {
                // no witness is possible at the round right after genesis;
                // the other way: we may panic on round.prev().prev() while extracting link's round
                (round > MempoolConfig::GENESIS_ROUND.next() || self.body.witness.is_empty())
                // leader must maintain its chain of proofs,
                // while others must link to previous points (checked at the end of this method);
                // its decided later (using dag round data) whether current point belongs to leader
                && !(self.body.anchor_proof == Link::ToSelf && self.body.proof.is_none())
                && !(self.body.anchor_trigger == Link::ToSelf && self.body.proof.is_none())
            }
            _ => false,
        };
        is_special_ok
            // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
            && self.body.proof.as_ref().map(|p| &p.digest) == self.body.includes.get(author)
            // in contrast, evidence must contain only signatures of others
            && self.body.proof.as_ref().map_or(true, |p| !p.evidence.contains_key(author))
            && self.is_link_well_formed(LinkField::Proof)
            && self.is_link_well_formed(LinkField::Trigger)
            && self.body.time >= self.body.anchor_time
            && self.body.payload.iter().fold(0, |acc, x| acc + x.len()) <= MempoolConfig::PAYLOAD_BATCH_BYTES
            && match (self.anchor_round(LinkField::Proof), self.anchor_round(LinkField::Trigger)) {
                (x, MempoolConfig::GENESIS_ROUND) => x >= MempoolConfig::GENESIS_ROUND,
                (MempoolConfig::GENESIS_ROUND, y) => y >= MempoolConfig::GENESIS_ROUND,
                // equality is impossible due to commit waves do not start every round;
                // anchor trigger may belong to a later round than proof and vice versa;
                // no indirect links over genesis tombstone
                (x, y) => x != y && x > MempoolConfig::GENESIS_ROUND && y > MempoolConfig::GENESIS_ROUND,
            }
    }

    fn is_link_well_formed(&self, link_field: LinkField) -> bool {
        match self.anchor_link(link_field) {
            Link::ToSelf => true,
            Link::Direct(Through::Includes(peer)) => self.body.includes.contains_key(peer),
            Link::Direct(Through::Witness(peer)) => self.body.witness.contains_key(peer),
            Link::Indirect {
                path: Through::Includes(peer),
                to,
            } => {
                self.body.includes.contains_key(peer)
                    && to.location.round.next() < self.body.location.round
            }
            Link::Indirect {
                path: Through::Witness(peer),
                to,
            } => {
                self.body.witness.contains_key(peer)
                    && to.location.round.next().next() < self.body.location.round
            }
        }
    }

    fn anchor_link(&self, link_field: LinkField) -> &'_ Link {
        match link_field {
            LinkField::Trigger => &self.body.anchor_trigger,
            LinkField::Proof => &self.body.anchor_proof,
        }
    }

    fn anchor_round(&self, link_field: LinkField) -> Round {
        match self.anchor_link(link_field) {
            Link::ToSelf => self.body.location.round,
            Link::Direct(Through::Includes(_)) => self.body.location.round.prev(),
            Link::Direct(Through::Witness(_)) => self.body.location.round.prev().prev(),
            Link::Indirect { to, .. } => to.location.round,
        }
    }

    fn anchor_id(&self, link_field: LinkField) -> PointId {
        match self.anchor_link(link_field) {
            Link::Indirect { to, .. } => to.clone(),
            _direct => self.anchor_link_id(link_field),
        }
    }

    fn anchor_link_id(&self, link_field: LinkField) -> PointId {
        let (digest, location) = match self.anchor_link(link_field) {
            Link::ToSelf => return self.id(),
            Link::Direct(Through::Includes(peer))
            | Link::Indirect {
                path: Through::Includes(peer),
                ..
            } => (self.body.includes.get(peer), Location {
                author: *peer,
                round: self.body.location.round.prev(),
            }),
            Link::Direct(Through::Witness(peer))
            | Link::Indirect {
                path: Through::Witness(peer),
                ..
            } => (self.body.witness.get(peer), Location {
                author: *peer,
                round: self.body.location.round.prev().prev(),
            }),
        };
        PointId {
            location,
            digest: digest
                .expect("Coding error: usage of ill-formed point")
                .clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use everscale_crypto::ed25519::SecretKey;
    use rand::{thread_rng, RngCore};
    use tycho_util::sync::rayon_run;

    use super::*;

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
            let digest = Digest(*key_pair.secret_key.nonce());
            includes.insert(peer_id, digest);
        }

        PointBody {
            location: Location {
                round: Round(thread_rng().next_u32()),
                author: PeerId::from(key_pair.public_key),
            },
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
        let digest = Digest([12; 32]);
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
            assert!(sig.verifies(peer_id, &digest), "invalid signature");
        }
        let elapsed = timer.elapsed();
        println!(
            "check {PEERS} sigs took {}",
            humantime::format_duration(elapsed)
        );
    }

    #[tokio::test]
    pub async fn check_sig_on_rayon() {
        let (digest, data) = sig_data();
        rayon_run(|| ()).await;

        let timer = Instant::now();
        let fut = rayon_run(move || {
            for (peer_id, sig) in &data {
                assert!(sig.verifies(peer_id, &digest), "invalid signature");
            }
        });
        let elapsed_start = timer.elapsed();
        fut.await;
        let elapsed_run = timer.elapsed();

        println!(
            "init rayon took {}",
            humantime::format_duration(elapsed_start)
        );
        println!(
            "check {PEERS} with rayon took {}",
            humantime::format_duration(elapsed_run)
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
        let mut hasher = Sha256::new();
        hasher.update(body.as_slice());
        let digest = Digest(hasher.finalize().into());
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
        println!("sha256 took {}", humantime::format_duration(sha_elapsed));
        println!(
            "total {}",
            humantime::format_duration(bincode_elapsed + sha_elapsed + sig_elapsed)
        )
    }
}
