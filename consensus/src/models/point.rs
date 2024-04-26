use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Sub};

use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256};

use tycho_network::PeerId;

use crate::engine::MempoolConfig;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);
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
        std::fmt::Display::fmt(self, f)?;
        f.write_str(")")
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct Signature(pub Bytes);
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
        std::fmt::Display::fmt(self, f)?;
        f.write_str(")")
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Round(pub u32);

impl Round {
    pub fn prev(&self) -> Round {
        self.0
            .checked_sub(1)
            .map(Round)
            .expect("DAG round number underflow, fix dag initial configuration")
    }
    pub fn next(&self) -> Round {
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
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
    /// `>= 2F` neighbours, order does not matter;
    /// point author is excluded: everyone must use the proven point to validate its proof
    // Note: bincode may be non-stable on (de)serializing HashMap due to different local order
    pub evidence: BTreeMap<PeerId, Signature>,
    // TODO if we use TL, then every node can sign hash of a point's body (not all body bytes)
    //  so we can include that hash into PrevPoint
    //  to check signatures inside BroadcastFilter::verify() without waiting for DAG
    //  (if that will be fast enough to respond without overlay query timeout)
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
}

impl PointBody {
    pub fn wrap(self, local_keypair: &KeyPair) -> Point {
        assert_eq!(
            self.location.author,
            PeerId::from(local_keypair.public_key),
            "produced point author must match local key pair"
        );
        let body = bincode::serialize(&self).expect("shouldn't happen");
        let sig = local_keypair.sign_raw(body.as_slice());
        let mut hasher = Sha256::new();
        hasher.update(body.as_slice());
        hasher.update(sig.as_slice());
        let digest = Digest(hasher.finalize().into());
        Point {
            body: self,
            signature: Signature(Bytes::from(sig.to_vec())),
            digest,
        }
    }

    pub fn sign(&self, local_keypair: &KeyPair) -> Signature {
        let body = bincode::serialize(&self).expect("shouldn't happen");
        let sig = local_keypair.sign_raw(body.as_slice());
        Signature(Bytes::from(sig.to_vec()))
    }
}

// Todo: Arc<Point{...}> => Point(Arc<...{...}>)
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Point {
    pub body: PointBody,
    // author's signature for the body
    pub signature: Signature,
    // hash of both data and author's signature
    pub digest: Digest,
}

impl Point {
    pub fn id(&self) -> PointId {
        PointId {
            location: self.body.location.clone(),
            digest: self.digest.clone(),
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        let Some(digest) = self.body.proof.as_ref().map(|p| &p.digest) else {
            return None;
        };
        Some(PointId {
            location: Location {
                round: self.body.location.round.prev(),
                author: self.body.location.author,
            },
            digest: digest.clone(),
        })
    }

    /// Failed integrity means the point may be created by someone else.
    /// blame every dependent point author and the sender of this point,
    /// do not use the author from point's body
    pub fn is_integrity_ok(&self) -> bool {
        let pubkey = self.body.location.author.as_public_key();
        let body = bincode::serialize(&self.body).ok();
        let sig: Result<[u8; 64], _> = self.signature.0.to_vec().try_into();
        let Some(((pubkey, body), sig)) = pubkey.zip(body).zip(sig.ok()) else {
            return false;
        };
        let mut hasher = Sha256::new();
        hasher.update(body.as_slice());
        hasher.update(sig.as_slice());
        let digest = Digest(hasher.finalize().into());
        pubkey.verify_raw(body.as_slice(), &sig) && digest == self.digest
    }

    /// blame author and every dependent point's author
    /// must be checked right after integrity, before any manipulations with the point
    pub fn is_well_formed(&self) -> bool {
        // any genesis is suitable, round number may be taken from configs
        let author = &self.body.location.author;
        let is_special_ok = match self.body.location.round {
            MempoolConfig::GENESIS_ROUND => {
                self.body.includes.is_empty()
                    && self.body.witness.is_empty()
                    && self.body.payload.is_empty()
                    && self.body.proof.is_none()
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
            && self.body.proof.as_ref().map(|p| &p.digest) == self.body.includes.get(&author)
            // in contrast, evidence must contain only signatures of others
            && self.body.proof.as_ref().map_or(true, |p| !p.evidence.contains_key(author))
            && self.is_link_well_formed(&self.body.anchor_proof)
            && self.is_link_well_formed(&self.body.anchor_trigger)
            && match (self.anchor_proof_round(), self.anchor_trigger_round()) {
                (x, MempoolConfig::GENESIS_ROUND) => x >= MempoolConfig::GENESIS_ROUND,
                (MempoolConfig::GENESIS_ROUND, y) => y >= MempoolConfig::GENESIS_ROUND,
                // equality is impossible due to commit waves do not start every round;
                // anchor trigger may belong to a later round than proof and vice versa;
                // no indirect links over genesis tombstone
                (x, y) => x != y && x > MempoolConfig::GENESIS_ROUND && y > MempoolConfig::GENESIS_ROUND,
            }
    }

    fn is_link_well_formed(&self, link: &Link) -> bool {
        match link {
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

    // TODO maybe implement field accessors parameterized by combination of enums

    pub fn anchor_trigger_round(&self) -> Round {
        self.get_linked_to_round(&self.body.anchor_trigger)
    }

    pub fn anchor_proof_round(&self) -> Round {
        self.get_linked_to_round(&self.body.anchor_proof)
    }

    pub fn anchor_trigger_id(&self) -> PointId {
        self.get_linked_to(&self.body.anchor_trigger)
    }

    pub fn anchor_proof_id(&self) -> PointId {
        self.get_linked_to(&self.body.anchor_proof)
    }

    pub fn anchor_trigger_through(&self) -> PointId {
        self.get_linked_through(&self.body.anchor_trigger)
    }

    pub fn anchor_proof_through(&self) -> PointId {
        self.get_linked_through(&self.body.anchor_proof)
    }

    fn get_linked_to_round(&self, link: &Link) -> Round {
        match link {
            Link::ToSelf => self.body.location.round.clone(),
            Link::Direct(Through::Includes(_)) => self.body.location.round.prev(),
            Link::Direct(Through::Witness(_)) => self.body.location.round.prev().prev(),
            Link::Indirect { to, .. } => to.location.round.clone(),
        }
    }

    fn get_linked_to(&self, link: &Link) -> PointId {
        match link {
            Link::ToSelf => self.id(),
            Link::Direct(Through::Includes(peer)) => self.get_linked(peer, true),
            Link::Direct(Through::Witness(peer)) => self.get_linked(peer, false),
            Link::Indirect { to, .. } => to.clone(),
        }
    }

    fn get_linked_through(&self, link: &Link) -> PointId {
        match link {
            Link::Indirect {
                path: Through::Includes(peer),
                ..
            } => self.get_linked(peer, true),
            Link::Indirect {
                path: Through::Witness(peer),
                ..
            } => self.get_linked(peer, false),
            _ => self.get_linked_to(link),
        }
    }

    fn get_linked(&self, peer: &PeerId, through_includes: bool) -> PointId {
        let (through, round) = if through_includes {
            (&self.body.includes, self.body.location.round.prev())
        } else {
            (&self.body.witness, self.body.location.round.prev().prev())
        };
        PointId {
            location: Location {
                round,
                author: peer.clone(),
            },
            digest: through
                .get(peer)
                .expect("Coding error: usage of ill-formed point")
                .clone(),
        }
    }
}
