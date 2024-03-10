use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

use bytes::Bytes;
use everscale_crypto::ed25519::ExpandedSecretKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256};

use tycho_network::PeerId;
use tycho_util::FastHashMap;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Digest([u8; 32]);

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Signature(pub Bytes);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId([u8; 32]);

impl From<&PeerId> for NodeId {
    fn from(value: &PeerId) -> Self {
        NodeId(value.0)
    }
}

impl From<&NodeId> for PeerId {
    fn from(value: &NodeId) -> Self {
        PeerId(value.0)
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialOrd, PartialEq, Debug)]
pub struct Round(pub u32);

impl Round {
    pub fn prev(&self) -> Option<Round> {
        self.0.checked_sub(1).map(Round)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Location {
    pub round: Round,
    pub author: NodeId,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
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
    // >= 2F witnesses, point author excluded
    pub evidence: FastHashMap<NodeId, Signature>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PointBody {
    pub location: Location, // let it be @ r+0
    pub time: SystemTime,
    pub payload: Vec<Bytes>,
    // of the same author
    pub proof: Option<PrevPoint>,
    // >= 2F+1 points @ r-1,
    // signed by author @ r-1 with some additional points just mentioned;
    // optionally includes author's own vertex (if exists).
    // BTree provides repeatable order on every node
    pub includes: BTreeMap<NodeId, Digest>,
    // >= 0 points @ r-2, signed by author @ r-1
    pub witness: BTreeMap<NodeId, Digest>,
    // the last known third point in a row by some leader;
    // defines author's current anchor
    pub last_commit_trigger: PointId,
    // (only) for every leader node - three points in a row:
    // in leader point @ r+0: prev leader proof
    // in leader proof @ r+1: current leader point @ r+0
    // in commit trigger @ r+2: leader proof @ r+1
    pub leader_chain: Option<PointId>,
}

impl PointBody {
    pub fn wrap(self, secret: ExpandedSecretKey) -> Option<Point> {
        let body = bincode::serialize(&self).ok()?;
        let pubkey = PeerId::from(&self.location.author).as_public_key()?;
        let sig = secret.sign_raw(body.as_slice(), &pubkey);
        let mut hasher = Sha256::new();
        hasher.update(body.as_slice());
        hasher.update(sig.as_slice());
        let digest = Digest(hasher.finalize().into());
        Some(Point {
            body: self,
            signature: Signature(Bytes::from(sig.to_vec())),
            digest,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Point {
    pub body: PointBody,
    // author's signature for the body
    pub signature: Signature,
    // hash of both data and author's signature
    pub digest: Digest,
}

impl Point {
    pub fn is_integrity_ok(&self) -> bool {
        let pubkey = PeerId::from(&self.body.location.author).as_public_key();
        let body = bincode::serialize(&self.body).ok();
        let sig: Result<[u8; 64], _> = self.signature.0.to_vec().try_into();
        if let Some(((pubkey, body), sig)) = pubkey.zip(body).zip(sig.ok()) {
            let mut hasher = Sha256::new();
            hasher.update(body.as_slice());
            hasher.update(sig.as_slice());
            let digest = Digest(hasher.finalize().into());
            pubkey.verify_raw(body.as_slice(), &sig) && digest == self.digest
        } else {
            false
        }
    }
}
