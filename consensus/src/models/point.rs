use std::collections::BTreeMap;
use std::time::SystemTime;

use bytes::Bytes;
use everscale_crypto::ed25519::ExpandedSecretKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256};

use tycho_network::PeerId;
use tycho_util::FastHashMap;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Digest(pub [u8; 32]);

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Signature(pub Bytes);

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Round(pub u32);

impl Round {
    pub fn prev(&self) -> Round {
        self.0
            .checked_sub(1)
            .map(Round)
            .unwrap_or_else(|| panic!("DAG round number overflow, fix dag initial configuration"))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Location {
    pub round: Round,
    pub author: PeerId,
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
    pub evidence: FastHashMap<PeerId, Signature>,
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
    pub includes: BTreeMap<PeerId, Digest>,
    // >= 0 points @ r-2, signed by author @ r-1
    pub witness: BTreeMap<PeerId, Digest>,
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
        let pubkey = self.location.author.as_public_key()?;
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
    pub fn id(&self) -> PointId {
        PointId {
            location: self.body.location.clone(),
            digest: self.digest.clone(),
        }
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
    pub fn is_well_formed(&self) -> bool {
        let author = &self.body.location.author;
        let prev_included = self.body.includes.get(&author);
        let prev_proven = self.body.proof.as_ref().map(|p| &p.digest);
        prev_included == prev_proven
    }
}
