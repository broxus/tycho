use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bumpalo::Bump;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::ConsensusConfig;
use tl_proto::{TlError, TlRead, TlWrite};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;
use crate::models::point::serde_helpers::{
    PointBodyWrite, PointPrefixRead, PointRawRead, PointRead, PointWrite,
};
use crate::models::point::{AnchorStageRole, Digest, Link, PointData, PointId, Round, Signature};
use crate::models::{PeerCount, PointInfo};

#[derive(Debug, thiserror::Error)]
pub enum PointIntegrityError {
    #[error("hash mismatch")]
    BadHash,
    /// The point may be created by someone else:
    /// blame every dependent point author and the sender of this point,
    /// do not use the author from point's body
    #[error("signature does not match author")]
    BadSig,
}

#[derive(Clone)]
pub struct Point(Arc<PointInner>);

#[cfg_attr(test, derive(PartialEq))]
struct PointInner {
    serialized: Vec<u8>,
    parsed: PointParsed,
}

#[cfg_attr(test, derive(PartialEq))]
struct PointParsed {
    // hash of everything except signature
    digest: Digest,
    // author's signature for the digest
    signature: Signature,
    round: Round, // let it be @ r+0
    payload_len: u32,
    payload_bytes: u32,
    data: PointData,
    /// signatures for own point from previous round (if one exists, else empty map):
    /// the node may prove its vertex@r-1 with its point@r+0 only; contains signatures from
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter, author is excluded;
    evidence: BTreeMap<PeerId, Signature>,
}

impl Debug for Point {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Point")
            .field("digest", self.digest())
            .field("signature", self.signature())
            .field("round", &self.round().0)
            .field("payload_len", &self.payload_len())
            .field("payload_bytes", &self.payload_bytes())
            .field("data", self.data())
            .field("evidence", self.evidence())
            .finish()
    }
}

impl Point {
    pub fn max_byte_size(consensus_config: &ConsensusConfig) -> usize {
        let min_ext_msg_boc_size: usize = 48;

        let max_payload_size: usize = tl_proto::bytes_max_size_hint(min_ext_msg_boc_size)
            * (1 + (consensus_config.payload_batch_bytes as usize / min_ext_msg_boc_size));

        let max_evidence_size: usize =
            PeerCount::MAX.full() * (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES);

        4 + Signature::MAX_TL_BYTES
            + max_payload_size
            + PointInfo::MAX_BYTE_SIZE
            + max_evidence_size
    }

    pub fn new(
        local_keypair: &KeyPair,
        round: Round,
        payload: &[Bytes],
        data: PointData,
        evidence: BTreeMap<PeerId, Signature>,
        conf: &MempoolConfig,
    ) -> Self {
        assert_eq!(
            data.author,
            PeerId::from(local_keypair.public_key),
            "produced point author must match local key pair"
        );

        let mut serialized = Vec::<u8>::with_capacity(conf.point_max_bytes);
        PointWrite {
            digest: &Digest::ZERO,
            signature: &Signature::ZERO,
            body: PointBodyWrite {
                round,
                payload,
                data: &data,
                evidence: &evidence,
            },
        }
        .write_to(&mut serialized);

        let body_offset = 4 + Digest::MAX_TL_BYTES + Signature::MAX_TL_BYTES;

        let digest = Digest::new(&serialized[body_offset..]);
        let signature = Signature::new(local_keypair, &digest);

        serialized[4..4 + Digest::MAX_TL_BYTES].copy_from_slice(digest.inner());
        serialized[4 + Digest::MAX_TL_BYTES..body_offset].copy_from_slice(signature.inner());

        let payload_len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        let payload_bytes =
            u32::try_from(payload.iter().fold(0, |acc, b| acc + b.len())).unwrap_or(u32::MAX);

        let parsed = PointParsed {
            digest,
            signature,
            round,
            payload_len,
            payload_bytes,
            data,
            evidence,
        };

        Self(Arc::new(PointInner { parsed, serialized }))
    }

    pub fn from_bytes(serialized: Vec<u8>) -> Result<Self, TlError> {
        let slice = &mut &serialized[..];
        let read = PointRead::read_from(slice)?;

        let payload_len = u32::try_from(read.body.payload.len()).unwrap_or(u32::MAX);
        let payload_bytes = u32::try_from(read.body.payload.iter().fold(0, |acc, b| acc + b.len()))
            .unwrap_or(u32::MAX);

        let parsed = PointParsed {
            digest: read.digest,
            signature: read.signature,
            round: read.body.round,
            payload_len,
            payload_bytes,
            data: read.body.data,
            evidence: read.body.evidence,
        };

        Ok(Self(Arc::new(PointInner { parsed, serialized })))
    }

    pub fn parse(serialized: Vec<u8>) -> Result<Result<Self, PointIntegrityError>, TlError> {
        fn is_hash_ok(data: &[u8]) -> Result<bool, TlError> {
            let data = &mut &data[..];
            let read = PointRawRead::<'_>::read_from(data)?;
            Ok(read.digest == Digest::new(read.body.as_ref()))
        }
        fn is_sig_ok(point: &Point) -> bool {
            (point.signature()).verifies(&point.data().author, point.digest())
        }

        if !is_hash_ok(&serialized)? {
            return Ok(Err(PointIntegrityError::BadHash));
        };

        let point = Self::from_bytes(serialized)?;

        if !is_sig_ok(&point) {
            return Ok(Err(PointIntegrityError::BadSig));
        }
        Ok(Ok(point))
    }

    pub fn serialized(&self) -> &[u8] {
        &self.0.serialized
    }

    pub fn digest(&self) -> &'_ Digest {
        &self.0.parsed.digest
    }

    pub fn signature(&self) -> &'_ Signature {
        &self.0.parsed.signature
    }

    pub fn round(&self) -> Round {
        self.0.parsed.round
    }

    pub fn payload_len(&self) -> u32 {
        self.0.parsed.payload_len
    }

    pub fn payload_bytes(&self) -> u32 {
        self.0.parsed.payload_bytes
    }

    pub fn data(&self) -> &PointData {
        &self.0.parsed.data
    }

    pub fn evidence(&self) -> &BTreeMap<PeerId, Signature> {
        &self.0.parsed.evidence
    }

    pub fn id(&self) -> PointId {
        let parsed = &self.0.parsed;
        PointId {
            author: parsed.data.author,
            round: parsed.round,
            digest: parsed.digest,
        }
    }

    pub fn prev_id(&self) -> Option<PointId> {
        let parsed = &self.0.parsed;
        Some(PointId {
            author: parsed.data.author,
            round: parsed.round.prev(),
            digest: *parsed.data.prev_digest()?,
        })
    }

    pub fn prev_proof(&self) -> Option<PrevPointProof> {
        let parsed = &self.0.parsed;
        Some(PrevPointProof {
            digest: *parsed.data.prev_digest()?,
            evidence: parsed.evidence.clone(),
        })
    }

    /// blame author and every dependent point's author
    /// must be checked right after integrity, before any manipulations with the point
    pub fn is_well_formed(&self, conf: &MempoolConfig) -> bool {
        let parsed = &self.0.parsed;
        (parsed.data).is_well_formed(parsed.round, parsed.payload_len, &parsed.evidence, conf)
    }

    pub fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
        self.0.parsed.data.anchor_link(link_field)
    }

    pub fn anchor_round(&self, link_field: AnchorStageRole) -> Round {
        (self.0.parsed.data).anchor_round(link_field, self.0.parsed.round)
    }

    /// the final destination of an anchor link
    pub fn anchor_id(&self, link_field: AnchorStageRole) -> PointId {
        let parsed = &self.0.parsed;
        (parsed.data)
            .anchor_id(link_field, parsed.round)
            .unwrap_or(self.id())
    }

    /// next point in path from `&self` to the anchor
    pub fn anchor_link_id(&self, link_field: AnchorStageRole) -> PointId {
        let parsed = &self.0.parsed;
        (parsed.data)
            .anchor_link_id(link_field, parsed.round)
            .unwrap_or(self.id())
    }

    // Note: resulting slice has lifetime of bump that is elided
    pub fn read_payload_from_tl_bytes<T>(data: T, bump: &Bump) -> Result<Vec<&[u8]>, TlError>
    where
        T: AsRef<[u8]>,
    {
        let prefix = PointPrefixRead::read_from(&mut data.as_ref())?;
        Ok((prefix.body.payload)
            .into_iter()
            .map(|item| &*bump.alloc_slice_copy(item))
            .collect())
    }
}

#[derive(Debug)]
pub struct PrevPointProof {
    pub digest: Digest,
    pub evidence: BTreeMap<PeerId, Signature>,
}

impl PrevPointProof {
    pub fn signatures_match(&self) -> bool {
        // according to the rule of thumb to yield every 0.01-0.1 ms,
        // and that each signature check takes near 0.03 ms,
        // every check deserves its own async task - delegate to rayon as a whole
        (self.evidence.iter()).all(|(peer, sig)| sig.verifies(peer, &self.digest))
    }
}

#[cfg(test)]
#[allow(dead_code, reason = "false positives")]
pub mod test_point {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use everscale_crypto::ed25519::SecretKey;
    use rand::{thread_rng, Rng, RngCore};

    use super::*;
    use crate::models::{Through, UnixTime};

    pub const PEERS: usize = 100;

    pub const MSG_BYTES: usize = 64 * 1024;

    pub fn new_key_pair() -> KeyPair {
        let mut secret_bytes: [u8; 32] = [0; 32];
        thread_rng().fill_bytes(&mut secret_bytes);
        KeyPair::from(&SecretKey::from_bytes(secret_bytes))
    }

    pub fn payload(conf: &MempoolConfig) -> Vec<Bytes> {
        let msg_count = conf.consensus.payload_batch_bytes as usize / MSG_BYTES;
        let mut payload = Vec::with_capacity(msg_count);
        let mut bytes = vec![0; MSG_BYTES];
        for _ in 0..msg_count {
            thread_rng().fill_bytes(bytes.as_mut_slice());
            payload.push(Bytes::copy_from_slice(&bytes));
        }
        payload
    }

    pub fn prev_point_data() -> (Digest, Vec<(PeerId, Signature)>) {
        let mut buf = [0; Digest::MAX_TL_BYTES];
        thread_rng().fill_bytes(&mut buf);
        let digest = Digest::wrap(buf);
        let mut evidence = Vec::with_capacity(PEERS);
        for _ in 0..PEERS {
            let key_pair = new_key_pair();
            let sig = Signature::new(&key_pair, &digest);
            let peer_id = PeerId::from(key_pair.public_key);
            evidence.push((peer_id, sig));
        }
        (digest, evidence)
    }

    pub fn point(key_pair: &KeyPair, payload: &[Bytes], conf: &MempoolConfig) -> Point {
        let prev_digest = Digest::new(&[42]);
        let mut includes = BTreeMap::default();
        includes.insert(PeerId::from(key_pair.public_key), prev_digest);
        let mut evidence = BTreeMap::default();
        let mut buf = [0; Digest::MAX_TL_BYTES];
        for i in 0..PEERS {
            let key_pair = new_key_pair();
            let peer_id = PeerId::from(key_pair.public_key);
            if i > 0 {
                thread_rng().fill_bytes(&mut buf);
                includes.insert(peer_id, Digest::wrap(buf));
            }
            evidence.insert(peer_id, Signature::new(&key_pair, &prev_digest));
        }
        let round = Round(thread_rng().gen_range(10..u32::MAX - 10));
        let anchor_time = UnixTime::now();
        let data = PointData {
            author: PeerId::from(key_pair.public_key),
            time: anchor_time.next(),
            includes,
            witness: BTreeMap::from([
                (PeerId([1; 32]), Digest::new(&[1])),
                (PeerId([2; 32]), Digest::new(&[2])),
            ]),
            anchor_trigger: Link::Direct(Through::Witness(PeerId([1; 32]))),
            anchor_proof: Link::Indirect {
                to: PointId {
                    author: PeerId([122; 32]),
                    round: round - 6_u32,
                    digest: Digest::new(&[2]),
                },
                path: Through::Witness(PeerId([2; 32])),
            },
            anchor_time,
        };
        Point::new(key_pair, round, payload, data, evidence, conf)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use anyhow::{ensure, Context, Result};
    use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
    use test_point::{new_key_pair, payload, point, prev_point_data, PEERS};
    use tycho_util::sync::rayon_run;

    use super::*;
    use crate::models::PointInfo;
    use crate::test_utils::default_test_config;

    #[test]
    pub fn check_serialize() -> Result<()> {
        let conf = default_test_config().conf;
        let point = point(&new_key_pair(), &payload(&conf), &conf);

        let point_2 = Point::parse(point.serialized().to_vec()).context("parse point bytes")??;
        ensure!(point.0 == point_2.0, "point serde roundtrip");

        let info = PointInfo::from(&point);
        let mut info_b = Vec::<u8>::with_capacity(info.max_size_hint());
        info.write_to(&mut info_b);

        let info_w = PointInfo::serializable_from(&point);
        let mut info_w_b = Vec::<u8>::with_capacity(info_w.max_size_hint());
        info_w.write_to(&mut info_w_b);

        ensure!(
            info == tl_proto::deserialize(&info_w_b).context("deserialize point info from ref")?,
            "point info serde roundtrip"
        );
        ensure!(info_b == info_w_b, "compare serialized info bytes");
        Ok(())
    }

    #[test]
    pub fn check_sig() {
        let _conf = default_test_config().conf;

        let (digest, data) = prev_point_data();

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
        let _conf = default_test_config().conf;

        let (digest, data) = prev_point_data();

        let timer = Instant::now();
        rayon_run(|| ()).await;
        let elapsed = timer.elapsed();
        println!("init rayon took {}", humantime::format_duration(elapsed));

        let timer = Instant::now();
        rayon_run(move || {
            assert!(
                data.iter()
                    .all(|(peer_id, sig)| sig.verifies(peer_id, &digest)),
                "invalid signature"
            );
        })
        .await;
        let elapsed = timer.elapsed();
        println!(
            "check {PEERS} sigs sequentially on rayon took {}",
            humantime::format_duration(elapsed)
        );
    }

    #[test]
    pub fn check_new_point() {
        let conf = default_test_config().conf;
        let point_key_pair = new_key_pair();
        let payload = payload(&conf);
        let point = point(&point_key_pair, &payload, &conf);

        let mut data = Vec::<u8>::with_capacity(conf.point_max_bytes);
        let timer = Instant::now();
        PointWrite {
            digest: point.digest(),
            signature: point.signature(),
            body: PointBodyWrite {
                payload: &payload,
                round: point.round(),
                data: point.data(),
                evidence: point.evidence(),
            },
        }
        .write_to(&mut data);
        let tl_elapsed = timer.elapsed();

        println!(
            "tl write {} bytes of point with {} bytes payload took {}",
            point.serialized().len(),
            point.payload_bytes(),
            humantime::format_duration(tl_elapsed)
        );

        let timer = Instant::now();
        let digest = Digest::new(&point.serialized()[4 + 32 + 64..]);
        let sha_elapsed = timer.elapsed();
        assert_eq!(&digest, point.digest(), "point digest");
        println!("hash took {}", humantime::format_duration(sha_elapsed));

        let timer = Instant::now();
        let sig = Signature::new(&point_key_pair, &digest);
        let sig_elapsed = timer.elapsed();
        assert_eq!(&sig, point.signature(), "point signature");
        println!("sig took {}", humantime::format_duration(sig_elapsed));

        let total = tl_elapsed + sha_elapsed + sig_elapsed;
        println!("total point build {}", humantime::format_duration(total));
    }

    #[test]
    pub fn massive_point_deserialization() {
        let conf = default_test_config().conf;
        let point = point(&new_key_pair(), &payload(&conf), &conf);

        let serialized = (0..PEERS)
            .map(|_| point.serialized().to_vec())
            .collect::<Vec<_>>();

        let timer = Instant::now();
        for bytes in serialized {
            if let Err(e) = Point::from_bytes(bytes) {
                println!("error {e:?}");
                return;
            }
        }

        let elapsed = timer.elapsed();
        println!(
            "tl read of {PEERS} point of size {} bytes of point with {} bytes payload took {}",
            point.serialized().len(),
            point.payload_bytes(),
            humantime::format_duration(elapsed)
        );
    }

    #[test]
    pub fn read_payload_from_tl() {
        let conf = default_test_config().conf;
        let payload = payload(&conf);
        let point = point(&new_key_pair(), &payload, &conf);

        let bump = Bump::with_capacity(point.payload_len() as usize);
        let timer = Instant::now();
        let payload_r = Point::read_payload_from_tl_bytes(point.serialized(), &bump)
            .expect("Failed to deserialize ShortPoint from Point bytes");
        let elapsed = timer.elapsed();

        assert_eq!(payload, payload_r);
        assert_eq!(payload.len(), point.payload_len() as usize);
        assert_eq!(
            payload.iter().map(|m| m.len()).sum::<usize>(),
            point.payload_bytes() as usize
        );
        println!(
            "read {} bytes of payload took {}",
            point.payload_bytes(),
            humantime::format_duration(elapsed)
        );
    }

    #[test]
    pub fn massive_point_serialization() {
        let conf = default_test_config().conf;
        let payload = payload(&conf);
        let point = point(&new_key_pair(), &payload, &conf);

        let mut data = Vec::<u8>::with_capacity(conf.point_max_bytes);
        let timer = Instant::now();
        const POINTS_LEN: u32 = 100;
        for _ in 0..POINTS_LEN {
            data.clear();
            PointWrite {
                digest: &Digest::ZERO,
                signature: &Signature::ZERO,
                body: PointBodyWrite {
                    payload: &payload,
                    round: point.round(),
                    data: point.data(),
                    evidence: point.evidence(),
                },
            }
            .write_to(&mut data);
        }

        let elapsed = timer.elapsed();
        println!(
            "tl write of {POINTS_LEN} point os size {} bytes of point with {} bytes payload took {}",
            point.serialized().len(),
            point.payload_bytes(),
            humantime::format_duration(elapsed)
        );
    }
}
