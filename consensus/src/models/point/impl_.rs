use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bumpalo::Bump;
use bytes::Bytes;
use tl_proto::{TlError, TlRead, TlWrite};
use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_types::models::ConsensusConfig;
use tycho_util::metrics::HistogramGuard;

use crate::engine::MempoolConfig;
use crate::models::point::proto_utils::{PointBodyWrite, PointRawRead, PointRead, PointWrite};
use crate::models::point::{Digest, PointData, Signature};
use crate::models::{PointInfo, Round};

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

#[derive(Debug, Copy, Clone, thiserror::Error)]
pub enum StructureIssue {
    #[error("{0:?} map must not contain author")]
    AuthorInMap(PointMap),
    #[error("bad {0:?} link through {1:?} map")]
    Link(AnchorStageRole, PointMap),
    #[error("Evidence map contains bad signature")]
    EvidenceSig,
}

#[derive(Debug, Copy, Clone)]
pub enum PointMap {
    Evidence, // r+0
    Includes, // r-1
    Witness,  // r-2
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnchorStageRole {
    Trigger,
    Proof,
}

#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Point {
    serialized: Arc<Vec<u8>>,
    info: PointInfo,
}

impl Debug for Point {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Point").field(&self.info).finish()
    }
}

type ParseResult =
    Result<Result<Result<Point, (Point, StructureIssue)>, PointIntegrityError>, TlError>;

impl Point {
    pub fn max_byte_size(consensus_config: &ConsensusConfig) -> usize {
        let min_ext_msg_boc_size: usize = 48;

        let max_payload_size: usize = tl_proto::bytes_max_size_hint(min_ext_msg_boc_size)
            * (1 + (consensus_config.payload_batch_bytes.get() as usize / min_ext_msg_boc_size));

        4 + max_payload_size + PointInfo::MAX_BYTE_SIZE
    }

    pub fn new(
        local_keypair: &KeyPair,
        author: PeerId,
        round: Round,
        payload: &[Bytes],
        data: PointData,
        conf: &MempoolConfig,
    ) -> Self {
        assert_eq!(
            author,
            PeerId::from(local_keypair.public_key),
            "produced point author must match local key pair"
        );

        let mut serialized = Vec::<u8>::with_capacity(conf.point_max_bytes);
        PointWrite {
            digest: &Digest::ZERO,
            signature: &Signature::ZERO,
            body: PointBodyWrite {
                author: &author,
                round,
                payload,
                data: &data,
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

        let info = PointInfo::new(
            digest,
            signature,
            author,
            round,
            payload_len,
            payload_bytes,
            data,
        );

        Self {
            serialized: Arc::new(serialized),
            info,
        }
    }

    pub fn from_bytes(serialized: Vec<u8>) -> Result<Self, TlError> {
        let slice = &mut &serialized[..];
        let read = PointRead::read_from(slice)?;

        let payload_len = u32::try_from(read.body.payload.len()).unwrap_or(u32::MAX);
        let payload_bytes = u32::try_from(read.body.payload.iter().fold(0, |acc, b| acc + b.len()))
            .unwrap_or(u32::MAX);

        let info = PointInfo::new(
            read.digest,
            read.signature,
            read.body.author,
            read.body.round,
            payload_len,
            payload_bytes,
            read.body.data,
        );

        Ok(Self {
            serialized: Arc::new(serialized),
            info,
        })
    }

    pub fn parse(serialized: Vec<u8>) -> ParseResult {
        let _duration = HistogramGuard::begin("tycho_mempool_point_parse_verify_time");

        let raw = PointRawRead::<'_>::read_from(&mut &serialized[..])?;

        if !(raw.signature).verifies(raw.author()?, raw.digest) {
            return Ok(Err(PointIntegrityError::BadSig));
        };

        if *raw.digest != Digest::new(raw.body.as_ref()) {
            return Ok(Err(PointIntegrityError::BadHash));
        };

        let point = Self::from_bytes(serialized)?;

        Ok(Ok(match point.info().check_structure() {
            Ok(()) => Ok(point),
            Err(issue) => Err((point, issue)),
        }))
    }

    pub fn serialized(&self) -> &[u8] {
        &self.serialized
    }

    pub fn info(&self) -> &PointInfo {
        &self.info
    }

    // Note: resulting slice has lifetime of bump that is elided
    pub fn read_payload_from_tl_bytes<T>(data: T, bump: &Bump) -> Result<Vec<&[u8]>, TlError>
    where
        T: AsRef<[u8]>,
    {
        let raw = PointRawRead::read_from(&mut data.as_ref())?;
        Ok((raw.payload()?)
            .into_iter()
            .map(|item| &*bump.alloc_slice_copy(item))
            .collect())
    }
}

#[cfg(test)]
#[allow(dead_code, reason = "false positives")]
pub mod test_point {

    use bytes::Bytes;
    use rand::RngCore;
    use tycho_crypto::ed25519::SecretKey;
    use tycho_util::FastHashMap;

    use super::*;
    use crate::models::{Link, PointId, Round, Through, UnixTime};

    pub const PEERS: usize = 100;

    pub const MSG_BYTES: usize = 64 * 1024;

    pub fn new_key_pair() -> KeyPair {
        let mut secret_bytes: [u8; 32] = [0; 32];
        rand::rng().fill_bytes(&mut secret_bytes);
        KeyPair::from(&SecretKey::from_bytes(secret_bytes))
    }

    pub fn payload(conf: &MempoolConfig) -> Vec<Bytes> {
        let msg_count = conf.consensus.payload_batch_bytes.get() as usize / MSG_BYTES;
        let mut payload = Vec::with_capacity(msg_count);
        let mut bytes = vec![0; MSG_BYTES];
        for _ in 0..msg_count {
            rand::rng().fill_bytes(bytes.as_mut_slice());
            payload.push(Bytes::copy_from_slice(&bytes));
        }
        payload
    }

    pub fn prev_point_data() -> (Digest, Vec<(PeerId, Signature)>) {
        let mut buf = [0; Digest::MAX_TL_BYTES];
        rand::rng().fill_bytes(&mut buf);
        let digest = Digest::wrap(&buf);
        let mut evidence = Vec::with_capacity(PEERS);
        for _ in 0..PEERS {
            let key_pair = new_key_pair();
            let sig = Signature::new(&key_pair, digest);
            let peer_id = PeerId::from(key_pair.public_key);
            evidence.push((peer_id, sig));
        }
        (*digest, evidence)
    }

    pub fn point(key_pair: &KeyPair, payload: &[Bytes], conf: &MempoolConfig) -> Point {
        let prev_digest = Digest::new(&[42]);
        let mut includes = FastHashMap::default();
        includes.insert(PeerId::from(key_pair.public_key), prev_digest);
        let mut evidence = FastHashMap::default();
        let mut buf = [0; Digest::MAX_TL_BYTES];
        for i in 0..PEERS {
            let key_pair = new_key_pair();
            let peer_id = PeerId::from(key_pair.public_key);
            if i > 0 {
                rand::rng().fill_bytes(&mut buf);
                includes.insert(peer_id, *Digest::wrap(&buf));
            }
            evidence.insert(peer_id, Signature::new(&key_pair, &prev_digest));
        }
        let author = PeerId::from(key_pair.public_key);
        let round = Round(rand::random_range(10..u32::MAX - 10));
        let anchor_time = UnixTime::now();
        let data = PointData {
            time: anchor_time.next(),
            includes,
            witness: FastHashMap::from_iter([
                (PeerId([1; 32]), Digest::new(&[1])),
                (PeerId([2; 32]), Digest::new(&[2])),
            ]),
            evidence,
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
        Point::new(key_pair, author, round, payload, data, conf)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use anyhow::{Context, Result, ensure};
    use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
    use test_point::{PEERS, new_key_pair, payload, point, prev_point_data};
    use tycho_util::sync::rayon_run;

    use super::*;
    use crate::test_utils::default_test_config;

    #[test]
    pub fn check_serialize() -> Result<()> {
        let conf = default_test_config().conf;
        let point = point(&new_key_pair(), &payload(&conf), &conf);

        let point_2 = Point::parse(point.serialized().to_vec())
            .context("parse point bytes")??
            .map_err(|(_, err)| err)?;
        ensure!(point == point_2, "point serde roundtrip");

        let mut info_b = Vec::<u8>::with_capacity(point.info().max_size_hint());
        point.info().write_to(&mut info_b);
        let info_2 = tl_proto::deserialize::<PointInfo>(&info_b).context("point info")?;

        ensure!(point.info() == &info_2, "compare deserialized info");

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
        let info = point.info();

        let mut data = Vec::<u8>::with_capacity(conf.point_max_bytes);
        let timer = Instant::now();
        PointWrite {
            digest: info.digest(),
            signature: info.signature(),
            body: PointBodyWrite {
                author: info.author(),
                round: info.round(),
                payload: &payload,
                data: info.data(),
            },
        }
        .write_to(&mut data);
        let tl_elapsed = timer.elapsed();

        println!(
            "tl write {} bytes of point with {} bytes payload took {}",
            point.serialized().len(),
            info.payload_bytes(),
            humantime::format_duration(tl_elapsed)
        );

        let timer = Instant::now();
        let digest = Digest::new(&point.serialized()[4 + 32 + 64..]);
        let sha_elapsed = timer.elapsed();
        assert_eq!(&digest, info.digest(), "point digest");
        println!("hash took {}", humantime::format_duration(sha_elapsed));

        let timer = Instant::now();
        let sig = Signature::new(&point_key_pair, &digest);
        let sig_elapsed = timer.elapsed();
        assert_eq!(&sig, info.signature(), "point signature");
        println!("sig took {}", humantime::format_duration(sig_elapsed));

        let total = tl_elapsed + sha_elapsed + sig_elapsed;
        println!("total point build {}", humantime::format_duration(total));
    }

    #[test]
    pub fn massive_point_deserialization() {
        let conf = default_test_config().conf;
        let point = point(&new_key_pair(), &payload(&conf), &conf);
        let info = point.info();

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
            info.payload_bytes(),
            humantime::format_duration(elapsed)
        );
    }

    #[test]
    pub fn read_payload_from_tl() {
        let conf = default_test_config().conf;
        let payload = payload(&conf);
        let point = point(&new_key_pair(), &payload, &conf);
        let info = point.info();

        let bump = Bump::with_capacity(info.payload_bytes() as usize);
        let timer = Instant::now();
        let payload_r = Point::read_payload_from_tl_bytes(point.serialized(), &bump)
            .expect("Failed to deserialize ShortPoint from Point bytes");
        let elapsed = timer.elapsed();

        assert_eq!(payload, payload_r);
        assert_eq!(payload.len(), info.payload_len() as usize);
        assert_eq!(
            payload.iter().map(|m| m.len()).sum::<usize>(),
            info.payload_bytes() as usize
        );
        println!(
            "read {} bytes of payload took {}",
            info.payload_bytes(),
            humantime::format_duration(elapsed)
        );
    }

    #[test]
    pub fn massive_point_serialization() {
        let conf = default_test_config().conf;
        let payload = payload(&conf);
        let point = point(&new_key_pair(), &payload, &conf);
        let info = point.info();

        let mut data = Vec::<u8>::with_capacity(conf.point_max_bytes);
        let timer = Instant::now();
        const POINTS_LEN: u32 = 100;
        for _ in 0..POINTS_LEN {
            data.clear();
            PointWrite {
                digest: &Digest::ZERO,
                signature: &Signature::ZERO,
                body: PointBodyWrite {
                    author: info.author(),
                    round: info.round(),
                    payload: &payload,
                    data: info.data(),
                },
            }
            .write_to(&mut data);
        }

        let elapsed = timer.elapsed();
        println!(
            "tl write of {POINTS_LEN} point os size {} bytes of point with {} bytes payload took {}",
            point.serialized().len(),
            info.payload_bytes(),
            humantime::format_duration(elapsed)
        );
    }
}
