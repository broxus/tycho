use std::fmt::{Display, Formatter};

use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

use crate::effects::{AltFmt, AltFormat};
use crate::models::{Digest, Round};

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PointKey {
    pub round: Round,
    pub digest: Digest,
}

impl PointKey {
    pub const MAX_TL_BYTES: usize = Round::MAX_TL_SIZE + Digest::MAX_TL_BYTES;

    pub fn new(round: Round, digest: Digest) -> Self {
        Self { round, digest }
    }

    pub fn fill(&self, key: &mut [u8; PointKey::MAX_TL_BYTES]) {
        key[..Round::MAX_TL_SIZE].copy_from_slice(&self.round.0.to_be_bytes()[..]);
        key[Round::MAX_TL_SIZE..].copy_from_slice(self.digest.inner());
    }

    pub fn fill_prefix(round: Round, key: &mut [u8; PointKey::MAX_TL_BYTES]) {
        key[..Round::MAX_TL_SIZE].copy_from_slice(&round.0.to_be_bytes()[..]);
        key[Round::MAX_TL_SIZE..].fill(0);
    }

    /// function of limited usage: zero round does not exist by application logic
    /// and 4 zero bytes usually represents empty value in storage;
    /// here None represents value less than 4 bytes
    pub fn parse_prefix(bytes: &[u8]) -> Option<Round> {
        if bytes.len() < 4 {
            None
        } else {
            let mut round_bytes = [0; 4];
            round_bytes.copy_from_slice(&bytes[..4]);
            Some(Round(u32::from_be_bytes(round_bytes)))
        }
    }

    pub fn format_loose(bytes: &[u8]) -> String {
        if let Some(round) = Self::parse_prefix(bytes) {
            if bytes.len() == PointKey::MAX_TL_BYTES {
                format!("round {} digest {}", round.0, (&bytes[4..]).alt())
            } else {
                format!("unknown {} bytes: {:.12}", bytes.len(), bytes.alt())
            }
        } else {
            format!("unknown short {} bytes {}", bytes.len(), bytes.alt())
        }
    }
}

impl TlWrite for PointKey {
    type Repr = tl_proto::Bare;

    fn max_size_hint(&self) -> usize {
        PointKey::MAX_TL_BYTES
    }

    fn write_to<P: TlPacket>(&self, packet: &mut P) {
        self.round.0.to_be_bytes().write_to(packet);
        self.digest.write_to(packet);
    }
}

impl<'tl> TlRead<'tl> for PointKey {
    type Repr = tl_proto::Bare;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        if packet.len() < PointKey::MAX_TL_BYTES {
            return Err(tl_proto::TlError::UnexpectedEof);
        }
        Ok(Self {
            round: Round(u32::from_be_bytes(<_>::read_from(packet)?)),
            digest: <_>::read_from(packet)?,
        })
    }
}

impl AltFormat for PointKey {}
impl<'a> Display for AltFmt<'a, PointKey> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let PointKey { round, digest } = AltFormat::unpack(self);
        write!(f, "round {} digest {}", round.0, digest.alt())
    }
}

#[cfg(test)]
mod test {
    use std::array;

    use anyhow::{Context, Result};

    use super::*;

    #[test]
    fn test() -> Result<()> {
        let key = PointKey::new(Round(1), *Digest::wrap(&array::from_fn(|i| i as u8)));
        let kvec = tl_proto::serialize(key);

        let key_2 = PointKey::read_from(&mut &kvec[..])?;

        anyhow::ensure!(key == key_2, "must match after tl write");

        let mut kar = [0; _];
        key.fill(&mut kar);

        anyhow::ensure!(kvec == kar.to_vec(), "bytes must match");

        let round = PointKey::parse_prefix(&kar).context("round")?;

        anyhow::ensure!(key.round == round, "round must match");

        Ok(())
    }
}
